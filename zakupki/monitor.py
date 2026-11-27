import csv
import hashlib
import html
import io
import ipaddress
import json
import math
import os
import re
import secrets
import smtplib
import sqlite3
import ssl
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from email.message import EmailMessage
from email.parser import BytesParser
from email.policy import default as email_policy
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from http.cookies import CookieError, SimpleCookie
from pathlib import Path
from zipfile import BadZipFile, ZipFile

LIMIT = 32 * 1024 * 1024
DATA = Path(os.getenv("DATA_DIR", "/data"))
TOKEN = secrets.token_urlsafe(32)
EDITOR_PORT = 8100
EDITOR_SESSION = "__Host-purchase_session"
EDITOR_LOGIN = "__Host-purchase_login"
EDITOR_SESSION_SECONDS = 12 * 3600
EDITOR_ACTIONS = frozenset({"add", "watch", "remove", "model_bind", "model_clear", "model_missing"})
IMPORT_LIMIT = 5 * 1024 * 1024
XLSX_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
LOCK = threading.Lock()
OPTIONS = {}
LAST_ERROR = ""
URL_KEYS = ("competitor_url", "catalog_url")
MODEL_LETTERS = str.maketrans("авексмнортху", "abekcmhoptxy")


class FormError(ValueError):
    pass


@contextmanager
def database():
    conn = sqlite3.connect(DATA / "prices.sqlite3")
    conn.row_factory = sqlite3.Row
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def initialize():
    DATA.mkdir(parents=True, exist_ok=True)
    with database() as db:
        db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS watch (
                item TEXT PRIMARY KEY, name TEXT NOT NULL, target INTEGER
            );
            CREATE TABLE IF NOT EXISTS sources (
                id TEXT PRIMARY KEY, name TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'unassigned'
            );
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY, digest TEXT NOT NULL,
                captured INTEGER NOT NULL, generated TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS offers (
                snapshot INTEGER NOT NULL, item TEXT NOT NULL, source TEXT NOT NULL,
                retail INTEGER, cost INTEGER, stock INTEGER,
                PRIMARY KEY (snapshot, item, source)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS offers_item ON offers(item, snapshot);
            CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value INTEGER);
            CREATE TABLE IF NOT EXISTS market_links (item TEXT PRIMARY KEY, title TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS market_missing (item TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS market (
                title TEXT NOT NULL, shop TEXT NOT NULL, price INTEGER,
                PRIMARY KEY(title,shop)
            );
            CREATE TABLE IF NOT EXISTS market_history (
                observed INTEGER NOT NULL, title TEXT NOT NULL, shop TEXT NOT NULL,
                price INTEGER NOT NULL, PRIMARY KEY(title,observed,shop)
            );
            CREATE TABLE IF NOT EXISTS hidden_market_shops (shop TEXT PRIMARY KEY);
            CREATE VIEW IF NOT EXISTS visible_market AS
                SELECT m.title, m.shop, m.price FROM market m
                WHERE NOT EXISTS (SELECT 1 FROM hidden_market_shops h WHERE h.shop=m.shop);
            CREATE VIEW IF NOT EXISTS market_models AS
                SELECT m.title, min(CASE WHEN h.shop IS NULL THEN m.price END) AS price
                FROM market m LEFT JOIN hidden_market_shops h ON h.shop=m.shop
                GROUP BY m.title;
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS product_names (item TEXT PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS watch_notes (item TEXT PRIMARY KEY, note TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS watch_metadata (
                item TEXT PRIMARY KEY, brand TEXT NOT NULL, category TEXT NOT NULL
            );
        """)


def money(value):
    try:
        amount = Decimal(str(value).replace(",", "."))
        if not amount.is_finite() or not 0 < amount <= 100000000:
            return None
        return int((amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP)) or None
    except InvalidOperation:
        return None


def parse(raw, feed_role, currency):
    stripped = raw.decode("utf-8-sig").lstrip()
    if stripped.startswith("<"):
        if "<!DOCTYPE" in stripped.upper():
            raise ValueError("DTD is unsupported")
        root = ET.fromstring(stripped)
        products = []
        for offer in root.findall(".//offers/offer"):
            if offer.findtext("currencyId", currency) != currency:
                continue
            price = offer.findtext("price")
            products.append({
                "id": offer.attrib["id"], "price": price,
                "name": offer.findtext("name") or offer.findtext("model"),
                "origPriceWRate": price if feed_role == "supplier" else None,
                "stock": int(offer.get("available", "").lower() == "true"),
            })
        document = {"generated": root.get("date", ""), "products": products}
    elif stripped.startswith("{"):
        document = json.loads(stripped)
    else:
        import yaml
        document = yaml.safe_load(stripped)
    catalog = {}
    for price in document.get("prices", []):
        key = price.get("priceConfigId") or price["id"]
        name = str(price.get("priceConfigName") or key)
        filename = str(price.get("fileName") or "").replace(chr(92), "/").rsplit("/", 1)[-1]
        is_parser = filename.casefold() == "parse_prices.csv" or name.casefold().startswith("парсер")
        catalog[str(price["id"])] = (str(key), name, is_parser)
    sources = {}
    offers = {}
    names = {}
    for product in document["products"]:
        item = str(product["id"])
        name = product.get("name") or product.get("title")
        if isinstance(name, str) and name.strip():
            names[item] = name.strip()
        for offer in [product, *product.get("extraPrices", [])]:
            price_id = str(offer.get("priceId") or "feed")
            source, name, is_parser = catalog.get(price_id, (price_id, price_id, False))
            sources[source] = (name, is_parser)
            stock = offer.get("stock")
            stock = int(stock) if str(stock).isdigit() else None
            available = offer.get("inStock")
            if available is not None and str(available) != "1":
                stock = 0
            retail = money(offer.get("price"))
            cost = money(offer.get("origPriceWRate"))
            row = (item, source, retail, cost, stock)
            existing = offers.get((item, source))
            if existing is None or offer_priority(row) < offer_priority(existing):
                offers[(item, source)] = row
    if not offers and not document.get("staledProductsIds"):
        raise ValueError("Empty feed")
    return str(document.get("generated") or ""), sources, list(offers.values()), names


def offer_priority(row):
    return (not bool(row[4]), row[3] is None, row[3] or row[2] or float("inf"))


def ingest(raw):
    digest = hashlib.sha256(raw).hexdigest()
    generated, sources, offers, names = parse(raw, OPTIONS.get("feed_role", "unassigned"),
                                              OPTIONS.get("currency", "BYN"))
    now = int(time.time())
    with database() as db:
        db.executemany("REPLACE INTO product_names VALUES(?,?)", names.items())
        role = OPTIONS.get("feed_role", "unassigned") if list(sources) == ["feed"] else "unassigned"
        db.executemany("""INSERT INTO sources(id,name,role) VALUES(?,?,?)
            ON CONFLICT(id) DO UPDATE SET name=excluded.name,
            role=CASE WHEN excluded.role='parser' THEN 'parser' ELSE sources.role END""",
            ((identifier, name, "parser" if is_parser else role)
             for identifier, (name, is_parser) in sources.items()))
        previous = db.execute("SELECT digest FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()
        if previous is None or previous["digest"] != digest:
            cursor = db.execute(
                "INSERT INTO snapshots(digest,captured,generated) VALUES(?,?,?)",
                (digest, now, generated),
            )
            db.executemany("INSERT INTO offers VALUES(?,?,?,?,?,?)",
                           ((cursor.lastrowid, *row) for row in offers))
            cutoff = now - max(7, int(OPTIONS.get("history_days", 90))) * 86400
            db.execute("DELETE FROM offers WHERE snapshot IN (SELECT id FROM snapshots WHERE captured<?)", (cutoff,))
            db.execute("DELETE FROM snapshots WHERE captured<?", (cutoff,))
        db.execute("REPLACE INTO state VALUES('checked',?)", (now,))
    return len(offers)


def download(url):
    if urllib.parse.urlsplit(url).scheme != "https":
        raise ValueError("HTTPS is required")
    with urllib.request.urlopen(url, timeout=30) as response:
        if urllib.parse.urlsplit(response.url).scheme != "https":
            raise ValueError("HTTPS is required")
        raw = response.read(LIMIT + 1)
    if len(raw) > LIMIT:
        raise ValueError("Feed too large")
    return raw


def parse_market(raw):
    try:
        content = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        content = raw.decode("cp1251")
    reader = csv.DictReader(io.StringIO(content), delimiter=";")
    fields = reader.fieldnames or []
    if fields[:1] != ["title"] or len(fields) < 2 or len(set(fields)) != len(fields):
        raise ValueError("Invalid competitor CSV header")
    entries = {}
    for row in reader:
        title = (row.get("title") or "").strip()
        if not title:
            continue
        for shop in fields[1:]:
            price = money(row.get(shop))
            key = (title, shop)
            if key not in entries or (price is not None and
                    (entries[key] is None or price < entries[key])):
                entries[key] = price
    if not entries:
        raise ValueError("Empty competitor CSV")
    return entries


def sync_market():
    entries = parse_market(download(OPTIONS["competitor_url"]))
    now = int(time.time())
    with database() as db:
        old = {(row[0], row[1]): row[2] for row in db.execute("SELECT * FROM market")}
        if entries != old:
            db.execute("DELETE FROM market")
            db.executemany("INSERT INTO market VALUES(?,?,?)",
                           ((title, shop, price) for (title, shop), price in entries.items()))
            db.executemany("INSERT OR REPLACE INTO market_history VALUES(?,?,?,?)",
                           ((now, title, shop, price) for (title, shop), price in entries.items()
                            if price is not None))
        db.execute("DELETE FROM market_history WHERE observed<?",
                   (now - int(OPTIONS.get("history_days", 90)) * 86400,))
        db.execute("REPLACE INTO state VALUES('market_checked',?)", (now,))


def sync_catalog():
    content = download(OPTIONS["catalog_url"]).decode("utf-8-sig").lstrip()
    if content.startswith("<"):
        if "<!DOCTYPE" in content.upper():
            raise ValueError("DTD is unsupported")
        offers = ET.fromstring(content).findall(".//offers/offer")
        pairs = ((offer.get("id"), offer.findtext("name") or offer.findtext("model"))
                 for offer in offers)
    else:
        document = json.loads(content)
        products = document if isinstance(document, list) else document["products"]
        pairs = ((product.get("id"), product.get("name") or product.get("title"))
                 for product in products)
    names = {str(item).strip(): name.strip() for item, name in pairs
             if item is not None and str(item).strip() and isinstance(name, str) and name.strip()}
    if not names:
        raise ValueError("Catalog contains no product names")
    with database() as db:
        db.executemany("REPLACE INTO product_names VALUES(?,?)", names.items())
        db.execute("REPLACE INTO state VALUES('catalog_checked',?)", (int(time.time()),))


def sync():
    errors = []
    if OPTIONS.get("feed_url"):
        try:
            ingest(download(OPTIONS["feed_url"]))
        except Exception as exc:
            errors.append(f"ShopManager · ошибка загрузки ({type(exc).__name__})")
    if OPTIONS.get("competitor_url"):
        try:
            sync_market()
        except Exception as exc:
            errors.append(f"Конкуренты · ошибка загрузки ({type(exc).__name__})")
    if OPTIONS.get("catalog_url"):
        try:
            sync_catalog()
        except Exception as exc:
            errors.append(f"Каталог товаров · ошибка загрузки ({type(exc).__name__})")
    return " · ".join(errors)


def model_key(name):
    return " ".join(re.findall(r"[^\W_]+", name.casefold().translate(MODEL_LETTERS)))


def match_model(name, market):
    key = model_key(name)
    if not any(character.isdigit() for character in key) or len(key.split()) < 2:
        return None
    matches = [title for title, candidate, _ in market
               if candidate == key or candidate.endswith(" " + key)]
    return matches[0] if len(matches) == 1 else None


def results(db):
    fresh = bool(OPTIONS.get("competitor_url")) and (
        time.time() - state(db, "market_checked") <= 2 * int(OPTIONS.get("poll_hours", 6)) * 3600
    )
    rows = db.execute("""
        WITH recent AS (SELECT max(id) AS id FROM snapshots),
        suppliers AS (
            SELECT id, name FROM sources
            WHERE role IN ('supplier','unassigned')
              AND name NOT LIKE 'Парсер%' AND name NOT LIKE 'парсер%'
        ), historical AS (
            SELECT h.item, h.cost, t.captured, hs.name AS supplier,
                   row_number() OVER (
                       PARTITION BY h.item ORDER BY h.cost, t.captured DESC, t.id DESC, hs.name, hs.id
                   ) AS rank
            FROM watch w JOIN offers h ON h.item=w.item
            JOIN snapshots t ON t.id=h.snapshot
            JOIN suppliers hs ON hs.id=h.source
            WHERE t.captured>=? AND h.stock>0 AND h.cost IS NOT NULL
        )
        SELECT w.item, w.target, w.name AS custom_name,
          coalesce(nullif(nullif(w.name,'ID ' || w.item),''), p.name, l.title,
                   'Название не указано') AS name,
          min(CASE WHEN s.id IS NOT NULL AND o.stock>0 THEN o.cost END) AS buy,
          min(CASE WHEN s.id IS NOT NULL AND coalesce(o.stock,0)<=0 THEN o.cost END) AS unavailable_buy,
          l.title AS manual_market_title, coalesce(n.note,'') AS note,
          coalesce(meta.brand,'') AS brand, coalesce(meta.category,'') AS category,
          missing.item IS NOT NULL AS market_marked_missing,
          low.cost AS low_90, low.captured AS low_captured, low.supplier AS low_supplier,
          (SELECT hs.name FROM offers h JOIN suppliers hs ON hs.id=h.source
           WHERE h.item=w.item AND h.snapshot=(SELECT id FROM recent) AND h.cost IS NOT NULL
           ORDER BY (coalesce(h.stock,0)>0) DESC, h.cost, hs.name, hs.id LIMIT 1) AS buy_supplier,
          (SELECT min(h.cost) FROM offers h JOIN suppliers hs ON hs.id=h.source
           WHERE h.item=w.item AND h.snapshot<(SELECT id FROM recent) AND h.stock>0
           AND h.cost IS NOT NULL
           GROUP BY h.snapshot ORDER BY h.snapshot DESC LIMIT 1) AS previous_buy
        FROM watch w
        LEFT JOIN offers o ON o.item=w.item AND o.snapshot=(SELECT id FROM recent)
        LEFT JOIN suppliers s ON s.id=o.source
        LEFT JOIN historical low ON low.item=w.item AND low.rank=1
        LEFT JOIN market_links l ON l.item=w.item
        LEFT JOIN market_missing missing ON missing.item=w.item
        LEFT JOIN product_names p ON p.item=w.item
        LEFT JOIN watch_notes n ON n.item=w.item
        LEFT JOIN watch_metadata meta ON meta.item=w.item
        GROUP BY w.item ORDER BY w.name COLLATE NOCASE
    """, (int(time.time()) - 90 * 86400,)).fetchall()
    market = [(row["title"], model_key(row["title"]), row["price"]) for row in db.execute(
        "SELECT title,price FROM market_models")]
    prices = {title: price for title, _, price in market}
    cheapest_shops = {}
    if fresh:
        for offer in db.execute("SELECT title,shop,price FROM visible_market WHERE price IS NOT NULL ORDER BY shop"):
            if offer["price"] == prices[offer["title"]]:
                cheapest_shops.setdefault(offer["title"], []).append(offer["shop"])
    results = []
    for row in rows:
        entry = dict(row)
        title = row["manual_market_title"] or match_model(row["name"], market)
        entry["market_title"] = title
        entry["market"] = prices.get(title) if fresh else None
        entry["market_shops"] = cheapest_shops.get(title, [])
        results.append(entry)
    return results


def decision(row):
    if row["buy"] is None:
        return "", 0
    if row["target"] and row["buy"] <= row["target"]:
        return "Цель достигнута", 1
    tolerance = int(OPTIONS.get("target_tolerance_percent", 5))
    if row["target"] and row["buy"] * 100 <= row["target"] * (100 + tolerance):
        return "Цена близка к цели", 2
    if row["market"] and 100 * (row["market"] - row["buy"]) >= int(
        OPTIONS.get("min_gap_percent", 15)
    ) * row["market"]:
        return "Разница с ценой конкурента", 3
    return "", 0


def amount(value):
    return "—" if value is None else f"{Decimal(value) / 100:.2f}"


def competitor_gap(row):
    buy, market = row["buy"], row["market"]
    if buy is None or market is None or market <= 0:
        return ""
    difference = market - buy
    if not difference:
        return "На уровне конкурента"
    currency = OPTIONS.get("currency", "BYN")
    percent = Decimal(abs(difference)) * 100 / market
    direction = "Дешевле" if difference > 0 else "Дороже"
    return f"{direction} на {amount(abs(difference))} {currency} ({percent:.1f}%)"


def low_price_date(row):
    return datetime.fromtimestamp(row["low_captured"] + 3 * 3600, timezone.utc).strftime("%d.%m.%Y")


def purchase_chart(db, item):
    rows = db.execute("""
        SELECT date(t.captured,'unixepoch','+3 hours') AS day, min(o.cost) AS price
        FROM offers o JOIN snapshots t ON t.id=o.snapshot JOIN sources s ON s.id=o.source
        WHERE o.item=? AND t.captured>=? AND o.stock>0 AND o.cost IS NOT NULL
          AND s.role IN ('supplier','unassigned')
          AND s.name NOT LIKE 'Парсер%' AND s.name NOT LIKE 'парсер%'
        GROUP BY day ORDER BY day
        """, (item, int(time.time()) - 90 * 86400)).fetchall()
    heading = '<h3>Минимальная закупка за 90 дней</h3>'
    if not rows:
        return heading + '<p class="sub">Пока нет цен с подтверждённым остатком.</p>'
    low = min(row["price"] for row in rows)
    high = max(row["price"] for row in rows)
    first = datetime.fromisoformat(rows[0]["day"])
    last = datetime.fromisoformat(rows[-1]["day"])
    span = (last - first).days
    points = []
    labels = []
    values = []
    for row in rows:
        day = datetime.fromisoformat(row["day"])
        price = amount(row["price"])
        x = 50 if not span else 100 * (day - first).days / span
        y = 50 if high == low else 100 * (high - row["price"]) / (high - low)
        points.append((x, y))
        date = day.strftime("%d.%m.%Y")
        labels.append(f'<span class="chart-point" style="left:{x:.3f}%;top:{y:.3f}%" '
                      f'title="{date} · {price}"></span>')
        values.append(f'<li>{date} · {price}</li>')
    segments = []
    for (x, y), (next_x, next_y) in zip(points, points[1:]):
        dx, dy = next_x - x, (next_y - y) / 2  # The chart is twice as wide as it is tall.
        width = math.hypot(dx, dy)
        angle = math.degrees(math.atan2(dy, dx))
        gap = " chart-gap" if dx > 100 / max(span, 1) + 0.00001 else ""
        segments.append(f'<span class="chart-line{gap}" style="left:{x:.3f}%;top:{y:.3f}%;'
                        f'width:{width:.3f}%;transform:rotate({angle:.3f}deg)"></span>')
    currency = esc(OPTIONS.get("currency", "BYN"))
    limits = f'{amount(low)} {currency}' if low == high else f'{amount(low)}–{amount(high)} {currency}'
    return (f'{heading}<p class="sub">Лучшая цена в наличии за день · {limits}</p>'
            f'<div class="purchase-chart" aria-hidden="true">{"".join(segments + labels)}</div>'
            f'<div class="chart-dates" aria-hidden="true"><span>{first:%d.%m.%Y}</span>'
            f'<span>{last.strftime("%d.%m.%Y") if span else ""}</span></div>'
            f'<details class="chart-values"><summary>Цены по дням</summary><ul>{"".join(values)}</ul></details>')


def purchase_trend(current, previous):
    if current is None or previous is None or current == previous:
        return ""
    direction, arrow, description = (("down", "↓", "снизилась") if current < previous
                                     else ("up", "↑", "выросла"))
    label = f"Цена {description} на {amount(abs(current - previous))}. Предыдущая цена {amount(previous)}"
    return (f' <span class="price-trend {direction}" role="img" aria-label="{esc(label)}" '
            f'title="{esc(label)}">{arrow}</span>')


def deliver(message):
    if OPTIONS.get("telegram_token") and OPTIONS.get("telegram_chat"):
        token = OPTIONS["telegram_token"]
        payload = json.dumps({"chat_id": OPTIONS["telegram_chat"], "text": message}).encode()
        request = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage",
            data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=20) as response:
            if not json.load(response).get("ok"):
                raise ValueError("Telegram delivery rejected")
    elif OPTIONS.get("smtp_host") and OPTIONS.get("email_to"):
        email = EmailMessage()
        email["Subject"] = "Сводка закупок"
        email["From"] = OPTIONS["email_from"]
        email["To"] = OPTIONS["email_to"]
        email.set_content(message)
        with smtplib.SMTP_SSL(OPTIONS["smtp_host"], int(OPTIONS.get("smtp_port", 465)),
                              timeout=20, context=ssl.create_default_context()) as client:
            if OPTIONS.get("smtp_user"):
                client.login(OPTIONS["smtp_user"], OPTIONS["smtp_password"])
            client.send_message(email)
    else:
        raise ValueError("Notification channel is missing")


def state(db, key):
    row = db.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
    return row[0] if row else 0


def send_due():
    if not ((OPTIONS.get("telegram_token") and OPTIONS.get("telegram_chat"))
            or (OPTIONS.get("smtp_host") and OPTIONS.get("email_to"))):
        return
    now = int(time.time())
    with database() as db:
        last = state(db, "digest")
        if last and now - last < int(OPTIONS.get("digest_days", 3)) * 86400:
            return
        if not last:
            db.execute("REPLACE INTO state VALUES('digest',?)", (now,))
            return
        checked = state(db, "checked")
        if now - checked > 2 * int(OPTIONS.get("poll_hours", 6)) * 3600:
            return
        candidates = [row for row in results(db) if decision(row)[1]]
    if candidates:
        candidates.sort(key=lambda row: (
            decision(row)[1], -(row["market"] or row["target"] or 0) + row["buy"]
        ))
        lines = [f"К закупке · {OPTIONS.get('currency', 'BYN')}"]
        footer = f"Всего позиций: {len(candidates)}. Полный список в приложении."
        for row in candidates[:15]:
            details = [f"Закупка {amount(row['buy'])} · {row['buy_supplier'][:80]}", decision(row)[0]]
            if row["previous_buy"] is not None and row["previous_buy"] != row["buy"]:
                change = Decimal(row["buy"] - row["previous_buy"]) / 100
                details.append(f"Изменение закупки {change:+.2f} (было {amount(row['previous_buy'])})")
            if row["market"] is not None:
                details.append(f"Конкуренты {amount(row['market'])} · {competitor_gap(row)}")
            if row["low_90"] is not None:
                details.append(f"Мин. за 90 дней {amount(row['low_90'])} · {low_price_date(row)}"
                               f" · {row['low_supplier'][:80]}")
            block = f"{row['name'][:80]} [{row['item'][:40]}]\n" + "\n".join(details)
            if len("\n\n".join([*lines, block, footer])) > 3900:
                break
            lines.append(block)
        if len(lines) - 1 < len(candidates):
            lines.append(footer)
        deliver("\n\n".join(lines))
    with database() as db:
        db.execute("REPLACE INTO state VALUES('digest',?)", (now,))


def worker():
    global LAST_ERROR
    last_attempt = 0
    while True:
        try:
            with LOCK:
                with database() as db:
                    checked = [state(db, key) for option, key in
                               (("feed_url", "checked"), ("competitor_url", "market_checked"),
                                ("catalog_url", "catalog_checked"))
                               if OPTIONS.get(option)]
                now = time.time()
                if (checked and now - last_attempt >= 300
                        and now - min(checked) >= int(OPTIONS.get("poll_hours", 6)) * 3600):
                    last_attempt = now
                    LAST_ERROR = sync()
                send_due()
        except Exception as exc:
            LAST_ERROR = f"Ошибка фоновой задачи ({type(exc).__name__})"
            time.sleep(300)
        time.sleep(30)


def esc(value):
    return html.escape(str(value), quote=True)


def form(action, body, css="", *, token=TOKEN, multipart=False):
    enctype = ' enctype="multipart/form-data"' if multipart else ""
    return (f'<form method="post" class="{css}"{enctype}><input type="hidden" name="token" value="{token}">'
            f'<input type="hidden" name="action" value="{action}">{body}</form>')


def xlsx_products(raw):
    def xml(archive, path):
        if archive.getinfo(path).file_size > 24 * 1024 * 1024:
            raise FormError("Лист Excel слишком большой.")
        content = archive.read(path).decode("utf-8-sig")
        if "<!DOCTYPE" in content.upper():
            raise FormError("Не поддерживается этот формат листа Excel.")
        return ET.fromstring(content)

    def string(element):
        return "".join(node.text or "" for node in element.iter(XLSX_NS + "t"))

    def cells(row, strings):
        values = {}
        for cell in row.findall(XLSX_NS + "c"):
            column = re.match(r"[A-Z]+", cell.get("r", ""))
            if not column or cell.find(XLSX_NS + "f") is not None:
                continue
            value = cell.findtext(XLSX_NS + "v", "")
            kind = cell.get("t")
            if kind == "s":
                value = strings[int(value)]
            elif kind == "inlineStr":
                value = string(cell)
            values[column[0]] = value.strip()
        return values

    try:
        with ZipFile(io.BytesIO(raw)) as archive:
            members = archive.namelist()
            strings = []
            if "xl/sharedStrings.xml" in members:
                strings = [string(entry) for entry in xml(archive, "xl/sharedStrings.xml")]
            sheets = [name for name in members if re.fullmatch(r"xl/worksheets/sheet[0-9]+\.xml", name)]
            for sheet in sheets[:10]:
                rows = iter(xml(archive, sheet).iter(XLSX_NS + "row"))
                for index, row in enumerate(rows):
                    if index >= 20:
                        break
                    headings = {value.casefold(): col for col, value in cells(row, strings).items()}
                    if {"идентификатор", "название товара"} <= headings.keys():
                        return read_product_rows(rows, strings, cells, headings)
    except (BadZipFile, ET.ParseError, OSError, ValueError, IndexError, KeyError, RuntimeError) as exc:
        if isinstance(exc, FormError):
            raise
        raise FormError("Не удалось прочитать Excel. Загрузите выгрузку «Сводный прайс» в формате .xlsx.") from None
    raise FormError("В файле нет колонок «Идентификатор» и «Название товара».")


def read_product_rows(rows, strings, cells, headings):
    products = {}
    invalid = duplicates = 0
    for row in rows:
        values = cells(row, strings)
        if not any(values.values()):
            continue
        item = values.get(headings["идентификатор"], "")
        name = values.get(headings["название товара"], "")
        match = re.fullmatch(r"#?([0-9]{1,120})(?:\.0+)?", item)
        if match:
            item = match[1]
        elif re.fullmatch(r"[0-9]{1,120}(?:\.[0-9]{1,120})?[Ee][+-]?[0-9]{1,3}", item):
            number = Decimal(item)
            item = str(int(number)) if number.adjusted() < 120 and number == number.to_integral_value() else ""
        else:
            item = ""
        if not item or not name or len(name) > 120:
            invalid += 1
            continue
        if item in products:
            duplicates += 1
        else:
            brand = values.get(headings.get("бренд"), "")[:120]
            category = values.get(headings.get("категория"), "")[:120]
            products[item] = (name, brand, category)
    return products, invalid, duplicates


def import_xlsx(raw):
    products, invalid, duplicates = xlsx_products(raw)
    with database() as db:
        db.executemany("INSERT OR IGNORE INTO watch(item,name,target) VALUES(?,?,NULL)",
                       ((item, name) for item, (name, _, _) in products.items()))
        inserted = db.total_changes
        db.executemany("""INSERT INTO watch_metadata VALUES(?,?,?) ON CONFLICT(item) DO UPDATE SET
            brand=coalesce(nullif(excluded.brand,''),watch_metadata.brand),
            category=coalesce(nullif(excluded.category,''),watch_metadata.category)""",
            ((item, brand, category) for item, (_, brand, category) in products.items() if brand or category))
    return inserted, len(products) - inserted, invalid, duplicates


def model_chooser(selected, models, term, checked, form):
    item = esc(selected["item"])
    hidden = f'<input type="hidden" name="item" value="{item}">'
    current = selected["market_title"]
    if current:
        method = "Ручная привязка" if selected["manual_market_title"] else "Автоматическое совпадение"
        current_text = f"{method} · {current}"
    else:
        current_text = "Нет модели в CSV" if selected["market_marked_missing"] else "Модель не привязана"
    search = (f'<form method="get" class="search">{hidden}<input type="hidden" name="choose" value="1">'
              f'<label>Поиск по названию или коду<input name="model_q" value="{esc(term)}" maxlength="120"></label>'
              '<button>Найти модели</button></form>')
    key = model_key(term)
    choices = [row for row in models if key in model_key(row["title"])]
    if choices:
        options = []
        for row in choices:
            title = esc(row["title"])
            price = amount(row["price"]) if row["price"] is not None else "Цена не указана"
            options.append(f'<label class="model-option"><input type="radio" name="market_title" '
                           f'value="{title}" required><span>{title}</span><span>{price}</span></label>')
        binding = form("model_bind", hidden + f'<input type="hidden" name="model_q" value="{esc(term)}">'
                       f'<div class="model-options">{"".join(options)}</div>'
                       '<button>Привязать</button>')
    elif not checked:
        binding = '<p>CSV ещё не загружен. Проверьте источник и нажмите «Обновить цены».</p>'
    elif not models:
        binding = '<p>В загруженном CSV нет моделей.</p>'
    else:
        binding = '<p>По запросу ничего не найдено.</p>'
    status = ""
    if selected["manual_market_title"] or selected["market_marked_missing"]:
        status = form("model_clear", hidden + '<button class="quiet">Вернуть автоматический выбор</button>')
    missing = ""
    if checked and not current and not selected["market_marked_missing"]:
        missing = form("model_missing", hidden + '<button class="quiet">Отметить отсутствие в CSV</button>')
    return (f'<section id="market-model"><h2>Выбор модели · {esc(selected["name"])}</h2>'
            f'<p>ID {item} · {esc(current_text)}</p>{search}{binding}'
            f'<div class="model-actions">{status}{missing}</div><p><a href="?">Вернуться к списку</a></p></section>')


WATCH_STOCK = {"": "Любое", "available": "В наличии", "unconfirmed": "Остаток не подтверждён",
               "no_price": "Нет закупочной цены"}
WATCH_MARKET = {"": "Любая", "priced": "Есть цена", "no_market": "Нет цены",
                "unlinked": "Модель не сопоставлена"}
WATCH_SIGNALS = {"": "Любой", "signal": "Есть сигнал", "none": "Без сигнала"}
WATCH_ORDERS = {
    "signal": "Сначала с сигналом", "cost": "Закупка по возрастанию",
    "gap": "Разница с конкурентом, BYN", "gap_percent": "Разница с конкурентом, %",
    "near_low": "Ближе к минимуму за 90 дней",
}


def watch_view(rows, query):
    keys = {"watch_q": "", "order": "signal", "brand": "", "category": "",
            "stock": "", "market_filter": "", "signal_filter": "", "price_from": "", "price_to": ""}
    filters = {key: query.get(key, [value])[0].strip()[:120] for key, value in keys.items()}
    legacy = query.get("filter", [""])[0]
    if legacy in WATCH_STOCK and not filters["stock"]:
        filters["stock"] = legacy
    if legacy == "no_market" and not filters["market_filter"]:
        filters["market_filter"] = legacy
    if legacy == "signal" and not filters["signal_filter"]:
        filters["signal_filter"] = legacy
    for key, choices in (("stock", WATCH_STOCK), ("market_filter", WATCH_MARKET), ("signal_filter", WATCH_SIGNALS)):
        if filters[key] not in choices:
            filters[key] = ""
    if filters["order"] not in WATCH_ORDERS:
        filters["order"] = "signal"
    term = filters["watch_q"].casefold()
    model_term = model_key(term)
    price_from, price_to = (money(filters[key]) for key in ("price_from", "price_to"))
    visible = []
    for row in rows:
        if filters["signal_filter"] and bool(decision(row)[1]) != (filters["signal_filter"] == "signal"):
            continue
        stock = filters["stock"]
        if stock == "available" and row["buy"] is None:
            continue
        if stock == "unconfirmed" and (row["buy"] is not None or row["unavailable_buy"] is None):
            continue
        if stock == "no_price" and (row["buy"] is not None or row["unavailable_buy"] is not None):
            continue
        market = filters["market_filter"]
        if market == "no_market" and row["market"] is not None:
            continue
        if market == "priced" and row["market"] is None:
            continue
        if market == "unlinked" and row["market_title"]:
            continue
        price = row["buy"] if row["buy"] is not None else row["unavailable_buy"]
        if price_from is not None and (price is None or price < price_from):
            continue
        if price_to is not None and (price is None or price > price_to):
            continue
        if any(filters[key] and row[key] != filters[key] for key in ("brand", "category")):
            continue
        if (term and term not in row["item"].casefold() and term not in row["name"].casefold()
                and (not model_term or model_term not in model_key(row["name"]))):
            continue
        visible.append(row)

    def ranking(row):
        buy, market, low = row["buy"], row["market"], row["low_90"]
        match filters["order"]:
            case "cost":
                return buy if buy is not None else float("inf")
            case "gap" | "gap_percent":
                if buy is None or not market:
                    return float("inf")
                return (buy - market) / market if filters["order"] == "gap_percent" else buy - market
            case "near_low":
                return abs(buy - low) / low if buy is not None and low else float("inf")
            case _:
                return decision(row)[1] or 4

    return sorted(visible, key=lambda row: (ranking(row), row["name"].casefold(), row["item"])), filters


def watch_controls(rows, filters):
    def select(key, caption, choices):
        options = "".join(f'<option value="{esc(value)}" {"selected" if value == filters[key] else ""}>'
                          f'{esc(label)}</option>' for value, label in choices)
        return f'<label>{caption}<select name="{key}">{options}</select></label>'

    catalog = []
    for key, caption in (("brand", "Бренд"), ("category", "Категория")):
        values = sorted({row[key] for row in rows if row[key]} | ({filters[key]} if filters[key] else set()),
                        key=str.casefold)
        if values:
            catalog.append(select(key, caption, [("", "Все")] + [(value, value) for value in values]))
    extra = [
        f'<label>Закупка от<input name="price_from" type="number" step="0.01" min="0" value="{esc(filters["price_from"])}"></label>',
        f'<label>Закупка до<input name="price_to" type="number" step="0.01" min="0" value="{esc(filters["price_to"])}"></label>',
        select("order", "Сортировка", WATCH_ORDERS.items()),
    ]
    opened = " open" if filters["price_from"] or filters["price_to"] or filters["order"] != "signal" else ""
    return (f'<form method="get" action="?#watchlist">'
            f'<div class="fields watch-bar"><label>Поиск в списке<input name="watch_q" maxlength="120" '
            f'value="{esc(filters["watch_q"])}" placeholder="Название или ID"></label>'
            f'{"".join(catalog)}{select("stock", "Наличие", WATCH_STOCK.items())}'
            f'{select("market_filter", "Цена конкурента", WATCH_MARKET.items())}'
            f'{select("signal_filter", "Сигнал", WATCH_SIGNALS.items())}</div>'
            f'<details class="watch-extra"{opened}><summary>Диапазон цен и сортировка</summary>'
            f'<div class="fields">{"".join(extra)}</div></details>'
            '<div class="fields watch-submit"><button>Применить</button><a href="?#watchlist">Сбросить</a></div></form>')


def page(query, error="", addition=None, *, editor=False, csrf_token=TOKEN):
    render_form = partial(form, token=csrf_token)
    search = query.get("q", [""])[0].strip()[:120]
    item = query.get("item", [""])[0][:120]
    choosing = query.get("choose") == ["1"]
    managing_shops = not editor and not item and query.get("shops") == ["1"]
    model_term = query.get("model_q", [""])[0].strip()[:120]
    with database() as db:
        checked = state(db, "checked")
        market_checked = state(db, "market_checked")
        latest = db.execute("SELECT max(id) FROM snapshots").fetchone()[0]
        rows = results(db)
        selected = next((row for row in rows if row["item"] == item), None)
        market_models = db.execute("SELECT title, price FROM market_models ORDER BY title").fetchall() if selected and choosing else []
        market_rows = db.execute("""SELECT datetime(observed,'unixepoch','+3 hours') AS stamp,
            m.shop, m.price FROM market_history m WHERE title=?
            AND NOT EXISTS (SELECT 1 FROM hidden_market_shops h WHERE h.shop=m.shop)
            ORDER BY observed DESC,m.shop LIMIT 200""",
            (selected["market_title"],)).fetchall() if selected and selected["market_title"] else []
        shops = db.execute("""SELECT m.shop, h.shop IS NOT NULL AS hidden
            FROM (SELECT shop FROM market UNION SELECT shop FROM hidden_market_shops) m
            LEFT JOIN hidden_market_shops h ON h.shop=m.shop ORDER BY m.shop COLLATE NOCASE
            """).fetchall() if managing_shops else []
        found = db.execute("""WITH candidates AS (
            SELECT item FROM offers WHERE snapshot=? AND instr(item,?)>0
            UNION SELECT item FROM product_names WHERE instr(item,?)>0
            UNION SELECT item FROM watch WHERE instr(item,?)>0
            UNION SELECT item FROM offers WHERE item=?
        )
        SELECT c.item, p.name FROM candidates c LEFT JOIN product_names p ON p.item=c.item
        ORDER BY c.item<>?, c.item LIMIT 20
        """, (latest, search, search, search, search, search)).fetchall() if search else []
        known = db.execute("SELECT name FROM product_names WHERE item=?", (item,)).fetchone() if item else None
        sources = [] if editor else db.execute("SELECT * FROM sources ORDER BY name").fetchall()
        details = db.execute("""SELECT datetime(t.captured,'unixepoch','+3 hours') AS stamp,
            s.name, o.retail, o.cost, o.stock FROM offers o JOIN snapshots t ON t.id=o.snapshot
            JOIN sources s ON s.id=o.source WHERE o.item=? ORDER BY o.snapshot DESC, s.name LIMIT 200
            """, (item,)).fetchall() if item else []
        chart = purchase_chart(db, item) if item and not choosing else ""
    refreshed = datetime.fromtimestamp(checked, timezone.utc).strftime("%d.%m.%Y %H:%M UTC") if checked else "ожидается"
    market_time = "источник не настроен" if editor else "укажите ссылку в разделе «Источники данных»"
    if OPTIONS.get("competitor_url"):
        market_time = datetime.fromtimestamp(market_checked, timezone.utc).strftime(
            "%d.%m.%Y %H:%M UTC") if market_checked else "ожидается"
    currency = esc(OPTIONS.get("currency", "BYN"))
    add_feedback = ""
    import_result = query.get("import_result")
    if isinstance(import_result, tuple):
        inserted, already, invalid, duplicates = import_result
        add_feedback = (f'<p class="add-feedback" role="status">Добавлено: {inserted}. '
                        f'Уже отслеживаются: {already}. Дубли в файле: {duplicates}. '
                        f'Строки с пропусками или ошибками: {invalid}.</p>')
    if addition:
        added_item, created = addition
        added = next((row for row in rows if row["item"] == added_item), None)
        if added:
            label = "Добавлен в список наблюдения" if created else "Уже в списке наблюдения"
            link = "?" + urllib.parse.urlencode({"item": added_item}) + "#product-details"
            add_feedback = (f'<p class="add-feedback" role="status"><strong>{label}.</strong> '
                            f'<a href="{esc(link)}">{esc(added["name"])} · ID {esc(added_item)}</a></p>')
    cards = []
    visible_rows, watch_filters = watch_view(rows, query) if not (item or managing_shops) else ([], {})
    controls = watch_controls(rows, watch_filters) if rows and watch_filters else ""
    for row in visible_rows:
        reason, signal = decision(row)
        action = render_form("remove", f'<input type="hidden" name="item" value="{esc(row["item"])}">'
                      f'<button class="quiet" aria-label="Убрать из списка: {esc(row["name"])}">Убрать</button>')
        detail = "?" + urllib.parse.urlencode({"item": row["item"]})
        identifier = f'ID {row["item"]}'
        subtitle = f'<span class="sub">{esc(identifier)}</span>'
        buy = amount(row["buy"]) + purchase_trend(row["buy"], row["previous_buy"])
        if row["buy"] is None and row["unavailable_buy"] is not None:
            buy = amount(row["unavailable_buy"]) + '<span class="sub">Остаток не подтверждён</span>'
        elif row["buy"] is None:
            buy = '<span class="sub">Нет закупочной цены</span>'
        if row["buy_supplier"]:
            buy += f'<span class="sub">{esc(row["buy_supplier"])}</span>'
        low = amount(row["low_90"])
        if row["low_90"] is not None:
            context = f'{low_price_date(row)} · {row["low_supplier"]}'
            if row["buy"] == row["low_90"]:
                context = f'Сейчас на минимуме · {context}'
            elif row["buy"] is not None:
                context += f' · сейчас +{amount(row["buy"] - row["low_90"])}'
            low += f'<span class="sub">{esc(context)}</span>'
        if row["market"] is not None:
            shops = esc(", ".join(row["market_shops"]))
            market = f'{amount(row["market"])}<span class="sub market-shops">{shops}</span>'
            gap = competitor_gap(row)
            if gap:
                market += f'<span class="sub">{esc(gap)}</span>'
        elif row["market_title"]:
            market = '<span class="sub">Нет актуальной цены</span>'
        elif row["market_marked_missing"]:
            market = '<span class="sub">Нет модели в CSV</span>'
        else:
            market = '<span class="sub">Модель не привязана</span>'
        status = f'<span class="signal signal-{signal}">{esc(reason)}</span>' if signal else "—"
        note = f'<a href="{esc(detail)}#product-details">{esc(row["note"] or "Добавить комментарий")}</a>'
        cards.append(f'<tr><td><a href="{esc(detail)}#product-details">{esc(row["name"])}</a>'
                     f'{subtitle}</td>'
                     f'<td>{buy}</td><td>{low}</td><td>{market}</td>'
                     f'<td>{amount(row["target"])}</td><td>{status}</td><td class="note">{note}</td><td>{action}</td></tr>')
    roles = {"unassigned": "Автоматически", "supplier": "Поставщик", "ignored": "Не учитывать"}
    source_forms = []
    for source in sources:
        if source["role"] == "parser":
            source_forms.append(f'<p>{esc(source["name"])} <span class="sub">Парсер цен · исключён</span></p>')
            continue
        role = "ignored" if source["role"] == "competitor" else source["role"]
        choices = "".join(f'<option value="{key}" {"selected" if key == role else ""}>{label}</option>'
                          for key, label in roles.items())
        source_forms.append(render_form("role",
            f'<input type="hidden" name="source" value="{esc(source["id"])}">'
            f'<label>{esc(source["name"])}<span class="sub">ID {esc(source["id"])}</span></label>'
            f'<select name="role" aria-label="Роль источника">{choices}</select><button>Сохранить</button>', "source"))
    watched = {row["item"] for row in rows}
    name_input = '<label>Название товара<input name="name" maxlength="120"></label>'
    search_results = []
    for row in found:
        found_id = row["item"]
        link = "?" + urllib.parse.urlencode({"item": found_id})
        control = ('<span class="sub">В списке наблюдения</span>' if found_id in watched else render_form(
            "add", f'<input type="hidden" name="item" value="{esc(found_id)}">'
            f'<input type="hidden" name="q" value="{esc(search)}">'
            f'{"" if row["name"] else name_input}<button>Добавить к отслеживанию</button>', "fields"))
        caption = f'{row["name"]} · ID {found_id}' if row["name"] else f'ID {found_id}'
        search_results.append(f'<li><a href="{esc(link)}">{esc(caption)}</a>{control}</li>')
    input_name = esc(query.get("add_name", [""])[0])
    add = render_form("add", f'<label>ID товара<input name="item" required maxlength="120" autocomplete="off" value="{esc(query.get("add_id", [""])[0])}"></label>'
               f'<label>Название товара<input name="name" maxlength="120" value="{input_name}"></label>'
               '<button>Добавить по ID</button>', "fields")
    upload = render_form("import_xlsx",
        '<label>Выгрузка избранного ShopManager (.xlsx)<input type="file" name="workbook" '
        'accept=".xlsx,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" required></label>'
        '<button>Импортировать избранное</button>', "fields search", multipart=True)
    settings = "" if editor else render_form("settings",
        f'<label>Ссылка на CSV конкурентов<input type="password" autocomplete="off" name="competitor_url" '
        f'value="{esc(OPTIONS.get("competitor_url") or "")}"></label>'
        f'<label>Ссылка на каталог названий (JSON или YML)<input type="password" autocomplete="off" name="catalog_url" '
        f'value="{esc(OPTIONS.get("catalog_url") or "")}"></label>'
        '<button>Сохранить источники</button>', "fields")
    edit = ""
    if selected:
        target = amount(selected["target"]) if selected["target"] is not None else ""
        name = selected["custom_name"]
        if name == f"ID {item}":
            name = ""
        edit = render_form("watch", f'<input type="hidden" name="item" value="{esc(item)}">'
                    f'<label>Название<input name="name" maxlength="120" value="{esc(name)}"></label>'
                    f'<label>Целевая цена закупки<input name="target" inputmode="decimal" value="{target}"></label>'
                    f'<label>Комментарий<textarea name="note" maxlength="500" rows="2">{esc(selected["note"])}</textarea></label>'
                    '<button>Сохранить изменения</button>', "fields")
        model_link = esc("?" + urllib.parse.urlencode({"item": item, "choose": "1"}) + "#market-model")
        model_title = selected["market_title"] or (
            "Нет модели в CSV" if selected["market_marked_missing"] else "Модель не привязана")
        model_action = "Изменить модель конкурента" if selected["market_title"] else "Выбрать модель конкурента"
        edit += f'<p>Модель у конкурентов · {esc(model_title)} <a href="{model_link}">{model_action}</a></p>'
    elif item:
        edit = render_form("add", f'<input type="hidden" name="item" value="{esc(item)}">'
                    f'{"" if known else name_input}<button>Добавить к отслеживанию</button>', "fields")
    history = ""
    if item:
        history_rows = "".join(f'<tr><td>{esc(row["stamp"])}</td><td>{esc(row["name"])}</td>'
                               f'<td>{amount(row["retail"])}</td><td>{amount(row["cost"])}</td>'
                               f'<td>{esc(row["stock"] if row["stock"] is not None else "—")}</td></tr>'
                               for row in details)
        heading = selected["name"] if selected else (known["name"] if known else f"Товар {item}")
        history = f'<section id="product-details"><h2>{esc(heading)}</h2>{edit}{chart}<h3>История цен</h3><div class="table"><table><thead><tr><th>Минск</th>'
        history += '<th>Источник</th><th>Цена</th><th>Закупка</th><th>Остаток</th></tr></thead>'
        history += f'<tbody>{history_rows}</tbody></table></div></section>'
        if market_rows:
            prices = "".join(f'<tr><td>{esc(row["stamp"])}</td><td>{esc(row["shop"])}</td>'
                             f'<td>{amount(row["price"])}</td></tr>' for row in market_rows)
            history += ('<section><h2>История цен конкурентов</h2><div class="table"><table>'
                        '<thead><tr><th>Минск</th><th>Магазин</th><th>Цена</th></tr></thead>'
                        f'<tbody>{prices}</tbody></table></div></section>')
    sync_context = (f'<input type="hidden" name="item" value="{esc(item)}">' if item else "")
    if choosing and item:
        sync_context += ('<input type="hidden" name="choose" value="1">'
                         f'<input type="hidden" name="model_q" value="{esc(model_term)}">')
    if managing_shops:
        sync_context += '<input type="hidden" name="shops" value="1">'
    sync_button = (render_form("logout", '<button class="quiet">Выйти</button>') if editor
                   else render_form("sync", sync_context + '<button class="quiet">Обновить цены</button>'))
    chooser = model_chooser(selected, market_models, model_term, market_checked, render_form) if choosing and selected else ""
    administration = "" if editor else (
        f'<section><h2>Источники данных</h2>{settings}</section>'
        f'<details><summary>Роли источников · {len(sources)}</summary>'
        f'<p>Оптовые цены ShopManager учитываются автоматически. Ненужный прайс можно исключить.</p>'
        f'{"".join(source_forms)}</details>')
    add_error = error if query.get("add_error") else ""
    problem_text = (error if not add_error else "") or LAST_ERROR
    problem = f'<p class="error" role="alert">{esc(problem_text)}</p>' if problem_text else ""
    form_problem = f'<p class="error" role="alert">{esc(add_error)}</p>' if add_error else ""
    table = (f'<div class="table" role="region" aria-label="Список наблюдения" tabindex="0">'
             f'<table class="watch-table"><thead><tr><th>Товар</th><th>Закупка</th>'
             f'<th>Мин. за 90 дней</th><th>Конкуренты</th><th>Цель</th>'
             f'<th>Сигнал</th><th>Комментарий</th><th aria-label="Действия"></th></tr></thead><tbody>{"".join(cards)}</tbody></table></div>'
             if cards else ('<p>По выбранным фильтрам ничего не найдено.</p>' if rows else
                            '<p>Добавьте первый товар по ID из выгрузки.</p>'))
    shop_settings = ""
    if managing_shops:
        shop_forms = []
        for shop in shops:
            action = "Показывать" if shop["hidden"] else "Скрыть"
            status = "Скрыт" if shop["hidden"] else "Учитывается"
            shop_forms.append(render_form("market_shop",
                f'<input type="hidden" name="shop" value="{esc(shop["shop"])}">'
                f'<input type="hidden" name="hidden" value="{int(not shop["hidden"])}">'
                f'<label>{esc(shop["shop"])}<span class="sub">{status}</span></label>'
                f'<button class="quiet" aria-label="{action} {esc(shop["shop"])}">{action}</button>', "source"))
        shop_settings = ('<section><h2>Магазины конкурентов</h2>'
                         '<p>Скрытые магазины исключаются из сравнения, истории в карточках и сводок.</p>'
                         + ("".join(shop_forms) or "<p>Список появится после загрузки CSV конкурентов.</p>")
                         + "</section>")
    dashboard = f"""
    <section id="watchlist"><h2>Список наблюдения · {len(visible_rows)} из {len(rows)}</h2>{controls}{table}</section>
    <section><h2>Добавить товар</h2>{add_feedback}{form_problem}
    <p>Если название есть в каталоге, достаточно ID. Для остальных товаров укажите название при добавлении.</p>
    {add}
    <form method="get" class="search"><label>Поиск ID в последней выгрузке<input name="q" value="{esc(search)}"></label>
    <button class="quiet">Найти</button></form>
    {"<ul class='matches'>" + "".join(search_results) + "</ul>" if search_results else ("<p>Совпадений нет</p>" if search else "")}
    {upload}</section>{administration}"""
    navigation = '<p><a href="?#watchlist">← К списку наблюдения</a></p>' if item or managing_shops else ""
    if choosing and selected:
        card_url = esc("?" + urllib.parse.urlencode({"item": item}) + "#product-details")
        navigation = f'<p><a href="{card_url}">← К карточке товара</a></p>'
    shops_link = '<br><a href="?shops=1">Настроить магазины</a>' if not (editor or item or managing_shops) else ""
    return f"""<!doctype html><html lang="ru"><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Монитор закупок</title><style>
    *{{box-sizing:border-box}} body{{margin:0;background:#f1f5f4;color:#182d28;font:16px system-ui,sans-serif}}
    main{{max-width:1840px;margin:auto;padding:clamp(12px,2vw,32px)}}
    header{{display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap}}
    h1{{font-size:clamp(30px,5vw,48px);margin:0;letter-spacing:-.045em}} h2{{margin:0 0 16px;font-size:21px}}
    p{{line-height:1.5}} header p,.sub{{color:#526761}} .sub{{display:block;font-size:14px}}
    section,details{{background:white;border:1px solid #dce6e3;border-radius:16px;margin-top:18px;padding:20px}}
    .fields,.source,.search{{display:flex;flex-wrap:wrap;align-items:end;gap:12px}} .source{{padding:12px 0;border-bottom:1px solid #edf1ef}}
    .source label{{flex:1 1 210px}} .fields label{{flex:1 1 155px}} label{{display:block;font-size:14px}}
    input,select,button,textarea{{font:inherit;padding:9px 12px;border:1px solid #a9bcb5;border-radius:8px}}
    textarea{{display:block;width:100%;margin-top:5px;resize:vertical}}
    input{{display:block;width:100%;margin-top:5px}} button{{background:#124c3c;color:white;border-color:#124c3c;cursor:pointer}}
    .quiet{{color:#124c3c;background:white;border-color:#a9bcb5}} a{{color:#075840;font-weight:600}}
    .table{{overflow:auto}} table{{border-collapse:collapse;width:100%;text-align:left}}
    td,th{{border-bottom:1px solid #edf1ef;padding:11px;white-space:nowrap}} th{{font-size:14px}}
    summary{{cursor:pointer;font-weight:650}} .error{{color:#9b2828}}
    .matches{{list-style:none;padding:0}} .matches li{{display:flex;flex-wrap:wrap;gap:14px;align-items:center;margin:8px 0}}
    .search{{margin-top:16px}}
    .add-feedback{{padding:10px 12px;background:#e1f2e9;border-radius:8px}}
    .signal{{display:inline-block;padding:4px 8px;border-radius:6px;background:#e1f2e9;color:#164e38}}
    .signal-2{{background:#fff0cf;color:#704600}} .signal-3{{background:#e8eff9;color:#234566}}
    .watch-table{{min-width:800px;table-layout:fixed;font-variant-numeric:tabular-nums}}
    .watch-table th,.watch-table td{{padding:10px 8px;white-space:normal;overflow-wrap:anywhere;vertical-align:middle}}
    .watch-table th:nth-child(1){{width:20%}}
    .watch-table th:nth-child(2){{width:11%}}
    .watch-table th:nth-child(3){{width:10%}}
    .watch-table th:nth-child(4){{width:14%}}
    .watch-table th:nth-child(5){{width:8%}}
    .watch-table th:nth-child(6){{width:14%}}
    .watch-table th:nth-child(7){{width:15%}}
    .watch-table th:nth-child(8){{width:8%}}
    .watch-table tr:hover td{{background:#f7faf9}}
    .watch-table td.note{{white-space:pre-wrap}}
    .watch-table td:last-child button{{padding:5px 6px;font-size:14px}}
    .price-trend{{font-weight:750}} .price-trend.down{{color:#16724e}} .price-trend.up{{color:#a72932}}
    .watch-bar label{{min-width:140px}}
    .watch-bar select{{display:block;width:100%;margin-top:5px}}
    .watch-submit{{margin-bottom:12px;align-items:center}}
    .watch-extra{{padding:10px 0;border:0;margin:8px 0 12px}}
    .watch-extra .fields{{margin-top:10px}} .watch-extra select{{display:block;width:100%;margin-top:5px}}
    @media(max-width:600px){{section,details{{padding:12px}}}}
    h1 a{{color:inherit;text-decoration:none;font-weight:inherit}} h1 a:hover{{text-decoration:underline}}
    .model-options{{margin:14px 0;max-height:24rem;overflow:auto;border:1px solid #dce6e3}}
    .model-option{{display:grid;grid-template-columns:auto 1fr auto;gap:12px;align-items:center;
      padding:10px;border-bottom:1px solid #edf1ef;cursor:pointer}}
    .model-option input{{width:auto;margin:0}} .model-actions{{display:flex;flex-wrap:wrap;gap:12px;margin-top:12px}}
    .purchase-chart{{position:relative;aspect-ratio:2;max-width:36rem;
      margin:12px 4px 8px;background:repeating-linear-gradient(to bottom,#dce6e3 0 1px,transparent 1px 25%);
      box-shadow:inset 0 -1px #dce6e3}}
    .chart-line,.chart-point{{position:absolute;background:#126451}}
    .chart-line{{height:2px;transform-origin:left center;margin-top:-1px;pointer-events:none}}
    .chart-gap{{background:repeating-linear-gradient(to right,#126451 0 4px,transparent 4px 8px)}}
    .chart-point{{width:7px;height:7px;border-radius:50%;transform:translate(-50%,-50%)}}
    .chart-dates{{display:flex;justify-content:space-between;max-width:36rem;color:#526761;font-size:14px}}
    .chart-values{{padding:10px 14px;margin-top:10px}}
    </style><main><header><div><h1><a href="?">Монитор закупок</a></h1><p>ShopManager · {refreshed}<br>
    Конкуренты · {market_time} · {currency}{shops_link}</p></div>{sync_button}</header>
    {navigation}{problem}{form_problem if item else ""}
    {(chooser or history) if item else (shop_settings if managing_shops else dashboard)}
    </main></html>"""


class Handler(BaseHTTPRequestHandler):
    editor = False
    csrf_token = TOKEN

    def verify_request(self):
        if os.getenv("HA_INGRESS") == "1" and self.client_address[0] != "172.30.32.2":
            self.send_error(403)
            return False
        return True

    def log_message(self, *args):
        pass

    def render(self, query, error="", status=200, addition=None):
        content = page(query, error, addition, editor=self.editor, csrf_token=self.csrf_token).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        if self.editor:
            self.send_header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; "
                             "form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self):
        if self.verify_request():
            self.render(urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query))

    def do_POST(self):
        global LAST_ERROR
        if not self.verify_request():
            return
        data = {}
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if self.headers.get_content_type() == "multipart/form-data":
                self.upload_workbook(length)
                return
            if not 0 < length <= 16384:
                self.send_error(413)
                return
            data = dict(urllib.parse.parse_qsl(self.rfile.read(length).decode(),
                                             keep_blank_values=True, max_num_fields=10))
            token = data.get("token", "").encode("utf-8")
            if not secrets.compare_digest(token, self.csrf_token.encode("ascii")):
                self.send_error(403)
                return
            action = data.get("action")
            if self.editor and action == "logout":
                self.logout()
                return
            if self.editor and action not in EDITOR_ACTIONS:
                self.send_error(403)
                return
            if action == "sync":
                query = {key: [data[key]] for key in ("item", "choose", "model_q", "shops") if key in data}
                if not LOCK.acquire(blocking=False):
                    self.render(query, "Уже выполняется фоновая задача.", 409)
                    return
                try:
                    LAST_ERROR = sync()
                finally:
                    LOCK.release()
                self.render(query)
                return
            else:
                created = self.change(data, action)
            if action == "add":
                query = {"q": [data["q"]]} if data.get("q") else {}
                self.render(query, addition=(data["item"].strip(), created))
            elif action == "watch":
                self.render({"item": [data["item"].strip()]})
            elif action in {"model_bind", "model_clear", "model_missing"}:
                self.render({"item": [data["item"].strip()], "choose": ["1"],
                             "model_q": [data.get("model_q", "")]})
            elif action == "market_shop":
                self.render({"shops": ["1"]})
            else:
                self.render({})
        except FormError as exc:
            query = {}
            if data.get("action") == "market_shop":
                query["shops"] = ["1"]
            if data.get("action") == "add":
                query["add_error"] = ["1"]
            if data.get("q"):
                query["q"] = [data["q"]]
            elif data.get("action") == "add":
                query["add_id"] = [data.get("item", "")]
                query["add_name"] = [data.get("name", "")]
            elif data.get("item"):
                query["item"] = [data["item"]]
                if data.get("action") in {"model_bind", "model_clear", "model_missing"}:
                    query["choose"] = ["1"]
                    query["model_q"] = [data.get("model_q", "")]
            self.render(query, str(exc), 400)
        except (ValueError, KeyError, sqlite3.Error, OSError) as exc:
            self.render({}, f"Операция не выполнена ({type(exc).__name__}).", 400)
        except Exception as exc:
            self.render({}, f"Данные не обработаны ({type(exc).__name__}).", 502)

    def upload_workbook(self, length):
        try:
            if not 0 < length <= IMPORT_LIMIT + 4096:
                raise FormError("Размер файла Excel должен быть не больше 5 МБ.")
            content_type = self.headers["Content-Type"].encode("ascii")
            message = BytesParser(policy=email_policy).parsebytes(
                b"Content-Type: " + content_type + b"\r\n\r\n" + self.rfile.read(length))
            if not message.is_multipart():
                raise FormError("Не удалось получить файл Excel.")
            fields = {part.get_param("name", header="content-disposition"): part
                      for part in message.iter_parts()
                      if part.get_content_disposition() == "form-data"}
            payload = {name: part.get_payload(decode=True) for name, part in fields.items()}
            if not secrets.compare_digest(payload.get("token") or b"", self.csrf_token.encode("ascii")):
                self.send_error(403)
                return
            if payload.get("action") != b"import_xlsx":
                self.send_error(400)
                return
            entry = fields.get("workbook")
            raw = payload.get("workbook")
            if not entry or not (entry.get_filename() or "").lower().endswith(".xlsx") or not raw:
                raise FormError("Выберите выгрузку ShopManager в формате .xlsx.")
            if len(raw) > IMPORT_LIMIT:
                raise FormError("Размер файла Excel должен быть не больше 5 МБ.")
            counts = import_xlsx(raw)
            self.render({"import_result": counts})
        except FormError as exc:
            self.render({"add_error": ["1"]}, str(exc), 400)

    def change(self, data, action):
        if action == "settings":
            self.save_settings(data)
            return
        with database() as db:
            if action == "market_shop":
                shop, hidden = data.get("shop", ""), data.get("hidden")
                if hidden == "0":
                    db.execute("DELETE FROM hidden_market_shops WHERE shop=?", (shop,))
                elif hidden == "1" and db.execute(
                    "SELECT 1 FROM market WHERE shop=? LIMIT 1", (shop,)
                ).fetchone():
                    db.execute("INSERT OR IGNORE INTO hidden_market_shops VALUES(?)", (shop,))
                else:
                    raise FormError("Магазин отсутствует в текущей выгрузке.")
                return
            if action == "role" and data.get("role") in {"unassigned", "supplier", "ignored"}:
                db.execute("UPDATE sources SET role=? WHERE id=? AND role<>'parser'",
                           (data["role"], data["source"]))
                return
            item = data.get("item", "").strip()
            if not 0 < len(item) <= 120:
                raise FormError("Введите ID товара длиной до 120 символов.")
            if action == "remove":
                db.execute("DELETE FROM watch WHERE item=?", (item,))
                db.execute("DELETE FROM market_links WHERE item=?", (item,))
                db.execute("DELETE FROM market_missing WHERE item=?", (item,))
                db.execute("DELETE FROM watch_notes WHERE item=?", (item,))
                db.execute("DELETE FROM watch_metadata WHERE item=?", (item,))
            elif action == "add":
                existing = db.execute("SELECT name FROM watch WHERE item=?", (item,)).fetchone()
                if existing:
                    return False
                name = data.get("name", "").strip()
                if len(name) > 120:
                    raise FormError("Название должно быть не длиннее 120 символов.")
                if not name and not db.execute("SELECT 1 FROM product_names WHERE item=?", (item,)).fetchone():
                    raise FormError("Для этого ID нет названия в каталоге. Введите название товара.")
                return db.execute("INSERT OR IGNORE INTO watch VALUES(?,?,NULL)", (item, name)).rowcount == 1
            elif action in {"model_bind", "model_clear", "model_missing"}:
                if not db.execute("SELECT 1 FROM watch WHERE item=?", (item,)).fetchone():
                    raise FormError("Сначала добавьте товар к отслеживанию.")
                if action == "model_bind":
                    title = data.get("market_title", "")
                    if not db.execute("SELECT 1 FROM market WHERE title=?", (title,)).fetchone():
                        raise FormError("Выберите строку из текущей выгрузки CSV.")
                    db.execute("REPLACE INTO market_links VALUES(?,?)", (item, title))
                    db.execute("DELETE FROM market_missing WHERE item=?", (item,))
                else:
                    db.execute("DELETE FROM market_links WHERE item=?", (item,))
                    if action == "model_missing":
                        db.execute("INSERT OR IGNORE INTO market_missing VALUES(?)", (item,))
                    else:
                        db.execute("DELETE FROM market_missing WHERE item=?", (item,))
            elif action == "watch":
                name = data.get("name", "").strip()
                target = data.get("target", "").strip()
                note = data.get("note")
                if note is not None and len(note.strip()) > 500:
                    raise FormError("Комментарий должен быть не длиннее 500 символов.")
                if len(name) > 120 or (target and money(target) is None):
                    raise FormError("Проверьте название и закупочный лимит.")
                db.execute("REPLACE INTO watch VALUES(?,?,?)", (item, name, money(target)))
                if note is not None:
                    db.execute("REPLACE INTO watch_notes VALUES(?,?)", (item, note.strip()))
            else:
                raise ValueError("Unknown action")

    def save_settings(self, data):
        values = {key: data.get(key, "").strip() for key in URL_KEYS}
        for value in values.values():
            if not value:
                continue
            try:
                url = urllib.parse.urlsplit(value)
                if url.scheme == "https" and url.hostname and not url.username and not url.password:
                    continue
            except ValueError:
                pass
            raise FormError("Укажите полную HTTPS-ссылку на источник.")
        with LOCK:
            with database() as db:
                for key, state_key in (("competitor_url", "market_checked"), ("catalog_url", "catalog_checked")):
                    if values[key] != OPTIONS.get(key, ""):
                        db.execute("DELETE FROM state WHERE key=?", (state_key,))
                db.executemany("REPLACE INTO settings VALUES(?,?)", values.items())
            OPTIONS.update(values)


class EditorHandler(Handler):
    editor = True
    proxy_ip = None
    credentials = ()
    sessions = {}
    session_lock = threading.Lock()

    def allowed_proxy(self):
        if self.client_address[0] != self.proxy_ip:
            self.send_error(403)
            return False
        return True

    def cookie(self, name):
        try:
            entry = SimpleCookie(self.headers.get("Cookie", "")).get(name)
        except CookieError:
            return ""
        return entry.value if entry else ""

    def set_cookie(self, name, value, age):
        self.send_header("Set-Cookie", f"{name}={value}; Max-Age={age}; Path=/; Secure; HttpOnly; SameSite=Strict")

    def verify_request(self):
        if not self.allowed_proxy():
            return False
        self.session_id = self.cookie(EDITOR_SESSION)
        with self.session_lock:
            session = self.sessions.get(self.session_id)
        if session and session[0] > time.monotonic():
            self.csrf_token = session[1]
            return True
        if self.command == "GET":
            self.login_page()
        else:
            self.send_error(403)
        return False

    def login_page(self, error="", status=200):
        token = secrets.token_urlsafe(32)
        alert = f'<p role="alert">{esc(error)}</p>' if error else ""
        content = f"""<!doctype html><html lang="ru"><meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1">
        <title>Вход · Монитор закупок</title><style>
        body{{font:16px system-ui,sans-serif;background:#f1f5f4;color:#182d28;margin:0;padding:24px}}
        main{{max-width:380px;margin:8vh auto;background:white;padding:24px;border-radius:16px}}
        label{{display:block;margin:12px 0}} input,button{{box-sizing:border-box;font:inherit;padding:10px}}
        input{{display:block;width:100%;margin-top:5px}} button{{background:#124c3c;color:white;border:0;border-radius:8px}}
        </style><main><h1>Монитор закупок</h1>{alert}
        <form action="?login=1" method="post"><input type="hidden" name="token" value="{token}">
        <label>Логин<input name="username" autocomplete="username" required maxlength="254"></label>
        <label>Пароль<input type="password" name="password" autocomplete="current-password" required maxlength="128"></label>
        <button>Войти</button></form></main></html>""".encode("utf-8")
        self.send_response(status)
        self.set_cookie(EDITOR_LOGIN, token, 600)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; "
                         "form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def do_POST(self):
        if urllib.parse.urlsplit(self.path).query != "login=1":
            super().do_POST()
            return
        if not self.allowed_proxy():
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 4096:
                raise ValueError
            data = dict(urllib.parse.parse_qsl(self.rfile.read(length).decode(), max_num_fields=3))
            token = data.get("token", "").encode()
            cookie = self.cookie(EDITOR_LOGIN).encode()
            if not cookie or not secrets.compare_digest(token, cookie):
                self.login_page("Откройте форму входа заново.", 403)
                return
            name = data.get("username", "").strip()
            password = data.get("password", "")
            digest = hashlib.sha256(f"{name}:{password}".encode()).digest()
        except (ValueError, OSError):
            self.login_page("Проверьте данные и попробуйте снова.", 400)
            return
        if not any(secrets.compare_digest(digest, entry) for entry in self.credentials):
            self.login_page("Неверный логин или пароль.", 401)
            return
        session_id = secrets.token_urlsafe(32)
        now = time.monotonic()
        old_session_id = self.cookie(EDITOR_SESSION)
        with self.session_lock:
            for key, session in list(self.sessions.items()):
                if session[0] <= now or key == old_session_id:
                    self.sessions.pop(key)
            self.sessions[session_id] = (now + EDITOR_SESSION_SECONDS, secrets.token_urlsafe(32))
        self.redirect_to_main(session_id, EDITOR_SESSION_SECONDS)

    def logout(self):
        with self.session_lock:
            self.sessions.pop(self.session_id, None)
        self.redirect_to_main("", 0)

    def redirect_to_main(self, session_id, age):
        self.send_response(303)
        self.set_cookie(EDITOR_SESSION, session_id, age)
        self.set_cookie(EDITOR_LOGIN, "", 0)
        self.send_header("Location", "?")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", "0")
        self.end_headers()


class EditorConfigError(ValueError):
    pass


def editor_access(options):
    accounts = options.get("editor_accounts") or []
    proxy = options.get("editor_proxy_ip")
    if not accounts and not proxy:
        return None
    try:
        address = ipaddress.IPv4Address(proxy)
    except ValueError:
        raise EditorConfigError("editor_proxy_ip: укажите IPv4-адрес HTTPS-прокси.") from None
    if address.is_unspecified or address.is_multicast or not accounts:
        raise EditorConfigError("Укажите адрес прокси и хотя бы одну учётную запись редактора.")
    usernames = set()
    credentials = []
    for index, account in enumerate(accounts, 1):
        name, password = account.get("username"), account.get("password")
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_.@+\-]{1,254}", name):
            raise EditorConfigError(f"editor_accounts, запись {index}: укажите логин или email.")
        if name in usernames:
            raise EditorConfigError(f"editor_accounts, запись {index}: логин уже встречается в списке.")
        if (not isinstance(password, str) or not 15 <= len(password) <= 128
                or not password.isascii() or not password.isprintable()):
            raise EditorConfigError(f"editor_accounts, запись {index}: пароль должен содержать "
                                    "15–128 печатных ASCII-символов.")
        usernames.add(name)
        credentials.append(hashlib.sha256(f"{name}:{password}".encode("ascii")).digest())
    return str(address), tuple(credentials)


def main():
    global OPTIONS
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--import-file", type=Path)
    parser.add_argument("--bind", default=os.getenv("BIND", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=8099)
    args = parser.parse_args()
    config = Path(os.getenv("OPTIONS_FILE", str(DATA / "options.json")))
    if config.exists():
        OPTIONS = json.loads(config.read_text(encoding="utf-8"))
    initialize()
    with database() as db:
        OPTIONS.update({row["key"]: row["value"] for row in db.execute("SELECT key,value FROM settings")})
    if args.import_file:
        if args.import_file.stat().st_size > LIMIT:
            parser.error("Feed too large")
        print(f"Imported {ingest(args.import_file.read_bytes())} offers.")
        return
    if args.bind != "127.0.0.1" and os.getenv("HA_INGRESS") != "1":
        parser.error("Non-local binding is reserved for Home Assistant Ingress")
    threading.Thread(target=worker, daemon=True).start()
    with ThreadingHTTPServer((args.bind, args.port), Handler) as server:
        if os.getenv("HA_INGRESS") == "1":
            try:
                access = editor_access(OPTIONS)
                if access:
                    EditorHandler.proxy_ip, EditorHandler.credentials = access
                    editors = ThreadingHTTPServer((args.bind, EDITOR_PORT), EditorHandler)
                    threading.Thread(target=editors.serve_forever, daemon=True).start()
            except EditorConfigError as exc:
                print(f"Вход для коллег отключён. {exc}", flush=True)
            except (ValueError, KeyError, TypeError, OSError) as exc:
                print(f"Editor access disabled: {type(exc).__name__}. Check app options and port.",
                      flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
