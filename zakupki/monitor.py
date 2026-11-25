import csv
import hashlib
import html
import io
import json
import os
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
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

LIMIT = 32 * 1024 * 1024
DATA = Path(os.getenv("DATA_DIR", "/data"))
TOKEN = secrets.token_urlsafe(32)
LOCK = threading.Lock()
OPTIONS = {}
LAST_ERROR = ""
URL_KEYS = ("competitor_url", "catalog_url")


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
            CREATE TABLE IF NOT EXISTS market (
                title TEXT NOT NULL, shop TEXT NOT NULL, price INTEGER,
                PRIMARY KEY(title,shop)
            );
            CREATE TABLE IF NOT EXISTS market_history (
                observed INTEGER NOT NULL, title TEXT NOT NULL, shop TEXT NOT NULL,
                price INTEGER NOT NULL, PRIMARY KEY(title,observed,shop)
            );
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS product_names (item TEXT PRIMARY KEY, name TEXT NOT NULL);
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
        catalog[str(price["id"])] = (str(key), str(price.get("priceConfigName") or key))
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
            source, name = catalog.get(price_id, (price_id, price_id))
            sources[source] = name
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
        previous = db.execute("SELECT digest FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()
        if previous is None or previous["digest"] != digest:
            cursor = db.execute(
                "INSERT INTO snapshots(digest,captured,generated) VALUES(?,?,?)",
                (digest, now, generated),
            )
            role = OPTIONS.get("feed_role", "unassigned") if list(sources) == ["feed"] else "unassigned"
            db.executemany("""INSERT INTO sources(id,name,role) VALUES(?,?,?)
                ON CONFLICT(id) DO UPDATE SET name=excluded.name""",
                ((identifier, name, role) for identifier, name in sources.items()))
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


def results(db):
    fresh = bool(OPTIONS.get("competitor_url")) and (
        time.time() - state(db, "market_checked") <= 2 * int(OPTIONS.get("poll_hours", 6)) * 3600
    )
    return db.execute("""
        WITH recent AS (SELECT max(id) AS id FROM snapshots)
        SELECT w.item, w.target, w.name AS custom_name,
          coalesce(nullif(nullif(w.name,'ID ' || w.item),''), p.name, l.title,
                   'Название не указано') AS name,
          min(CASE WHEN s.role='supplier' THEN o.cost END) AS buy,
          (SELECT min(m.price) FROM market m WHERE ? AND m.title=l.title) AS market,
          l.title AS market_title,
          count(o.source) AS available
        FROM watch w
        LEFT JOIN offers o ON o.item=w.item AND o.snapshot=(SELECT id FROM recent)
          AND o.stock>0
        LEFT JOIN sources s ON s.id=o.source
        LEFT JOIN market_links l ON l.item=w.item
        LEFT JOIN product_names p ON p.item=w.item
        GROUP BY w.item ORDER BY w.name COLLATE NOCASE
    """, (fresh,)).fetchall()


def decision(row):
    if row["buy"] is None:
        return "", 0
    if row["target"] and row["buy"] <= row["target"]:
        return "В пределах закупочного лимита", 1
    if row["market"] and 100 * (row["market"] - row["buy"]) >= int(
        OPTIONS.get("min_gap_percent", 15)
    ) * row["market"]:
        return "Разница с ценой конкурента", 2
    return "", 0


def amount(value):
    return "—" if value is None else f"{Decimal(value) / 100:.2f}"


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
        for row in candidates[:15]:
            lines.append(f"{row['name'][:80]} [{row['item'][:40]}] · закупка {amount(row['buy'])}"
                         f" · рынок {amount(row['market'])} · {decision(row)[0]}")
        if len(candidates) > 15:
            lines.append(f"Всего позиций: {len(candidates)}. Полный список в приложении.")
        deliver("\n".join(lines)[:3900])
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


def form(action, body, css=""):
    return (f'<form method="post" class="{css}"><input type="hidden" name="token" value="{TOKEN}">'
            f'<input type="hidden" name="action" value="{action}">{body}</form>')


def page(query, error=""):
    search = query.get("q", [""])[0].strip()[:120]
    item = query.get("item", [""])[0][:120]
    with database() as db:
        checked = state(db, "checked")
        market_checked = state(db, "market_checked")
        latest = db.execute("SELECT max(id) FROM snapshots").fetchone()[0]
        rows = results(db)
        selected = next((row for row in rows if row["item"] == item), None)
        market_titles = db.execute("SELECT DISTINCT title FROM market ORDER BY title").fetchall() if selected else []
        market_rows = db.execute("""SELECT datetime(observed,'unixepoch','+3 hours') AS stamp,
            shop, price FROM market_history WHERE title=? ORDER BY observed DESC,shop LIMIT 200""",
            (selected["market_title"],)).fetchall() if selected and selected["market_title"] else []
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
        sources = db.execute("SELECT * FROM sources ORDER BY name").fetchall()
        details = db.execute("""SELECT datetime(t.captured,'unixepoch','+3 hours') AS stamp,
            s.name, o.retail, o.cost, o.stock FROM offers o JOIN snapshots t ON t.id=o.snapshot
            JOIN sources s ON s.id=o.source WHERE o.item=? ORDER BY o.snapshot DESC, s.name LIMIT 200
            """, (item,)).fetchall() if item else []
    refreshed = datetime.fromtimestamp(checked, timezone.utc).strftime("%d.%m.%Y %H:%M UTC") if checked else "ожидается"
    market_time = "укажите ссылку в разделе «Источники данных»"
    if OPTIONS.get("competitor_url"):
        market_time = datetime.fromtimestamp(market_checked, timezone.utc).strftime(
            "%d.%m.%Y %H:%M UTC") if market_checked else "ожидается"
    currency = esc(OPTIONS.get("currency", "BYN"))
    cards = []
    for row in sorted(rows, key=lambda row: not decision(row)[1]):
        reason = decision(row)[0]
        action = form("remove", f'<input type="hidden" name="item" value="{esc(row["item"])}">'
                      '<button class="quiet">Убрать из списка</button>')
        detail = "?" + urllib.parse.urlencode({"item": row["item"]})
        identifier = f'ID {row["item"]}'
        subtitle = f'<span class="sub">{esc(identifier)}</span>'
        cards.append(f'<tr><td><a href="{esc(detail)}">{esc(row["name"])}</a>'
                     f'{subtitle}</td>'
                     f'<td>{amount(row["buy"])}</td><td>{amount(row["market"])}</td>'
                     f'<td>{amount(row["target"])}</td><td>{esc(reason or "—")}</td><td>{action}</td></tr>')
    roles = {"unassigned": "Не учитывать", "supplier": "Поставщик"}
    source_forms = []
    for source in sources:
        role = "supplier" if source["role"] == "supplier" else "unassigned"
        choices = "".join(f'<option value="{key}" {"selected" if key == role else ""}>{label}</option>'
                          for key, label in roles.items())
        source_forms.append(form("role",
            f'<input type="hidden" name="source" value="{esc(source["id"])}">'
            f'<label>{esc(source["name"])}<span class="sub">ID {esc(source["id"])}</span></label>'
            f'<select name="role" aria-label="Роль источника">{choices}</select><button>Сохранить</button>', "source"))
    watched = {row["item"] for row in rows}
    name_input = '<label>Название товара<input name="name" maxlength="120"></label>'
    search_results = []
    for row in found:
        found_id = row["item"]
        link = "?" + urllib.parse.urlencode({"item": found_id})
        control = ('<span class="sub">В списке наблюдения</span>' if found_id in watched else form(
            "add", f'<input type="hidden" name="item" value="{esc(found_id)}">'
            f'<input type="hidden" name="q" value="{esc(search)}">'
            f'{"" if row["name"] else name_input}<button>Добавить к отслеживанию</button>', "fields"))
        caption = f'{row["name"]} · ID {found_id}' if row["name"] else f'ID {found_id}'
        search_results.append(f'<li><a href="{esc(link)}">{esc(caption)}</a>{control}</li>')
    add = form("add", f'<label>ID товара<input name="item" required maxlength="120" value="{esc(query.get("add_id", [""])[0])}"></label>'
               f'{name_input}<button>Добавить по ID</button>', "fields")
    settings = form("settings",
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
        title = esc(selected["market_title"] or "")
        options = "".join(f'<option value="{esc(row["title"])}"></option>' for row in market_titles)
        edit = form("watch", f'<input type="hidden" name="item" value="{esc(item)}">'
                    f'<label>Название<input name="name" maxlength="120" value="{esc(name)}"></label>'
                    f'<label>Закупочный лимит<input name="target" inputmode="decimal" value="{target}"></label>'
                    f'<label>Модель у конкурентов<input name="market_title" list="market-titles" value="{title}"></label>'
                    f'<datalist id="market-titles">{options}</datalist>'
                    '<button>Сохранить изменения</button>', "fields")
    elif item:
        edit = form("add", f'<input type="hidden" name="item" value="{esc(item)}">'
                    f'{"" if known else name_input}<button>Добавить к отслеживанию</button>', "fields")
    history = ""
    if item:
        history_rows = "".join(f'<tr><td>{esc(row["stamp"])}</td><td>{esc(row["name"])}</td>'
                               f'<td>{amount(row["retail"])}</td><td>{amount(row["cost"])}</td>'
                               f'<td>{esc(row["stock"] if row["stock"] is not None else "—")}</td></tr>'
                               for row in details)
        heading = selected["name"] if selected else (known["name"] if known else f"Товар {item}")
        history = f'<section><h2>{esc(heading)}</h2>{edit}<h3>История цен</h3><div class="table"><table><thead><tr><th>Минск</th>'
        history += '<th>Источник</th><th>Цена</th><th>Закупка</th><th>Остаток</th></tr></thead>'
        history += f'<tbody>{history_rows}</tbody></table></div></section>'
        if market_rows:
            prices = "".join(f'<tr><td>{esc(row["stamp"])}</td><td>{esc(row["shop"])}</td>'
                             f'<td>{amount(row["price"])}</td></tr>' for row in market_rows)
            history += ('<section><h2>История цен конкурентов</h2><div class="table"><table>'
                        '<thead><tr><th>Минск</th><th>Магазин</th><th>Цена</th></tr></thead>'
                        f'<tbody>{prices}</tbody></table></div></section>')
    sync_button = form("sync", '<button class="quiet">Обновить цены</button>')
    problem = f'<p class="error" role="alert">{esc(error or LAST_ERROR)}</p>' if error or LAST_ERROR else ""
    table = (f'<div class="table"><table><thead><tr><th>Товар</th><th>Закупка</th><th>Конкуренты</th><th>Лимит</th>'
             f'<th>Основание</th><th></th></tr></thead><tbody>{"".join(cards)}</tbody></table></div>'
             if cards else '<p>Добавьте первый товар по ID из выгрузки.</p>')
    return f"""<!doctype html><html lang="ru"><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Монитор закупок</title><style>
    *{{box-sizing:border-box}} body{{margin:0;background:#f1f5f4;color:#182d28;font:16px system-ui,sans-serif}}
    main{{max-width:1180px;margin:auto;padding:clamp(16px,4vw,48px)}}
    header{{display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap}}
    h1{{font-size:clamp(30px,5vw,48px);margin:0;letter-spacing:-.045em}} h2{{margin:0 0 16px;font-size:21px}}
    p{{line-height:1.5}} header p,.sub{{color:#526761}} .sub{{display:block;font-size:14px}}
    section,details{{background:white;border:1px solid #dce6e3;border-radius:16px;margin-top:18px;padding:20px}}
    .fields,.source,.search{{display:flex;flex-wrap:wrap;align-items:end;gap:12px}} .source{{padding:12px 0;border-bottom:1px solid #edf1ef}}
    .source label{{flex:1 1 210px}} .fields label{{flex:1 1 155px}} label{{display:block;font-size:14px}}
    input,select,button{{font:inherit;padding:9px 12px;border:1px solid #a9bcb5;border-radius:8px}}
    input{{display:block;width:100%;margin-top:5px}} button{{background:#124c3c;color:white;border-color:#124c3c;cursor:pointer}}
    .quiet{{color:#124c3c;background:white;border-color:#a9bcb5}} a{{color:#075840;font-weight:600}}
    .table{{overflow:auto}} table{{border-collapse:collapse;width:100%;text-align:left}}
    td,th{{border-bottom:1px solid #edf1ef;padding:11px;white-space:nowrap}} th{{font-size:14px}}
    summary{{cursor:pointer;font-weight:650}} .error{{color:#9b2828}}
    .matches{{list-style:none;padding:0}} .matches li{{display:flex;flex-wrap:wrap;gap:14px;align-items:center;margin:8px 0}}
    .search{{margin-top:16px}}
    </style><main><header><div><h1>Монитор закупок</h1><p>ShopManager · {refreshed}<br>
    Конкуренты · {market_time} · {currency}</p></div>{sync_button}</header>
    {problem}<section><h2>Список наблюдения · {len(rows)}</h2>{table}</section>
    <section><h2>Добавить товар</h2>
    <p>Если название есть в каталоге, достаточно ID. Для остальных товаров укажите название при добавлении.</p>
    {add}
    <form method="get" class="search"><label>Поиск ID в последней выгрузке<input name="q" value="{esc(search)}"></label>
    <button class="quiet">Найти</button></form>
    {"<ul class='matches'>" + "".join(search_results) + "</ul>" if search_results else ("<p>Совпадений нет</p>" if search else "")}</section>
    {history}<section><h2>Источники данных</h2>{settings}</section>
    <details><summary>Роли источников · {len(sources)}</summary>
    <p>Отметьте оптовые прайсы ShopManager как поставщиков.</p>{"".join(source_forms)}</details>
    </main></html>"""


class Handler(BaseHTTPRequestHandler):
    def verify_request(self):
        if os.getenv("HA_INGRESS") == "1" and self.client_address[0] != "172.30.32.2":
            self.send_error(403)
            return False
        return True

    def log_message(self, *args):
        pass

    def render(self, query, error="", status=200):
        content = page(query, error).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
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
            if not 0 < length <= 8192:
                self.send_error(413)
                return
            data = dict(urllib.parse.parse_qsl(self.rfile.read(length).decode(),
                                             keep_blank_values=True, max_num_fields=10))
            if not secrets.compare_digest(data.get("token", ""), TOKEN):
                self.send_error(403)
                return
            action = data.get("action")
            if action == "sync":
                if not LOCK.acquire(blocking=False):
                    self.render({}, "Уже выполняется фоновая задача.", 409)
                    return
                try:
                    LAST_ERROR = sync()
                finally:
                    LOCK.release()
            else:
                self.change(data, action)
            if action == "add" and data.get("q"):
                self.render({"q": [data["q"]]})
            elif action == "watch":
                self.render({"item": [data["item"].strip()]})
            else:
                self.render({})
        except FormError as exc:
            query = {}
            if data.get("q"):
                query["q"] = [data["q"]]
            elif data.get("action") == "add":
                query["add_id"] = [data.get("item", "")]
            elif data.get("item"):
                query["item"] = [data["item"]]
            self.render(query, str(exc), 400)
        except (ValueError, KeyError, sqlite3.Error, OSError) as exc:
            self.render({}, f"Операция не выполнена ({type(exc).__name__}).", 400)
        except Exception as exc:
            self.render({}, f"Данные не обработаны ({type(exc).__name__}).", 502)

    def change(self, data, action):
        if action == "settings":
            self.save_settings(data)
            return
        with database() as db:
            if action == "role" and data.get("role") in {"unassigned", "supplier"}:
                db.execute("UPDATE sources SET role=? WHERE id=?", (data["role"], data["source"]))
                return
            item = data.get("item", "").strip()
            if not 0 < len(item) <= 120:
                raise FormError("Введите ID товара длиной до 120 символов.")
            if action == "remove":
                db.execute("DELETE FROM watch WHERE item=?", (item,))
                db.execute("DELETE FROM market_links WHERE item=?", (item,))
            elif action == "add":
                existing = db.execute("SELECT name FROM watch WHERE item=?", (item,)).fetchone()
                if existing:
                    return
                name = data.get("name", "").strip()
                if len(name) > 120:
                    raise FormError("Название должно быть не длиннее 120 символов.")
                if not name and not db.execute("SELECT 1 FROM product_names WHERE item=?", (item,)).fetchone():
                    raise FormError("Для этого ID нет названия в каталоге. Введите название товара.")
                db.execute("INSERT OR IGNORE INTO watch VALUES(?,?,NULL)", (item, name))
            elif action == "watch":
                name = data.get("name", "").strip()
                target = data.get("target", "").strip()
                if len(name) > 120 or (target and money(target) is None):
                    raise FormError("Проверьте название и закупочный лимит.")
                market_title = data.get("market_title")
                if market_title is not None:
                    market_title = market_title.strip()
                    if market_title:
                        match = db.execute("""SELECT title FROM market WHERE title=?
                            UNION SELECT title FROM market_links WHERE item=? AND title=?""",
                            (market_title, item, market_title)).fetchone()
                        if not match:
                            raise FormError("Выберите точное название модели из списка конкурентов.")
                        db.execute("REPLACE INTO market_links VALUES(?,?)", (item, market_title))
                    else:
                        db.execute("DELETE FROM market_links WHERE item=?", (item,))
                db.execute("REPLACE INTO watch VALUES(?,?,?)", (item, name, money(target)))
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
    ThreadingHTTPServer((args.bind, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
