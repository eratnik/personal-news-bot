import json
import logging
import re
import secrets
import ssl
import threading
import time
from html import escape
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener

from registry_export import collect, csv_bytes

URL = "https://danger.gskp.by/products-registry"
INGRESS_IP = "172.30.32.2"
SNAPSHOT = Path("/data/registry-response.html")
EXPORT_DIAGNOSTIC = Path("/data/registry-filter-response.html")
EXPORT = Path("/data/registry-export.csv")
BATCH_EXPORT = Path("/data/registry-export-all.csv")
FILTERS = Path("/data/registry-filters.json")
DEFAULT_FILTERS = {
    "name": [
        "холодильник", "морозильник", "стиральная машина", "посудомоечная машина",
        "пылесос", "кондиционер", "варочная панель", "микроволновая печь",
    ],
    "tnved": [],
}
TOKEN = secrets.token_urlsafe()
MAX_BYTES = 5 * 1024 * 1024
batch_running = False
batch_status = ""


class RegistryRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urlsplit(newurl)
        if target.scheme != "https" or target.netloc.lower() != "danger.gskp.by":
            raise HTTPError(newurl, code, "Redirect outside registry", headers, fp)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def registry_tls_context():
    context = ssl.create_default_context()
    # Require the chain to reach a trusted root, even with the issuer supplied here.
    context.verify_flags &= ~ssl.VERIFY_X509_PARTIAL_CHAIN
    context.load_verify_locations(
        cafile=Path(__file__).with_name("globalsign-r6-alphassl-2025.pem")
    )
    return context


def fetch_registry(url):
    request = Request(url, headers={"User-Agent": "DomisRegistry/0.4.0"})
    opener = build_opener(RegistryRedirect(), HTTPSHandler(context=registry_tls_context()))
    with opener.open(request, timeout=12) as response:
        content_type = response.headers.get_content_type()
        body = response.read(MAX_BYTES + 1)
        if content_type != "text/html" or not body.strip() or len(body) > MAX_BYTES:
            raise ValueError(f"Неожиданный ответ: {content_type}, {len(body)} байт")
        if response.geturl() != url:
            raise ValueError("Реестр перенаправил запрос на другой адрес")
        return response.status, body


def save_file(path, data):
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(data)
    temporary.replace(path)


def read_filters():
    if FILTERS.exists():
        return json.loads(FILTERS.read_text(encoding="utf-8"))
    return DEFAULT_FILTERS


def save_filters(fields):
    filters = {}
    for field in DEFAULT_FILTERS:
        lines = fields.get(field, [""])[0].splitlines()
        values = list(dict.fromkeys(line.strip() for line in lines if line.strip()))
        if len(values) > 30 or any(len(value) > 100 for value in values):
            raise ValueError("Можно сохранить до 30 значений каждого вида, до 100 символов на строку")
        if field == "tnved" and any(not re.fullmatch(r"[0-9]{4,10}", value) for value in values):
            raise ValueError("Каждый код ТН ВЭД должен содержать от 4 до 10 цифр")
        filters[field] = values
    save_file(FILTERS, json.dumps(filters, ensure_ascii=False).encode("utf-8"))
    return f"Сохранено ключевых слов: {len(filters['name'])}. Кодов ТН ВЭД: {len(filters['tnved'])}."


def probe():
    status, body = fetch_registry(URL)
    save_file(SNAPSHOT, body)
    return f"HTTP {status}. Получен HTML, {len(body)} байт."


def fetch_rows(field, value, *, all_pages=False):
    last_response = None

    def fetch_page(url):
        nonlocal last_response
        if all_pages:
            time.sleep(0.5)
        last_response = None
        _, last_response = fetch_registry(url)
        return last_response.decode("utf-8")

    try:
        if all_pages:
            return collect(fetch_page, URL, field, value, page_limit=None)
        return collect(fetch_page, URL, field, value)
    except ValueError as error:
        if last_response is not None:
            save_file(EXPORT_DIAGNOSTIC, last_response)
            raise ValueError(f"{error}. Ответ для диагностики сохранён") from error
        raise


def export_registry(field, value):
    rows, total = fetch_rows(field, value)
    save_file(EXPORT, csv_bytes(rows))
    if total == 0:
        return f"По фильтру «{value}» реестр не нашёл записей."
    summary = f"Фильтр «{value}». Выгружено {len(rows)} из {total} записей."
    if len(rows) < total:
        summary += " В CSV сохранены первые две страницы."
    return summary


def run_batch(filters):
    global batch_running, batch_status
    unique = {}
    try:
        for index, (field, value) in enumerate(filters, 1):
            batch_status = f"Проверяется фильтр {index} из {len(filters)} · {value}"
            rows, _ = fetch_rows(field, value, all_pages=True)
            for row in rows:
                unique.setdefault(row["record"], row)
        save_file(BATCH_EXPORT, csv_bytes(unique.values()))
        batch_status = f"Проверено фильтров: {len(filters)}. Уникальных записей: {len(unique)}."
    except (OSError, ValueError) as error:
        batch_status = f"Выгрузка остановилась на фильтре «{value}» · {error}"
    finally:
        batch_running = False
        logging.info("%s", batch_status)


def start_batch():
    global batch_running, batch_status
    if batch_running:
        return "Общая выгрузка уже выполняется."
    filters = [(field, value) for field, values in read_filters().items() for value in values]
    if not filters:
        raise ValueError("Добавьте хотя бы один ключевой запрос или код ТН ВЭД")
    batch_running = True
    batch_status = "Общая выгрузка запущена."
    threading.Thread(target=run_batch, args=(filters,), daemon=True).start()
    return ""


class Handler(BaseHTTPRequestHandler):
    def send_content(self, status, content, mime, filename=None):
        self.send_response(status)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        if filename:
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(content)

    def show_page(self, status_message=""):
        result = f'<p role="status">{escape(status_message)}</p>' if status_message else ""
        download = '<p><a href="./snapshot">Скачать последний ответ HTML</a></p>' if SNAPSHOT.exists() else ""
        csv_download = '<p><a href="./csv">Скачать CSV последней успешной выгрузки</a></p>' if EXPORT.exists() else ""
        diagnostic_download = '<p><a href="./filter-response">Скачать HTML последней ошибки выгрузки</a></p>' if EXPORT_DIAGNOSTIC.exists() else ""
        filters = read_filters()
        batch_download = (
            '<p><a href="./batch-csv">Скачать последний общий CSV</a></p>'
            if BATCH_EXPORT.exists() else ""
        )
        batch_result = f'<p role="status">{escape(batch_status)}</p>' if batch_status else ""
        batch_disabled = " disabled" if batch_running else ""
        options = "".join(
            f'<option value="{escape(field + ":" + value)}">{escape(label + " · " + value)}</option>'
            for field, label in (("name", "Наименование"), ("tnved", "ТН ВЭД"))
            for value in filters[field]
        )
        saved_control = f"""
<form method="post" action="./export">
  <input type="hidden" name="token" value="{TOKEN}">
  <label>Сохранённый фильтр <select name="preset">{options}</select></label>
  <button>Выгрузить CSV</button>
</form>""" if options else ""
        names = escape("\n".join(filters["name"]))
        codes = escape("\n".join(filters["tnved"]))
        page = f"""<!doctype html>
<html lang="ru">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Domis Registry</title>
<style>
body {{ font: 1.05rem system-ui, sans-serif; margin: 3rem auto; max-width: 42rem; padding: 0 1rem; }}
button {{ font: inherit; padding: .6rem 1rem; cursor: pointer; }}
form {{ margin: 1rem 0; }}
textarea {{ display: block; width: 100%; box-sizing: border-box; margin: .4rem 0 .8rem; }}
</style>
<main>
<h1>Реестр опасной продукции</h1>
<form method="post" action="./batch">
  <input type="hidden" name="token" value="{TOKEN}">
  <button{batch_disabled}>Выгрузить все фильтры</button>
</form>
{batch_result}
<p><a href="./">Обновить статус</a></p>
{batch_download}
<hr>
<p>Выгрузка по одному фильтру сохраняет до двух страниц.</p>
{saved_control}
<details>
<summary>Разовый поиск</summary>
<form method="post" action="./export">
  <input type="hidden" name="token" value="{TOKEN}">
  <label>Фильтр
    <select name="field">
      <option value="name">Наименование</option>
      <option value="tnved">Код ТН ВЭД</option>
    </select>
  </label>
  <label>Значение <input name="value" required maxlength="100"></label>
  <button>Выгрузить CSV</button>
</form>
</details>
{result}
{csv_download}
{diagnostic_download}
<details>
<summary>Настройка сохранённых фильтров</summary>
<p>Введите по одному ключевому слову, фразе или коду на строку.</p>
<form method="post" action="./filters">
  <input type="hidden" name="token" value="{TOKEN}">
  <label>Ключевые слова<textarea name="name" rows="8" maxlength="3030">{names}</textarea></label>
  <label>Коды ТН ВЭД<textarea name="tnved" rows="4" maxlength="330">{codes}</textarea></label>
  <button>Сохранить фильтры</button>
</form>
</details>
<hr>
<form method="post" action="./probe">
  <input type="hidden" name="token" value="{TOKEN}">
  <button>Проверить соединение</button>
</form>
{download}
</main>
</html>"""
        self.send_content(200, page.encode(), "text/html; charset=utf-8")

    def do_GET(self):
        if self.client_address[0] != INGRESS_IP:
            return self.send_error(403)
        if self.path == "/":
            return self.show_page()
        if self.path == "/snapshot" and SNAPSHOT.exists():
            return self.send_content(200, SNAPSHOT.read_bytes(), "application/octet-stream", SNAPSHOT.name)
        if self.path == "/csv" and EXPORT.exists():
            return self.send_content(200, EXPORT.read_bytes(), "text/csv; charset=utf-8", EXPORT.name)
        if self.path == "/batch-csv" and BATCH_EXPORT.exists():
            return self.send_content(200, BATCH_EXPORT.read_bytes(), "text/csv; charset=utf-8", BATCH_EXPORT.name)
        if self.path == "/filter-response" and EXPORT_DIAGNOSTIC.exists():
            return self.send_content(
                200, EXPORT_DIAGNOSTIC.read_bytes(), "application/octet-stream", EXPORT_DIAGNOSTIC.name
            )
        self.send_error(404)

    def do_POST(self):
        if self.client_address[0] != INGRESS_IP:
            return self.send_error(403)
        if self.path not in ("/probe", "/export", "/filters", "/batch"):
            return self.send_error(404)
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 65536:
                raise ValueError("Invalid request size")
            fields = parse_qs(self.rfile.read(length).decode("ascii"))
        except ValueError:
            return self.send_error(400)
        if fields.get("token") != [TOKEN]:
            return self.send_error(403)
        try:
            if self.path == "/batch":
                result = start_batch()
            elif self.path in ("/export", "/probe") and batch_running:
                result = "Дождитесь завершения общей выгрузки."
            elif self.path == "/export":
                if "preset" in fields:
                    field, _, value = fields["preset"][0].partition(":")
                    if value not in read_filters().get(field, []):
                        raise ValueError("Сохранённый фильтр не найден")
                else:
                    field = fields.get("field", [""])[0]
                    value = fields.get("value", [""])[0].strip()
                result = export_registry(field, value)
            elif self.path == "/filters":
                result = save_filters(fields)
            else:
                result = probe()
        except (OSError, ValueError) as error:
            result = f"Ошибка: {error}"
        logging.info("%s", result)
        self.show_page(result)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    logging.info("Domis Registry: ready on port 8099")
    HTTPServer(("0.0.0.0", 8099), Handler).serve_forever()
