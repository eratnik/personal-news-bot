import logging
import secrets
import ssl
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
EXPORT = Path("/data/registry-export.csv")
TOKEN = secrets.token_urlsafe()
MAX_BYTES = 5 * 1024 * 1024


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
    request = Request(url, headers={"User-Agent": "DomisRegistry/0.2.0"})
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


def probe():
    status, body = fetch_registry(URL)
    save_file(SNAPSHOT, body)
    return f"HTTP {status}. Получен HTML, {len(body)} байт."


def export_registry(field, value):
    rows, total = collect(lambda url: fetch_registry(url)[1].decode("utf-8"), URL, field, value)
    save_file(EXPORT, csv_bytes(rows))
    summary = f"Выгружено {len(rows)} из {total} записей."
    if len(rows) < total:
        summary += " В CSV сохранены первые две страницы."
    return summary


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
        page = f"""<!doctype html>
<html lang="ru">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Domis Registry</title>
<style>
body {{ font: 1.05rem system-ui, sans-serif; margin: 3rem auto; max-width: 42rem; padding: 0 1rem; }}
button {{ font: inherit; padding: .6rem 1rem; cursor: pointer; }}
</style>
<main>
<h1>Реестр опасной продукции</h1>
<p>Пробная выгрузка сохраняет до двух страниц по одному фильтру.</p>
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
{result}
{csv_download}
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
        self.send_error(404)

    def do_POST(self):
        if self.client_address[0] != INGRESS_IP:
            return self.send_error(403)
        if self.path not in ("/probe", "/export"):
            return self.send_error(404)
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 4096:
                raise ValueError("Invalid request size")
            fields = parse_qs(self.rfile.read(length).decode("ascii"))
        except ValueError:
            return self.send_error(400)
        if fields.get("token") != [TOKEN]:
            return self.send_error(403)
        try:
            if self.path == "/export":
                field = fields.get("field", [""])[0]
                value = fields.get("value", [""])[0].strip()
                result = export_registry(field, value)
            else:
                result = probe()
        except (OSError, ValueError) as error:
            result = f"Ошибка при получении страницы: {error}"
        logging.info("%s", result)
        self.show_page(result)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    logging.info("Domis Registry: ready on port 8099")
    HTTPServer(("0.0.0.0", 8099), Handler).serve_forever()
