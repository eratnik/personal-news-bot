import csv
import io
import re
from html.parser import HTMLParser
from itertools import count
from urllib.parse import urlencode


FIELDS = (
    "title", "prescription", "issueDate", "effectDate", "tnved", "trts",
    "pd_number", "manufacturer", "measure", "importers", "suppliers", "violation",
)
FIELDS_IN_RUSSIAN = (
    "Запись реестра", "Предписание", "Дата выдачи", "Дата введения меры",
    "ТН ВЭД", "ТР ТС", "Документ соответствия", "Производитель", "Мера",
    "Импортёры", "Поставщики", "Нарушения",
)
PAGE_LIMIT = 2


class RegistryPage(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.rows = []
        self.filters = {}
        self.page_info = ""
        self.card = None
        self.field = None
        self.info = False
        self.grid_seen = False
        self.pagination_seen = False
        self.footer_seen = False
        self.empty_block = False
        self.empty_paragraph = False
        self.empty_message = ""
        self.feed(html)
        self.close()

    def handle_starttag(self, tag, attributes):
        attrs = dict(attributes)
        classes = (attrs.get("class") or "").split()
        if tag == "input" and attrs.get("id") in ("searchProduct", "searchTnved"):
            self.filters[attrs["name"]] = attrs.get("value") or ""
        if tag == "div" and attrs.get("id") == "cardsGrid":
            self.grid_seen = True
        if "pagination-bar" in classes or attrs.get("id") in ("pagination", "pageInfo"):
            self.pagination_seen = True
        if tag == "footer" and attrs.get("role") == "contentinfo":
            self.footer_seen = True
        if tag == "div" and "cards-empty" in classes:
            self.empty_block = True
        if tag == "p" and self.empty_block:
            self.empty_paragraph = True
        if tag == "div" and attrs.get("id") == "pageInfo":
            self.info = True
            self.page_info = ""
        if tag == "article" and "rc" in classes:
            self.card = {"title": attrs.get("aria-label") or ""}
        if tag == "dd" and self.card is not None:
            self.field = next(
                (name for name in FIELDS[1:] if f"rc-field-value--{name}" in classes), None
            )
            if self.field:
                self.card[self.field] = ""

    def handle_data(self, data):
        if self.empty_paragraph:
            self.empty_message += data
        if self.info:
            self.page_info += data
        if self.field:
            self.card[self.field] += data

    def handle_endtag(self, tag):
        if tag == "p":
            self.empty_paragraph = False
        if tag == "dd":
            self.field = None
        if tag == "div":
            self.info = False
            self.empty_block = False
        if tag == "article" and self.card is not None:
            self.rows.append({key: " ".join(value.split()) for key, value in self.card.items()})
            self.card = None


def collect(fetch, registry_url, field, value, page_limit=PAGE_LIMIT):
    if field not in ("name", "tnved") or not 1 <= len(value) <= 100:
        raise ValueError("Укажите наименование или код ТН ВЭД длиной до 100 символов")
    if field == "tnved" and not re.fullmatch(r"[0-9]{4,10}", value):
        raise ValueError("Код ТН ВЭД должен содержать от 4 до 10 цифр")
    rows = {}
    expected_start = 1
    total = None
    pages = count(1) if page_limit is None else range(1, page_limit + 1)
    for number in pages:
        query = urlencode({field: value, "page": number, "per_page": 10})
        page_url = f"{registry_url}?{query}"
        page = RegistryPage(fetch(page_url))
        info = " ".join(page.page_info.split())
        if page.filters.get(field) != value:
            raise ValueError(f"Страница {number}. Реестр не подтвердил значение фильтра «{value}»")
        if number == 1 and not page.rows and " ".join(page.empty_message.split()) == "Записи не найдены":
            return [], 0
        if not page.grid_seen:
            raise ValueError(f"Страница {number}. В ответе реестра отсутствует блок карточек")
        match = re.fullmatch(r"Показано ([0-9]+) [–-] ([0-9]+) из ([0-9]+)", info)
        if match:
            start, end, page_total = map(int, match.groups())
        elif number == 1 and not page.pagination_seen and page.footer_seen and 1 <= len(page.rows) <= 10:
            start, end, page_total = 1, len(page.rows), len(page.rows)
        else:
            raise ValueError(f"Страница {number}. Реестр не вернул проверяемый диапазон записей")
        if (start, end, page_total) == (0, 0, 0) and not page.rows and number == 1:
            return [], 0
        if (
            start != expected_start or not start <= end <= page_total
            or end - start + 1 != len(page.rows) or len(page.rows) > 10
            or (total is not None and page_total != total)
        ):
            raise ValueError("Количество записей или порядок страниц изменились")
        total = page_total
        for row in page.rows:
            record = row["title"].partition(". ")[0]
            if not record or record in rows or not row.get("measure"):
                raise ValueError("В выдаче найдена повторная или неполная карточка")
            rows[record] = {**row, "record": record, "url": f"{registry_url}?{urlencode({'year_number': record})}"}
        if end == total:
            break
        expected_start = end + 1
    return list(rows.values()), total


def csv_bytes(rows):
    output = io.StringIO(newline="")
    writer = csv.writer(output, delimiter=";")
    writer.writerow(("Номер записи", *FIELDS_IN_RUSSIAN, "Ссылка на поиск записи"))
    for row in rows:
        values = (row.get(key, "") for key in ("record", *FIELDS, "url"))
        writer.writerow("'" + value if value.startswith(("=", "+", "-", "@")) and value != "-" else value for value in values)
    return output.getvalue().encode("utf-8-sig")
