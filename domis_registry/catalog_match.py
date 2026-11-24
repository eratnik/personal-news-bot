import csv
import io
import re
import unicodedata
from collections import defaultdict


CATALOG_URL = "https://domis.by/files/prices/all_products.csv"
TOKEN = re.compile(r"[\w]+(?:[-./][\w]+)*", re.UNICODE)
LABEL = re.compile(r"\b(?:модел[ьи]|артикул[аы]?)\b\s*[:№]?\s*[«\"']?", re.IGNORECASE)
CONFUSABLES = str.maketrans("АВЕКМНОРСТУХ", "ABEKMHOPCTYX")
DASHES = str.maketrans("‐‑‒–—−", "------")
REGISTRY_FIELDS = (
    "Номер записи", "Запись реестра", "Мера", "Дата введения меры", "Ссылка на поиск записи",
)
REPORT_FIELDS = (
    "ID товара domis.by", "Товар domis.by", "Цена в выгрузке", "Совпавшее обозначение",
    *REGISTRY_FIELDS,
)


def normalize(text):
    return unicodedata.normalize("NFKC", text).upper().translate(DASHES)


def key(code):
    return re.sub(r"[^\w]|_", "", normalize(code).translate(CONFUSABLES))


def model_code(token, *, explicitly_named=False):
    code = key(token)
    if len(code) < 4:
        return False
    has_letter = any(char.isalpha() for char in code)
    has_digit = any(char.isdigit() for char in code)
    if has_letter and has_digit:
        return True
    return explicitly_named and has_letter and "-" in token and token == token.upper()


def read_catalog(payload):
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = payload.decode("cp1251")
    products = {}
    for fields in csv.reader(io.StringIO(text, newline=""), delimiter=";", strict=True):
        if not fields:
            continue
        if len(fields) != 3:
            raise ValueError("В каталоге domis.by ожидались три колонки")
        sku, title, price = (field.strip() for field in fields)
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", sku):
            raise ValueError("Каталог domis.by содержит некорректный ID товара")
        if not title or not re.fullmatch(r"\d+(?:[.,]\d+)?", price):
            raise ValueError("Каталог domis.by содержит строку без названия или цены")
        product = (sku, title, price)
        if sku in products and products[sku] != product:
            raise ValueError("В каталоге domis.by есть разные строки с одним ID")
        products[sku] = product
    if not products:
        raise ValueError("Каталог domis.by пуст")
    return list(products.values())


def registry_codes(title):
    title = normalize(title.partition(". ")[2] or title)
    codes = {}
    labels = list(LABEL.finditer(title))
    if labels:
        for label in labels:
            tail = title[label.end():]
            first = TOKEN.match(tail)
            if first is None:
                continue
            token = first.group()
            if model_code(token, explicitly_named=True):
                codes[key(token)] = token
            elif token.isalpha():
                second = re.match(r"\s+", tail[first.end():])
                if second:
                    next_token = TOKEN.match(tail, first.end() + second.end())
                    if next_token is None:
                        continue
                    suffix = next_token.group()
                    if suffix.isdigit() and len(token) <= 5 and model_code(token + suffix):
                        designation = token + " " + suffix
                        codes[key(designation)] = designation
                    elif model_code(suffix):
                        designation = token + " " + suffix if len(token) <= 3 else suffix
                        codes[key(designation)] = designation
    else:
        for token in TOKEN.findall(title):
            if model_code(token):
                codes[key(token)] = token
    return codes


def catalog_index(products):
    index = defaultdict(list)
    for product in products:
        tokens = TOKEN.findall(normalize(product[1]))
        codes = {key(token) for token in tokens if model_code(token, explicitly_named=True)}
        for prefix, token in zip(tokens, tokens[1:]):
            if prefix.isalpha() and (
                (len(prefix) <= 3 and model_code(token))
                or (len(prefix) <= 5 and token.isdigit() and model_code(prefix + token))
            ):
                codes.add(key(prefix + token))
        for code in codes:
            index[code].append(product)
    return index


def csv_cell(value):
    if value.lstrip().startswith(("=", "+", "-", "@")) and value != "-":
        return "'" + value
    return value


def compare(registry_csv, catalog_payload):
    products = read_catalog(catalog_payload)
    index = catalog_index(products)
    reader = csv.DictReader(io.StringIO(registry_csv, newline=""), delimiter=";", strict=True)
    if not set(REGISTRY_FIELDS).issubset(reader.fieldnames or []):
        raise ValueError("В общем CSV реестра отсутствуют поля для сверки")
    output = io.StringIO(newline="")
    writer = csv.writer(output, delimiter=";")
    writer.writerow(REPORT_FIELDS)
    matches = 0
    for record in reader:
        values = [record[field] for field in REGISTRY_FIELDS]
        if None in values or not values[0] or not values[1]:
            raise ValueError("В общем CSV реестра найдена неполная запись")
        candidates = {}
        for code, label in registry_codes(values[1]).items():
            for product in index.get(code, ()):
                candidates.setdefault(product, []).append(label)
        for product, labels in sorted(candidates.items()):
            writer.writerow(map(csv_cell, (*product, ", ".join(labels), *values)))
            matches += 1
    return output.getvalue().encode("utf-8-sig"), len(products), matches
