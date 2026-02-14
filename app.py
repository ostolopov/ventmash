import csv
import math
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional, Dict, Any

from flask import Flask, jsonify, request, send_from_directory

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "fans_data.csv"


def normalize_whitespace(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\u00A0", " ").split())


def parse_number_loose(value: Any) -> Optional[float]:
    s = normalize_whitespace(value).replace(" ", "").replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


@dataclass
class Range:
    min: Optional[float]
    max: Optional[float]
    raw: str


def parse_range_loose(value: Any) -> Range:
    raw = normalize_whitespace(value)
    if not raw:
        return Range(None, None, "")
    parts = [normalize_whitespace(p) for p in raw.split("-")]
    if len(parts) == 1:
        n = parse_number_loose(parts[0])
        return Range(n, n, raw)
    min_v = parse_number_loose(parts[0])
    max_v = parse_number_loose("-".join(parts[1:]))
    return Range(min_v, max_v, raw)


def slugify(value: str) -> str:
    import re

    s = normalize_whitespace(value).lower()
    # Уберём все, кроме букв/цифр, заменяя на дефис
    s = re.sub(r"[^\w]+", "-", s, flags=re.UNICODE)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


def ranges_overlap(a: Range, b: Range) -> bool:
    a_min = -math.inf if a.min is None else a.min
    a_max = math.inf if a.max is None else a.max
    b_min = -math.inf if b.min is None else b.min
    b_max = math.inf if b.max is None else b.max
    return a_min <= b_max and b_min <= a_max


@dataclass
class Product:
    id: str
    number: str
    type: str
    model: str
    size: str
    diameter: Optional[float]
    airflow: Range
    pressure: Range
    power: Optional[float]
    noise_level: Optional[float]
    price: Optional[float]
    _raw: Dict[str, str]
    _meta: Dict[str, Any]


class Store:
    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.products: List[Product] = []
        self.by_id: Dict[str, Product] = {}
        self.by_model: Dict[str, Product] = {}
        self.by_model_slug: Dict[str, Product] = {}
        self.load()

    def load(self) -> None:
        self.products.clear()
        self.by_id.clear()
        self.by_model.clear()
        self.by_model_slug.clear()

        # Поддерживаем и запятую, и точку с запятой как разделитель.
        with self.csv_path.open("r", encoding="utf-8") as f:
            # Пробуем автоматически определить разделитель по первой строке.
            sample = f.read(1024)
            f.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=";,")
            except csv.Error:
                # Fallback: предполагаем точку с запятой (как в текущем файле).
                dialect = csv.excel
                dialect.delimiter = ";"

            reader = csv.DictReader(f, dialect=dialect)

            for i, row in enumerate(reader, start=1):
                number = normalize_whitespace(row.get("number")) or str(i)
                type_ = normalize_whitespace(row.get("type"))
                model = normalize_whitespace(row.get("model"))
                size = normalize_whitespace(row.get("size"))
                diameter = parse_number_loose(row.get("diameter"))

                airflow = parse_range_loose(row.get("efficiency"))
                pressure = parse_range_loose(row.get("pressure"))
                power = parse_number_loose(row.get("power"))
                noise_level = parse_number_loose(row.get("noise_level"))
                price = parse_number_loose(row.get("price"))

                if not (type_ or model or size):
                    continue

                p = Product(
                    id=number,
                    number=number,
                    type=type_,
                    model=model,
                    size=size,
                    diameter=diameter,
                    airflow=airflow,
                    pressure=pressure,
                    power=power,
                    noise_level=noise_level,
                    price=price,
                    _raw={
                        "diameter": normalize_whitespace(row.get("diameter")),
                        "efficiency": normalize_whitespace(row.get("efficiency")),
                        "pressure": normalize_whitespace(row.get("pressure")),
                        "power": normalize_whitespace(row.get("power")),
                        "noise_level": normalize_whitespace(row.get("noise_level")),
                        "price": normalize_whitespace(row.get("price")),
                    },
                    _meta={
                        "model_slug": slugify(model),
                    },
                )

                self.products.append(p)
                self.by_id[p.id] = p
                if p.model:
                    self.by_model[p.model.lower()] = p
                if p._meta["model_slug"]:
                    self.by_model_slug[p._meta["model_slug"]] = p


store = Store(CSV_PATH)

app = Flask(
    __name__,
    static_folder=str(BASE_DIR / "public"),
    static_url_path="",
)


def product_to_json(p: Product) -> Dict[str, Any]:
    data = asdict(p)
    # dataclasses -> JSON-friendly dict, Range уже превратится в словарь
    return data


@app.get("/api/products")
def api_products():
    q = normalize_whitespace(request.args.get("q")).lower()
    type_ = normalize_whitespace(request.args.get("type"))
    diameter = parse_number_loose(request.args.get("diameter"))

    min_price = parse_number_loose(request.args.get("minPrice"))
    max_price = parse_number_loose(request.args.get("maxPrice"))
    min_power = parse_number_loose(request.args.get("minPower"))
    max_power = parse_number_loose(request.args.get("maxPower"))
    min_noise = parse_number_loose(request.args.get("minNoise"))
    max_noise = parse_number_loose(request.args.get("maxNoise"))
    min_diameter = parse_number_loose(request.args.get("minDiameter"))
    max_diameter = parse_number_loose(request.args.get("maxDiameter"))

    min_airflow = parse_number_loose(request.args.get("minAirflow"))
    max_airflow = parse_number_loose(request.args.get("maxAirflow"))
    min_pressure = parse_number_loose(request.args.get("minPressure"))
    max_pressure = parse_number_loose(request.args.get("maxPressure"))

    sort = normalize_whitespace(request.args.get("sort")) or "price_asc"

    result = list(store.products)

    if q:
        result = [
            p
            for p in result
            if q in f"{p.model} {p.size} {p.type}".lower()
        ]

    if type_:
        result = [p for p in result if p.type == type_]

    if diameter is not None:
        result = [p for p in result if p.diameter == diameter]

    if min_price is not None:
        result = [p for p in result if p.price is not None and p.price >= min_price]
    if max_price is not None:
        result = [p for p in result if p.price is not None and p.price <= max_price]

    if min_power is not None:
        result = [p for p in result if p.power is not None and p.power >= min_power]
    if max_power is not None:
        result = [p for p in result if p.power is not None and p.power <= max_power]

    if min_noise is not None:
        result = [
            p
            for p in result
            if p.noise_level is not None and p.noise_level >= min_noise
        ]
    if max_noise is not None:
        result = [
            p
            for p in result
            if p.noise_level is not None and p.noise_level <= max_noise
        ]

    if min_diameter is not None:
        result = [
            p
            for p in result
            if p.diameter is not None and p.diameter >= min_diameter
        ]
    if max_diameter is not None:
        result = [
            p
            for p in result
            if p.diameter is not None and p.diameter <= max_diameter
        ]

    if min_airflow is not None or max_airflow is not None:
        b = Range(min_airflow, max_airflow, "")
        result = [p for p in result if ranges_overlap(p.airflow, b)]

    if min_pressure is not None or max_pressure is not None:
        b = Range(min_pressure, max_pressure, "")
        result = [p for p in result if ranges_overlap(p.pressure, b)]

    # Сортировка по цене (asc/desc), элементы без цены всегда в конце.
    def sort_key(p: Product):
        no_price = p.price is None
        base_price = 0.0 if p.price is None else float(p.price)
        if sort == "price_desc":
            base_price = -base_price
        return (1 if no_price else 0, base_price, p.model)

    result.sort(key=sort_key)

    return jsonify([product_to_json(p) for p in result])


@app.get("/api/products/<id_or_model>")
def api_product_detail(id_or_model: str):
    raw = normalize_whitespace(id_or_model)
    key = raw.lower()

    p = store.by_id.get(raw) or store.by_model.get(key) or store.by_model_slug.get(
        slugify(raw)
    )
    if not p:
        return jsonify({"error": "Product not found"}), 404
    return jsonify(product_to_json(p))


@app.get("/api/health")
def api_health():
    return jsonify({"ok": True, "products": len(store.products)})


@app.get("/")
def index_page():
    # Отдаём готовый index.html из public
    return send_from_directory(app.static_folder, "index.html")


@app.get("/product.html")
def product_page():
    return send_from_directory(app.static_folder, "product.html")


@app.get("/style.css")
def style_css():
    return app.send_static_file("style.css")


@app.get("/script.js")
def script_js():
    return app.send_static_file("script.js")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "3000"))
    app.run(host="0.0.0.0", port=port, debug=True)

