import os
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv
from flask import Flask, g, jsonify, request, send_from_directory

from config import DATABASE_URL, PORT
from db.connection import close_pool, get_connection, init_pool, put_connection
from db.init_db import init_db
from db.load_csv import load_csv_into_db
from db.repository import count_products, get_by_id, get_by_model_or_slug, list_products

load_dotenv(Path(__file__).resolve().parent / ".env")

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


def slugify(value: str) -> str:
    import re
    s = normalize_whitespace(value).lower()
    s = re.sub(r"[^\w]+", "-", s, flags=re.UNICODE)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


app = Flask(
    __name__,
    static_folder=str(BASE_DIR / "public"),
    static_url_path="",
)


@app.before_request
def before_request():
    g.db = get_connection()


@app.teardown_request
def teardown_request(exc=None):
    if hasattr(g, "db"):
        put_connection(g.db)


@app.get("/api/products")
def api_products():
    q = normalize_whitespace(request.args.get("q")).lower() or None
    type_ = normalize_whitespace(request.args.get("type")) or None
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

    result = list_products(
        g.db,
        q=q,
        type_=type_,
        diameter=diameter,
        min_price=min_price,
        max_price=max_price,
        min_power=min_power,
        max_power=max_power,
        min_noise=min_noise,
        max_noise=max_noise,
        min_diameter=min_diameter,
        max_diameter=max_diameter,
        min_airflow=min_airflow,
        max_airflow=max_airflow,
        min_pressure=min_pressure,
        max_pressure=max_pressure,
        sort=sort,
    )
    return jsonify(result)


@app.get("/api/products/<id_or_model>")
def api_product_detail(id_or_model: str):
    raw = normalize_whitespace(id_or_model)
    p = get_by_id(g.db, raw) or get_by_model_or_slug(
        g.db, raw.lower(), slugify(raw)
    )
    if not p:
        return jsonify({"error": "Product not found"}), 404
    return jsonify(p)


@app.get("/api/health")
def api_health():
    n = count_products(g.db)
    return jsonify({"ok": True, "products": n})


@app.get("/")
def index_page():
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
    init_pool(DATABASE_URL)
    conn = get_connection()
    try:
        init_db(conn)
        if count_products(conn) == 0 and CSV_PATH.exists():
            load_csv_into_db(conn, CSV_PATH)
    finally:
        put_connection(conn)

    try:
        app.run(host="0.0.0.0", port=PORT, debug=True)
    finally:
        close_pool()
