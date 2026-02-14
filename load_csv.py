#!/usr/bin/env python3
"""
Загрузка данных из fans_data.csv в БД. Запуск из корня проекта:
  python load_csv.py
  или: python -m db.load_csv
"""
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

from config import DATABASE_URL
from db.connection import close_pool, get_connection, init_pool, put_connection
from db.init_db import init_db
from db.load_csv import load_csv_into_db

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "fans_data.csv"

if __name__ == "__main__":
    init_pool(DATABASE_URL)
    conn = get_connection()
    try:
        init_db(conn)
        n = load_csv_into_db(conn, CSV_PATH)
        print(f"Загружено записей: {n}")
    finally:
        put_connection(conn)
        close_pool()
