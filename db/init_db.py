"""
Инициализация БД: создание таблицы products при первом запуске.
"""
import psycopg2

INIT_SQL = """
CREATE TABLE IF NOT EXISTS products (
    id TEXT PRIMARY KEY,
    number TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL DEFAULT '',
    size TEXT NOT NULL DEFAULT '',
    diameter NUMERIC,
    airflow_min NUMERIC,
    airflow_max NUMERIC,
    airflow_raw TEXT,
    pressure_min NUMERIC,
    pressure_max NUMERIC,
    pressure_raw TEXT,
    power NUMERIC,
    noise_level NUMERIC,
    price NUMERIC,
    raw_diameter TEXT,
    raw_efficiency TEXT,
    raw_pressure TEXT,
    raw_power TEXT,
    raw_noise_level TEXT,
    raw_price TEXT,
    model_slug TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_products_type ON products(type);
CREATE INDEX IF NOT EXISTS idx_products_model_slug ON products(model_slug);
CREATE INDEX IF NOT EXISTS idx_products_price ON products(price);
"""


def init_db(conn) -> None:
    """Создаёт таблицу products и индексы, если их ещё нет."""
    with conn.cursor() as cur:
        cur.execute(INIT_SQL)
    conn.commit()
