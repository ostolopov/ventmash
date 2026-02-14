"""
Конфигурация приложения из переменных окружения.
Перед импортом config должен быть загружен .env (см. app.py).
"""
import os

# База данных
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://localhost/ventmash",  # fallback для локальной разработки
)

# Сервер
PORT = int(os.environ.get("PORT", "3000"))
