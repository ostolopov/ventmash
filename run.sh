#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

VENV_DIR=".venv"
if [ ! -d "$VENV_DIR" ]; then
  echo "▶ Создаю виртуальное окружение ($VENV_DIR)..."
  python3 -m venv "$VENV_DIR"
fi

echo "▶ Активация виртуального окружения..."
source "$VENV_DIR/bin/activate"

echo "▶ Установка зависимостей (pip install -r requirements.txt)..."
pip install -r requirements.txt

echo "▶ Запуск Python-бэкенда..."
python app.py

