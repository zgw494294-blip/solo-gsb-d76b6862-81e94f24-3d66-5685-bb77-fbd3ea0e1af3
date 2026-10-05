#!/bin/sh
# Initialize the SQLite database (idempotent) before starting the API.
set -e

python -c "from app.config import ensure_dirs; ensure_dirs(); from app import db; db.init_db(); print('database initialized at', db.DB_PATH)"

exec "$@"
