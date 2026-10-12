import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = PROJECT_ROOT / "sql"

SOURCE_DSN = os.getenv("SOURCE_DSN", "postgresql://postgres:postgres@localhost:5433/shop")
WAREHOUSE_DSN = os.getenv("WAREHOUSE_DSN", "postgresql://postgres:postgres@localhost:5433/warehouse")
LAKE_PATH = Path(os.getenv("LAKE_PATH", PROJECT_ROOT / "lake")).resolve()