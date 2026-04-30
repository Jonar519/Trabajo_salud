import os
import sys
from pathlib import Path


project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root / "src"))

from frontend_server import run_frontend_server


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0").strip() or "0.0.0.0"
    port = _int_env("PORT", 8000)
    csv_path = (os.getenv("CSV_PATH") or "").strip() or None
    pbix_path = (os.getenv("PBIX_PATH") or "").strip() or None
    run_frontend_server(host=host, port=port, csv_path=csv_path, pbix_path=pbix_path)
