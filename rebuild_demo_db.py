from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from sql_ai_copilot.bootstrap import ensure_demo_database
from sql_ai_copilot.config.settings import get_settings


def main() -> None:
    settings = get_settings()
    ensure_demo_database(settings=settings, force_reseed=True, verbose=True)


if __name__ == "__main__":
    main()

