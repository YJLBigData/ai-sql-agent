#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PYTHONPATH=src ./.venv/bin/python -m sql_ai_copilot.cli.main init-db "$@"
