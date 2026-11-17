#!/usr/bin/env bash
# Chained pilot run: select -> parse smoke test -> full parse. Stops on first failure.
# Usage: tmux new -d -s pilot 'scripts/overnight.sh'
set -euo pipefail
cd "$(dirname "$0")/.."
. .venv/bin/activate
mkdir -p data /tmp/duckdb_spill
exec > >(tee -a data/overnight.log) 2>&1
echo "== start $(date)"
python select_matches.py
python parse_matches.py --limit 3
python parse_matches.py --workers 4
echo "== done $(date)"
