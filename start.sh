#!/usr/bin/env bash
# Start local server and open browser
set -euo pipefail

cd "$(dirname "$0")/site"
PORT="${PORT:-8080}"

if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Python is required but was not found in PATH."
  exit 1
fi

echo "Starting AI Exposure viewer at http://localhost:$PORT"
# Try to open browser
if command -v open &>/dev/null; then
  sleep 0.5 && open "http://localhost:$PORT" &
elif command -v xdg-open &>/dev/null; then
  sleep 0.5 && xdg-open "http://localhost:$PORT" &
fi
"$PYTHON_BIN" -m http.server "$PORT"
