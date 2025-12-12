#!/bin/zsh

# Always run from this folder
cd "$(dirname "$0")"

# Start Postgres.app manually if needed before running this script.

echo "Activating virtualenv..."
source .venv/bin/activate

echo "Starting API (uvicorn) on http://127.0.0.1:8000 ..."
uvicorn api:app --reload &
API_PID=$!

echo "Starting static server on http://127.0.0.1:5500 ..."
python3 -m http.server 5500

echo "Static server stopped, shutting down API..."
kill $API_PID 2>/dev/null || true
echo "All done."
