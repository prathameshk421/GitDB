#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ ! -f "$ROOT_DIR/.venv/bin/activate" ]]; then
  echo "Missing Python virtual environment at .venv/."
  echo "Create it first with: python -m venv .venv && source .venv/bin/activate && pip install -e '.[dev]'"
  exit 1
fi

if [[ ! -f "$ROOT_DIR/ui/package.json" ]]; then
  echo "Missing UI package.json at ui/package.json"
  exit 1
fi

if [[ ! -d "$ROOT_DIR/ui/node_modules" ]]; then
  echo "UI dependencies are not installed. Run: cd ui && npm install"
  exit 1
fi

source "$ROOT_DIR/.venv/bin/activate"

cleanup() {
  local exit_code=$?
  if [[ -n "${API_PID:-}" ]] && kill -0 "$API_PID" 2>/dev/null; then
    kill "$API_PID" 2>/dev/null || true
  fi
  if [[ -n "${UI_PID:-}" ]] && kill -0 "$UI_PID" 2>/dev/null; then
    kill "$UI_PID" 2>/dev/null || true
  fi
  wait 2>/dev/null || true
  exit "$exit_code"
}

trap cleanup INT TERM EXIT

echo "Starting Flask API on http://127.0.0.1:5001 ..."
(
  cd "$ROOT_DIR"
  python -m api.app
) &
API_PID=$!

echo "Starting React UI (Vite) ..."
(
  cd "$ROOT_DIR/ui"
  npm run dev
) &
UI_PID=$!

echo "GitDB dev stack is running. Press Ctrl+C to stop both services."

# Keep script attached to child processes and exit if any process exits.
while true; do
  if ! kill -0 "$API_PID" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$UI_PID" 2>/dev/null; then
    break
  fi
  sleep 1
done
