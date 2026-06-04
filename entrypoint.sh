#!/bin/bash
set -e

echo "========================================="
echo " hrumbles-bulk-worker starting"
echo "========================================="

REDIS_URL="redis://${REDIS_HOST:-redis}:${REDIS_PORT:-6379}"
echo ">>> Redis URL: $REDIS_URL"

echo ">>> Waiting for Redis..."
until python -c "import redis; redis.Redis(host='${REDIS_HOST:-redis}', port=${REDIS_PORT:-6379}).ping()" 2>/dev/null; do
  echo "    Redis not ready, retrying in 2s..."
  sleep 2
done
echo ">>> Redis is ready"

echo ">>> Resetting stuck files from previous run..."
python -c "
from config import supabase
try:
    supabase.rpc('reset_stuck_bulk_files', {}).execute()
    print('    Stuck files reset OK')
except Exception as e:
    print(f'    Warning: could not reset stuck files: {e}')
    print('    Continuing startup...')
"

echo ">>> Starting RQ worker watchdog..."
(
  set +e  # ← CRITICAL FIX: stops set -e from killing the loop on worker exit
  while true; do
    echo ">>> [Watchdog] Starting RQ worker..."
    rq worker bulk-pipeline \
      --url "$REDIS_URL" \
      --name "bulk-worker-$(hostname)" \
      --max-jobs 200 \
      2>&1
    EXIT_CODE=$?
    echo ">>> [Watchdog] RQ worker exited (code: $EXIT_CODE). Restarting in 5s..."
    sleep 5
  done
) &
WATCHDOG_PID=$!
echo ">>> Watchdog started (PID: $WATCHDOG_PID)"

sleep 2

echo ">>> Starting Flask API on port ${PORT:-5010}..."
python app.py

echo ">>> Flask exited, killing watchdog..."
kill $WATCHDOG_PID 2>/dev/null || true
wait $WATCHDOG_PID 2>/dev/null || true
echo ">>> Shutdown complete"