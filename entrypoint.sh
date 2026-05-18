#!/bin/bash
set -e

echo "========================================="
echo " hrumbles-bulk-worker starting"
echo "========================================="

REDIS_URL="redis://${REDIS_HOST:-redis}:${REDIS_PORT:-6379}"
echo ">>> Redis URL: $REDIS_URL"

# Wait for Redis to be ready
echo ">>> Waiting for Redis..."
until python -c "import redis; redis.Redis(host='${REDIS_HOST:-redis}', port=${REDIS_PORT:-6379}).ping()" 2>/dev/null; do
  echo "    Redis not ready, retrying in 2s..."
  sleep 2
done
echo ">>> Redis is ready"

# Reset any stuck files from previous crash
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

# Start RQ worker in background (bulk-pipeline queue only)
echo ">>> Starting RQ worker (bulk-pipeline queue)..."
rq worker bulk-pipeline \
  --url "$REDIS_URL" \
  --name "bulk-worker-$(hostname)" \
  --max-jobs 1000 \
  &
RQ_PID=$!
echo ">>> RQ worker started (PID: $RQ_PID)"

# Small delay before Flask starts
sleep 1

# Start Flask (with embedded APScheduler)
echo ">>> Starting Flask API on port ${PORT:-5010}..."
python app.py

# If Flask exits (shouldn't happen), kill RQ worker
echo ">>> Flask exited, shutting down RQ worker..."
kill $RQ_PID 2>/dev/null || true
wait $RQ_PID 2>/dev/null || true
echo ">>> Shutdown complete"