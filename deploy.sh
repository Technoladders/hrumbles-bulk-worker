#!/bin/bash
# ============================================================
# deploy.sh — hrumbles-bulk-worker server-side deploy script
# Run this on the Hostinger VPS as root
# Usage: bash deploy.sh [IMAGE_TAG]
# ============================================================

set -e

# ── Config ────────────────────────────────────────────────────────────────────
CONTAINER_NAME="hrumbles-bulk-worker"
IMAGE_NAME="${DOCKERHUB_USERNAME:-your_dockerhub_username}/hrumbles-bulk-worker"
IMAGE_TAG="${1:-latest}"
FULL_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"
APP_DIR="/opt/hrumbles-bulk-worker"
NETWORK="resume-parser-network"   # same network as Redis
PORT=5010                          # internal only, NOT exposed publicly

echo "=========================================="
echo " Deploying $CONTAINER_NAME"
echo " Image:   $FULL_IMAGE"
echo " Network: $NETWORK"
echo "=========================================="

# ── Ensure network exists ────────────────────────────────────────────────────
if ! docker network ls --format '{{.Name}}' | grep -q "^${NETWORK}$"; then
  echo ">>> Creating Docker network: $NETWORK"
  docker network create "$NETWORK"
else
  echo ">>> Network exists: $NETWORK"
fi

# ── Pull latest image ────────────────────────────────────────────────────────
echo ">>> Pulling $FULL_IMAGE ..."
docker pull "$FULL_IMAGE"

# ── Stop existing container (if running) ─────────────────────────────────────
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo ">>> Stopping old container..."
  docker stop "$CONTAINER_NAME" 2>/dev/null || true
  docker rm   "$CONTAINER_NAME" 2>/dev/null || true
fi

# ── Start new container ──────────────────────────────────────────────────────
echo ">>> Starting new container..."
docker run -d \
  --name "$CONTAINER_NAME" \
  --network "$NETWORK" \
  --restart unless-stopped \
  --env-file "$APP_DIR/.env" \
  --memory 512m \
  --memory-swap 768m \
  --cpus 1.0 \
  "$FULL_IMAGE"

# ── Wait and health check ────────────────────────────────────────────────────
echo ">>> Waiting 10s for container to initialise..."
sleep 10

CONTAINER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$CONTAINER_NAME")
echo ">>> Container IP: $CONTAINER_IP"

if docker exec "$CONTAINER_NAME" curl -sf http://localhost:$PORT/health > /dev/null 2>&1; then
  echo ">>> Health check PASSED"
else
  echo ">>> Health check FAILED — checking logs:"
  docker logs --tail 50 "$CONTAINER_NAME"
  exit 1
fi

echo ""
echo "=========================================="
echo " $CONTAINER_NAME deployed successfully"
echo " Container: $(docker ps --filter name=$CONTAINER_NAME --format '{{.Status}}')"
echo "=========================================="