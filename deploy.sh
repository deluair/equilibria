#!/bin/bash
set -e

VPS_HOST="${VPS_HOST:-ubuntu@40.160.2.223}"
REMOTE="/home/ubuntu/equilibria"

echo "Deploying Equilibria..."

# Sync code to VPS
rsync -avz \
    --exclude '.venv' \
    --exclude '__pycache__' \
    --exclude '.git' \
    --exclude 'data/' \
    --exclude '.claude' \
    --exclude 'node_modules' \
    --exclude '.next' \
    --exclude '.env' \
    --exclude '*.pyc' \
    . "${VPS_HOST}:${REMOTE}/"

# Install deps and restart service
ssh "$VPS_HOST" "cd $REMOTE && source .venv/bin/activate && pip install -e . && sudo systemctl restart equilibria"

# Health check
echo "Health check..."
sleep 3
curl -sf "http://localhost:8003/api/health" && echo " OK" || echo " FAILED"
