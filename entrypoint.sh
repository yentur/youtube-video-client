#!/bin/bash
# Vast.ai instance entrypoint — clones the client repo and runs main.py.
# Restarts on failure; the client itself decides when to terminate.

set -eu
echo "[entrypoint] starting at $(date -Is)"

env | grep -E '^(API_BASE_URL|CLIENT_ID|VAST_INSTANCE_ID)=' >> /etc/environment || true

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y --no-install-recommends git curl ffmpeg ca-certificates python3 python3-venv python3-pip

# install uv (fast python package manager)
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="/root/.local/bin:$PATH"
fi

cd /root
if [ ! -d youtube-video-client ]; then
  git clone https://github.com/yentur/youtube-video-client.git
fi
cd youtube-video-client

if [ ! -d .venv ]; then
  uv venv .venv
fi
. .venv/bin/activate
uv pip install -r requirements.txt

ATTEMPTS=0
while [ "$ATTEMPTS" -lt 50 ]; do
  ATTEMPTS=$((ATTEMPTS+1))
  echo "[entrypoint] attempt $ATTEMPTS — pulling latest + launching client"
  git pull --quiet 2>&1 || true
  uv pip install --quiet -r requirements.txt 2>&1 | tail -1 || true
  python main.py || true
  echo "[entrypoint] client exited; sleeping 5s before retry"
  sleep 5
done
echo "[entrypoint] exhausted retries — exiting"
