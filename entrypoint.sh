#!/bin/bash
# Vast.ai onstart entrypoint — clones the client repo and runs main.py.
# Usually NOT called directly: the orchestrator's onstart_cmd already does the
# clone + run. This file is provided for ad-hoc use (e.g. local debug).

set -eu
echo "[entrypoint] starting at $(date -Is)"

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y --no-install-recommends git curl ffmpeg ca-certificates python3 python3-venv python3-pip

cd /root
if [ -d youtube-video-client ]; then rm -rf youtube-video-client; fi
git clone --depth=1 https://github.com/yentur/youtube-video-client.git
cd youtube-video-client

python3 -m pip install --no-cache-dir --upgrade pip
python3 -m pip install --no-cache-dir -r requirements.txt

while true; do
  echo "[entrypoint] launching client at $(date -Is)"
  python3 main.py
  rc=$?
  if [ "$rc" = "42" ]; then
    echo "[entrypoint] client requested shutdown (rc=42); sleeping forever"
    sleep infinity
  fi
  echo "[entrypoint] client exited rc=$rc; restarting in 10s"
  sleep 10
done
