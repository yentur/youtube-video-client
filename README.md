# YouTube Video Client

Per-link cycle worker for the **YT Dataset Orchestrator** (`https://github.com/yentur/youtube-video-client` / Vast.ai).

## What it does
1. Registers with the orchestrator → receives AWS creds + rules.
2. Loops:
   - Asks for the next link (`GET /api/v1/get-link`).
   - Downloads bestaudio + subtitles (any language) with `yt-dlp`.
   - Uploads to S3:
     - `s3://<bucket>/<prefix>/<type>/<channel>/<video_id>.<ext>` (audio)
     - `s3://<bucket>/<prefix>/<type>/<channel>/<video_id>.srt`   (subtitle, if any)
   - If the audio object already exists, it's skipped (idempotent).
   - Reports completion to the orchestrator.
3. Retries each link up to **5** times. After **3** consecutive distinct-link
   failures it requests its own shutdown and exits with rc=42.

## Required env
| Variable           | Meaning                                                       |
| ------------------ | ------------------------------------------------------------- |
| `API_BASE_URL`     | Orchestrator URL, e.g. `http://1.2.3.4:8765`                   |
| `CLIENT_ID`        | Stable client id (orchestrator sets this on rent)              |
| `VAST_INSTANCE_ID` | Vast container id (optional, used for tracking)                |
| `COOKIES_URL`      | Optional `s3://...` or `https://...` to a yt-dlp cookies file  |

## Run locally (debug)
```bash
pip install -r requirements.txt
API_BASE_URL=http://localhost:8765 CLIENT_ID=local-debug python3 main.py
```

## Exit codes
- `0`  — work is done (server said all_done).
- `42` — server requested shutdown OR self-decided (3 consecutive failures, idle).
- `1`  — fatal error.
