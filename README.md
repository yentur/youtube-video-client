# youtube-video-client

Client component of the **vast-dataset** distributed YouTube downloader.
Designed to run on an ephemeral Vast.ai instance and pull work from a
central orchestrator.

## What it does

1. Registers itself with the orchestrator (`API_BASE_URL`) using its
   `CLIENT_ID`.
2. Fetches AWS S3 credentials and limits from the orchestrator.
3. In a loop:
   - asks for the next YouTube link,
   - downloads the audio (16 kHz WAV) with `yt-dlp`,
   - downloads a subtitle if any is available (manual preferred, auto
     fallback; tr → en → first language),
   - uploads both files to `s3://<bucket>/<type>/<channel>/<title>.{wav,srt}`,
   - reports the result back to the orchestrator.
4. Retries the same link up to **5×** with exponential backoff. If
   **3 different links fail in a row**, asks the orchestrator to destroy
   this Vast instance and exits.

Skips silently if the file already exists in S3.

## Required env vars

| Variable           | Meaning                                       |
| ------------------ | --------------------------------------------- |
| `API_BASE_URL`     | Orchestrator URL, e.g. `http://1.2.3.4:8765` |
| `CLIENT_ID`        | Orchestrator-assigned slot id (optional)      |
| `VAST_INSTANCE_ID` | Vast container label (optional, best-effort)  |

## Local run

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
API_BASE_URL=http://localhost:8765 CLIENT_ID=dev python main.py
```

## On a Vast.ai box

`entrypoint.sh` is the canonical onstart script — it installs deps, clones
this repo, then runs `main.py` in a restart loop.
