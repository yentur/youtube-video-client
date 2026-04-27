"""YouTube downloader client for the vast-dataset orchestrator.

Behaviour:
  - register with main API
  - loop: get-next-video → download audio (+ subtitle if available) → S3 → notify
  - 5 retries on the same video before giving up
  - if 3 different videos fail consecutively, ask the orchestrator to shut us
    down (which will destroy the Vast instance)
  - skip if the file is already in S3
  - heartbeat the API while working

Env vars:
  API_BASE_URL      — orchestrator URL, e.g. http://1.2.3.4:8765
  CLIENT_ID         — stable client id (matches orchestrator slot)
  VAST_INSTANCE_ID  — vast container label (best-effort, optional)
"""
import json
import logging
import os
import random
import shutil
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

import boto3
import requests
import yt_dlp


# ---------- config ----------
API_BASE_URL = os.environ.get("API_BASE_URL", "").rstrip("/")
CLIENT_ID = os.environ.get("CLIENT_ID") or ("client-" + uuid.uuid4().hex[:12])
VAST_INSTANCE_ID = os.environ.get("VAST_INSTANCE_ID", "")

LOG = logging.getLogger("client")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

S3 = {
    "access_key_id": None, "secret_access_key": None, "region": None,
    "bucket": None, "prefix": "",
}
LIMITS = {"max_retries_same_video": 5, "max_consecutive_failures": 3}


# ---------- API helpers ----------
def _post(path: str, json_body: dict, timeout: int = 30) -> dict:
    url = f"{API_BASE_URL}{path}"
    r = requests.post(url, json=json_body, timeout=timeout,
                      headers={"X-Client-Id": CLIENT_ID})
    r.raise_for_status()
    return r.json()


def _get(path: str, params: dict | None = None, timeout: int = 30) -> dict:
    url = f"{API_BASE_URL}{path}"
    r = requests.get(url, params=params or {}, timeout=timeout,
                     headers={"X-Client-Id": CLIENT_ID})
    r.raise_for_status()
    return r.json()


def register():
    LOG.info(f"Registering as {CLIENT_ID} (vast={VAST_INSTANCE_ID})")
    return _post("/register", {"instance_id": VAST_INSTANCE_ID or None})


def fetch_config():
    LOG.info("Fetching config from orchestrator")
    data = _get("/get-config")
    cfg = data["config"]
    aws = cfg["aws"]
    S3.update({
        "access_key_id": aws["access_key_id"],
        "secret_access_key": aws["secret_access_key"],
        "region": aws["region"],
        "bucket": aws["s3_bucket"],
        "prefix": aws.get("s3_prefix", ""),
    })
    LIMITS.update(cfg.get("limits", {}))
    LOG.info(f"S3 bucket={S3['bucket']} region={S3['region']}")


def get_next_video() -> dict | None:
    data = _get("/get-next-video", params={"client_id": CLIENT_ID})
    if data.get("status") != "success":
        LOG.info(f"No more videos: {data.get('message')}")
        return None
    return data


def notify_completion(video_id: str, status: str, message: str = "",
                      has_subtitle: bool | None = None,
                      s3_audio: str | None = None,
                      s3_subtitle: str | None = None) -> dict:
    return _post("/notify-completion", {
        "client_id": CLIENT_ID,
        "video_id": video_id,
        "status": status,
        "message": message[:500] if message else "",
        "has_subtitle": has_subtitle,
        "s3_audio": s3_audio,
        "s3_subtitle": s3_subtitle,
    })


def request_shutdown(reason: str):
    try:
        _post("/shutdown-request", {"client_id": CLIENT_ID, "reason": reason})
    except Exception as e:
        LOG.warning(f"shutdown request failed: {e}")


def heartbeat_loop(stop_event: threading.Event):
    while not stop_event.wait(60):
        try:
            _post("/heartbeat", {"client_id": CLIENT_ID}, timeout=10)
        except Exception as e:
            LOG.debug(f"heartbeat failed: {e}")


# ---------- S3 helpers ----------
def s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=S3["access_key_id"],
        aws_secret_access_key=S3["secret_access_key"],
        region_name=S3["region"],
    )


def s3_object_exists(s3, key: str) -> bool:
    try:
        s3.head_object(Bucket=S3["bucket"], Key=key)
        return True
    except Exception:
        return False


def s3_upload(s3, path: str, key: str) -> str:
    LOG.info(f"S3 upload → s3://{S3['bucket']}/{key}")
    s3.upload_file(path, S3["bucket"], key)
    return f"s3://{S3['bucket']}/{key}"


def safe_name(s: str, n: int = 100) -> str:
    return "".join(c if c.isalnum() or c in " -_()" else "_" for c in s)[:n].strip(" _-") or "untitled"


# ---------- yt-dlp helpers ----------
YDL_COMMON = {
    "quiet": True,
    "no_warnings": True,
    "noplaylist": True,
    "no_check_certificate": True,
    # `mweb` + `tv` are the two clients that most reliably hand out playable
    # stream URLs without a po_token on cloud IPs. `missing_pot` tells yt-dlp
    # to keep formats even when the proof-of-origin token isn't available.
    "extractor_args": {
        "youtube": {
            "player_client": ["mweb", "tv", "web_safari", "ios"],
            "formats": "missing_pot",
        },
    },
    "http_headers": {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                      "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
                      "Mobile/15E148 Safari/604.1",
    },
    "geo_bypass": True,
}


def probe_video(url: str) -> dict:
    opts = {**YDL_COMMON, "skip_download": True}
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False)


def pick_subtitle_lang(info: dict) -> str | None:
    """Prefer manual subs, fall back to auto. Prefer tr → en → first."""
    manual = info.get("subtitles") or {}
    auto = info.get("automatic_captions") or {}
    candidates = list(manual.keys()) or list(auto.keys())
    if not candidates:
        return None
    for pref in ("tr", "en"):
        if pref in candidates:
            return pref
    # any base language match (en-US -> en)
    for pref in ("tr", "en"):
        for c in candidates:
            if c.startswith(pref):
                return c
    return candidates[0]


def download_audio_and_subs(url: str, work_dir: str, video_title: str
                            ) -> tuple[str | None, str | None, bool]:
    """Returns (audio_path, subtitle_path, had_subtitle_attempted)."""
    base = safe_name(video_title)
    output_template = os.path.join(work_dir, f"{base}.%(ext)s")
    wav_path = os.path.join(work_dir, f"{base}.wav")

    info = probe_video(url)
    sub_lang = pick_subtitle_lang(info)

    # 1. download audio (always)
    audio_opts = {
        **YDL_COMMON,
        "format": "bestaudio/best",
        "outtmpl": output_template,
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "wav",
            "preferredquality": "192",
        }],
        "postprocessor_args": ["-ar", "16000"],
        "retries": 3,
        "fragment_retries": 3,
        "concurrent_fragment_downloads": 4,
    }
    with yt_dlp.YoutubeDL(audio_opts) as ydl:
        ydl.download([url])

    if not os.path.exists(wav_path):
        # yt-dlp may have used a slightly different filename
        candidates = list(Path(work_dir).glob(f"{base}*.wav"))
        if candidates:
            wav_path = str(candidates[0])
        else:
            raise RuntimeError("audio file not produced")

    # 2. try subtitle (best-effort)
    sub_path = None
    if sub_lang:
        sub_opts = {
            **YDL_COMMON,
            "skip_download": True,
            "writesubtitles": True,
            "writeautomaticsub": True,
            "subtitleslangs": [sub_lang],
            "subtitlesformat": "srt",
            "outtmpl": output_template,
        }
        try:
            with yt_dlp.YoutubeDL(sub_opts) as ydl:
                ydl.download([url])
            cands = list(Path(work_dir).glob(f"{base}*.srt")) + \
                    list(Path(work_dir).glob(f"{base}*.vtt"))
            if cands:
                sub_path = str(cands[0])
        except Exception as e:
            LOG.warning(f"subtitle download failed: {e}")

    return wav_path, sub_path, sub_lang is not None


# ---------- main per-video flow ----------
def process_video(video: dict, s3) -> tuple[str, str, dict]:
    """Return (status, message, extras). status is one of:
    success | no_subtitle | exists | error
    """
    url = video["video_url"]
    vtype = video.get("type", "default")
    work_dir = tempfile.mkdtemp(prefix="yt_")
    try:
        info = probe_video(url)
        title = info.get("title", "untitled")
        channel = info.get("uploader", "unknown")
        base = safe_name(title)
        ch = safe_name(channel, 50)
        prefix = (S3["prefix"].strip("/") + "/") if S3["prefix"].strip("/") else ""
        wav_key = f"{prefix}{vtype}/{ch}/{base}.wav"
        sub_key = f"{prefix}{vtype}/{ch}/{base}.srt"

        wav_exists = s3_object_exists(s3, wav_key)
        sub_exists = s3_object_exists(s3, sub_key)
        if wav_exists and sub_exists:
            return "exists", "already in s3", {
                "s3_audio": f"s3://{S3['bucket']}/{wav_key}",
                "s3_subtitle": f"s3://{S3['bucket']}/{sub_key}",
                "has_subtitle": True,
            }

        wav_path, sub_path, attempted_sub = download_audio_and_subs(url, work_dir, title)

        s3_audio = None
        if not wav_exists:
            s3_audio = s3_upload(s3, wav_path, wav_key)
        else:
            s3_audio = f"s3://{S3['bucket']}/{wav_key}"

        s3_sub = None
        if sub_path:
            s3_sub = s3_upload(s3, sub_path, sub_key)
            return "success", "ok", {"s3_audio": s3_audio, "s3_subtitle": s3_sub,
                                     "has_subtitle": True}
        else:
            # audio uploaded, no subtitle available
            return "no_subtitle", "audio uploaded; no subtitle", {
                "s3_audio": s3_audio, "s3_subtitle": None, "has_subtitle": False,
            }
    finally:
        try:
            shutil.rmtree(work_dir, ignore_errors=True)
        except Exception:
            pass


def process_video_with_retry(video: dict, s3, max_retries: int) -> tuple[str, str, dict]:
    last_err = ""
    for attempt in range(1, max_retries + 1):
        try:
            LOG.info(f"[{attempt}/{max_retries}] processing {video['video_url']}")
            return process_video(video, s3)
        except Exception as e:
            last_err = str(e)
            LOG.warning(f"attempt {attempt} failed: {e}")
            time.sleep(min(2 ** attempt, 30) + random.uniform(0, 2))
    return "error", last_err, {}


# ---------- top-level loop ----------
def run():
    if not API_BASE_URL:
        LOG.error("API_BASE_URL is not set; cannot continue")
        sys.exit(1)

    # registration retry: orchestrator may not be reachable instantly
    for i in range(20):
        try:
            register()
            fetch_config()
            break
        except Exception as e:
            LOG.warning(f"register failed (try {i+1}): {e}")
            time.sleep(min(5 + i * 3, 60))
    else:
        LOG.error("Could not register with orchestrator — exiting")
        sys.exit(2)

    s3 = s3_client()
    consecutive_failures = 0
    stop_hb = threading.Event()
    threading.Thread(target=heartbeat_loop, args=(stop_hb,), daemon=True).start()

    try:
        while True:
            try:
                video = get_next_video()
            except Exception as e:
                LOG.warning(f"get-next-video failed: {e}")
                time.sleep(15)
                continue

            if video is None:
                LOG.info("No more videos — exiting normally")
                return

            status, msg, extras = process_video_with_retry(
                video, s3, max_retries=LIMITS["max_retries_same_video"]
            )

            try:
                resp = notify_completion(
                    video["video_id"], status, msg,
                    has_subtitle=extras.get("has_subtitle"),
                    s3_audio=extras.get("s3_audio"),
                    s3_subtitle=extras.get("s3_subtitle"),
                )
            except Exception as e:
                LOG.warning(f"notify_completion failed: {e}")
                resp = {}

            if status == "error":
                consecutive_failures += 1
                LOG.warning(f"consecutive failures: {consecutive_failures}")
                if (consecutive_failures >= LIMITS["max_consecutive_failures"]
                        or resp.get("shutdown")):
                    request_shutdown(
                        f"{consecutive_failures} consecutive failures"
                    )
                    LOG.error("requesting shutdown — exiting")
                    return
            else:
                consecutive_failures = 0

            time.sleep(random.uniform(0.5, 2.0))
    finally:
        stop_hb.set()


if __name__ == "__main__":
    run()
