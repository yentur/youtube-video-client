"""YouTube downloader client — per-link cycle.

Loop:
  1) /api/v1/register -> get AWS creds + rules
  2) /api/v1/get-link -> a single link
  3) yt-dlp audio (+ subtitle if available)
  4) S3 upload (skip if object exists)
  5) /api/v1/complete -> server records + tells us if we should shutdown

Retries:
  - up to 5 attempts on a single link (transient failures)
  - 3 consecutive distinct-link errors -> /request-shutdown -> exit 42

Env:
  API_BASE_URL      orchestrator URL, e.g. http://1.2.3.4:8765
  CLIENT_ID         stable client id (set by orchestrator on rent)
  VAST_INSTANCE_ID  optional Vast container id
  COOKIES_URL       optional s3://... or https://... cookies.txt for yt-dlp
"""
from __future__ import annotations

import json
import logging
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.parse
import uuid
from pathlib import Path
from typing import Any

import boto3
import requests
from botocore.exceptions import ClientError

# ---------- env ----------
API_BASE_URL = os.environ.get("API_BASE_URL", "").rstrip("/")
CLIENT_ID = os.environ.get("CLIENT_ID") or ("client-" + uuid.uuid4().hex[:10])
VAST_INSTANCE_ID = os.environ.get("VAST_INSTANCE_ID", os.environ.get("CONTAINER_ID", ""))
COOKIES_URL = os.environ.get("COOKIES_URL", "").strip()
COOKIES_FILE = "/tmp/yt_cookies.txt"

LOG = logging.getLogger("client")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    stream=sys.stdout,
)


# ---------- HTTP helpers ----------
def _req(method: str, path: str, **kwargs) -> dict:
    url = f"{API_BASE_URL}{path}"
    last = None
    for attempt in range(8):
        try:
            r = requests.request(method, url, timeout=kwargs.pop("timeout", 30), **kwargs)
            if r.status_code >= 500:
                raise requests.HTTPError(f"{r.status_code} {r.text[:200]}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            sleep = min(30, 2 ** attempt) + random.uniform(0, 0.5)
            LOG.warning("HTTP %s %s attempt=%d failed: %s; retrying in %.1fs",
                        method, path, attempt, e, sleep)
            time.sleep(sleep)
    raise RuntimeError(f"HTTP {method} {path} failed after retries: {last}")


def register() -> dict:
    body = {
        "client_id": CLIENT_ID,
        "vast_instance_id": VAST_INSTANCE_ID,
        "ip": _public_ip(),
    }
    return _req("POST", "/api/v1/register", json=body)


def get_link() -> dict:
    return _req("GET", "/api/v1/get-link", params={"client_id": CLIENT_ID})


def complete(payload: dict) -> dict:
    return _req("POST", "/api/v1/complete", json=payload)


def request_shutdown(reason: str) -> dict:
    return _req("POST", "/api/v1/request-shutdown", json={"client_id": CLIENT_ID, "reason": reason})


def _public_ip() -> str:
    for url in ("https://api.ipify.org", "https://ifconfig.me/ip"):
        try:
            r = requests.get(url, timeout=5)
            if r.ok:
                return r.text.strip()
        except Exception:
            pass
    return ""


# ---------- video id ----------
_YT_ID_RE = re.compile(r"(?:v=|/shorts/|youtu\.be/)([A-Za-z0-9_-]{6,})")

def video_id(url: str) -> str:
    m = _YT_ID_RE.search(url)
    if m:
        return m.group(1)
    return url[-16:]


# ---------- cookies ----------
def fetch_cookies(cookies_url: str, s3) -> str:
    if not cookies_url:
        return ""
    try:
        if cookies_url.startswith("s3://"):
            u = urllib.parse.urlparse(cookies_url)
            s3.download_file(u.netloc, u.path.lstrip("/"), COOKIES_FILE)
        else:
            r = requests.get(cookies_url, timeout=15)
            r.raise_for_status()
            Path(COOKIES_FILE).write_text(r.text)
        LOG.info("cookies fetched -> %s", COOKIES_FILE)
        return COOKIES_FILE
    except Exception as e:
        LOG.warning("cookies fetch failed: %s", e)
        return ""


# ---------- yt-dlp ----------
def ytdlp_download(url: str, workdir: str, cookies_path: str) -> dict:
    """Download bestaudio + subtitle (any auto/manual). Returns metadata dict.

    Output:
      audio: {workdir}/{video_id}.{ext}    (m4a/webm/mp3 — whatever bestaudio is)
      subs : {workdir}/{video_id}.{lang}.{srt|vtt}
    """
    vid = video_id(url)
    out_tmpl = os.path.join(workdir, f"{vid}.%(ext)s")
    cmd = [
        "yt-dlp",
        "-f", "bestaudio/best",
        "--no-playlist",
        "--no-overwrites",
        "--ignore-errors",
        "--no-warnings",
        "--write-info-json",
        "--write-subs",
        "--write-auto-subs",
        "--sub-langs", "all",
        "--convert-subs", "srt",
        "--retries", "3",
        "--socket-timeout", "30",
        "-o", out_tmpl,
    ]
    if cookies_path and os.path.exists(cookies_path):
        cmd += ["--cookies", cookies_path]
    cmd.append(url)

    LOG.info("yt-dlp run: %s", url)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
    if proc.returncode != 0:
        # surface the last meaningful line from stderr/stdout
        msg = (proc.stderr or proc.stdout or "")[-500:]
        raise RuntimeError(f"yt-dlp rc={proc.returncode}: {msg.strip()}")

    # locate audio file
    audio_path = ""
    for p in Path(workdir).glob(f"{vid}.*"):
        if p.suffix.lower() in (".m4a", ".webm", ".mp3", ".opus", ".ogg", ".aac", ".wav"):
            audio_path = str(p)
            break
    if not audio_path:
        # try by info.json title fallback
        infos = list(Path(workdir).glob(f"{vid}.info.json"))
        if not infos:
            raise RuntimeError("no audio file produced")
    if not audio_path:
        # last resort — pick any file that isn't info.json/sub
        for p in Path(workdir).iterdir():
            if p.suffix.lower() not in (".json", ".srt", ".vtt"):
                audio_path = str(p)
                break
    if not audio_path:
        raise RuntimeError("no audio file produced")

    # locate subtitles (any language)
    sub_paths = sorted(Path(workdir).glob(f"{vid}*.srt"))
    sub_path = str(sub_paths[0]) if sub_paths else ""

    # info.json -> channel/title
    info_path = Path(workdir) / f"{vid}.info.json"
    channel = "unknown"
    title = vid
    if info_path.exists():
        try:
            info = json.loads(info_path.read_text(encoding="utf-8", errors="ignore"))
            channel = (info.get("channel") or info.get("uploader") or "unknown").strip() or "unknown"
            title = (info.get("title") or vid).strip()
        except Exception:
            pass

    return {
        "video_id": vid,
        "audio_path": audio_path,
        "sub_path": sub_path,
        "channel": channel,
        "title": title,
    }


# ---------- S3 ----------
_SAFE_RE = re.compile(r"[^A-Za-z0-9._\-]")

def safe_seg(s: str, max_len: int = 80) -> str:
    s = _SAFE_RE.sub("_", s).strip("_")
    return s[:max_len] or "x"


def s3_key(prefix: str, vtype: str, channel: str, video_id_: str, ext: str) -> str:
    parts = []
    if prefix:
        parts.append(prefix.strip("/"))
    parts.append(safe_seg(vtype))
    parts.append(safe_seg(channel))
    parts.append(f"{safe_seg(video_id_)}{ext}")
    return "/".join(parts)


def s3_object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def s3_upload(s3, local_path: str, bucket: str, key: str) -> None:
    s3.upload_file(local_path, bucket, key)


# ---------- main loop ----------
def run() -> int:
    if not API_BASE_URL:
        LOG.error("API_BASE_URL not set")
        return 1

    LOG.info("client starting: id=%s vast=%s api=%s", CLIENT_ID, VAST_INSTANCE_ID, API_BASE_URL)

    reg = register()
    LOG.info("registered ok=%s bucket=%s", reg.get("ok"), reg.get("s3_bucket"))

    s3 = boto3.client(
        "s3",
        aws_access_key_id=reg["aws_access_key_id"],
        aws_secret_access_key=reg["aws_secret_access_key"],
        region_name=reg.get("aws_region", "us-east-1"),
    )

    cookies_path = fetch_cookies(reg.get("cookies_url", "") or COOKIES_URL, s3)

    bucket = reg["s3_bucket"]
    prefix = reg.get("s3_prefix", "") or ""
    max_retries_same = int(reg.get("max_retries_same_video_per_client", 5))
    max_consec = int(reg.get("max_consecutive_failures_per_client", 3))

    consecutive_link_failures = 0
    empty_loops = 0

    while True:
        try:
            pkt = get_link()
        except Exception as e:
            LOG.error("get-link failed: %s; sleeping 15s", e)
            time.sleep(15)
            continue

        if pkt.get("blacklisted"):
            LOG.error("blacklisted by server, exiting")
            return 42

        if pkt.get("empty"):
            if pkt.get("done"):
                LOG.info("server says all_done; exiting cleanly")
                return 0
            empty_loops += 1
            if empty_loops >= 18:  # ~3 minutes of no-work
                LOG.info("no work for ~3 minutes; requesting shutdown")
                try: request_shutdown("idle-no-work")
                except Exception: pass
                return 42
            LOG.info("server says wait; sleeping 10s (empty=%d)", empty_loops)
            time.sleep(10)
            continue

        empty_loops = 0
        link = pkt["link"]
        vtype = pkt.get("type", "unknown")
        result = process_link(s3, bucket, prefix, link, vtype, cookies_path, max_retries_same)
        try:
            comp = complete(result)
        except Exception as e:
            LOG.error("complete post failed: %s", e)
            comp = {"ok": False}

        # streak counting
        if result["status"] == "error":
            consecutive_link_failures += 1
            LOG.warning("consecutive failures: %d/%d", consecutive_link_failures, max_consec)
        else:
            consecutive_link_failures = 0

        if comp.get("shutdown") or consecutive_link_failures >= max_consec:
            LOG.warning("shutdown signal or threshold hit; requesting shutdown")
            try: request_shutdown(comp.get("reason") or "consecutive-failures")
            except Exception: pass
            return 42

        if comp.get("all_done"):
            LOG.info("all_done from server; exiting")
            return 0


def process_link(s3, bucket: str, prefix: str, url: str, vtype: str,
                 cookies_path: str, max_retries: int) -> dict:
    """Try to download & upload one link, with up to max_retries attempts.
    Returns the body to POST to /api/v1/complete."""
    vid = video_id(url)
    last_err = ""
    workdir = tempfile.mkdtemp(prefix=f"yt_{vid}_")
    started = time.time()

    # cheap S3 skip check (server has its own)
    audio_key_guess = s3_key(prefix, vtype, "unknown", vid, ".m4a")
    # we don't know channel yet, so just do the strict check after download

    try:
        for attempt in range(1, max_retries + 1):
            try:
                meta = ytdlp_download(url, workdir, cookies_path)
                channel = meta["channel"] or "unknown"
                ext = os.path.splitext(meta["audio_path"])[1] or ".bin"
                audio_key = s3_key(prefix, vtype, channel, vid, ext)

                if s3_object_exists(s3, bucket, audio_key):
                    LOG.info("S3 already has %s/%s -> skipped", bucket, audio_key)
                    return _result(url, "skipped", False, audio_key, "", started, "already in s3")

                s3_upload(s3, meta["audio_path"], bucket, audio_key)

                sub_key = ""
                has_sub = False
                if meta["sub_path"]:
                    sub_ext = os.path.splitext(meta["sub_path"])[1] or ".srt"
                    sub_key = s3_key(prefix, vtype, channel, vid, sub_ext)
                    s3_upload(s3, meta["sub_path"], bucket, sub_key)
                    has_sub = True

                status = "ok" if has_sub else "no_subtitle"
                return _result(url, status, has_sub, audio_key, sub_key, started)
            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"
                LOG.warning("attempt %d/%d for %s: %s", attempt, max_retries, url, last_err)
                # wipe partials so next attempt is clean
                for p in Path(workdir).iterdir():
                    try: p.unlink()
                    except Exception: pass
                time.sleep(min(8.0, 1.5 * attempt))
        return _result(url, "error", False, "", "", started, last_err or "max retries hit")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def _result(url: str, status: str, has_sub: bool, audio_key: str,
            sub_key: str, started: float, err: str = "") -> dict:
    return {
        "client_id": CLIENT_ID,
        "link": url,
        "status": status,
        "has_subtitle": has_sub,
        "s3_audio_key": audio_key,
        "s3_subtitle_key": sub_key,
        "duration_sec": round(time.time() - started, 2),
        "error": err,
    }


if __name__ == "__main__":
    try:
        rc = run()
    except KeyboardInterrupt:
        rc = 130
    except Exception:
        traceback.print_exc()
        rc = 1
    sys.exit(rc)
