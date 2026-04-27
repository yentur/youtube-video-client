"""YouTube downloader client — concurrent batch worker.

Behaviour:
  - register with main API
  - fetch S3 creds + worker count
  - main loop:
      * top up an in-memory queue from /get-next-video-batch
      * N worker threads pop one video, download (audio + sub if any),
        upload to S3, post completion individually
  - 5 retries per video before reporting error
  - server decides when to shut us down (returns shutdown=True)

Env vars:
  API_BASE_URL      — orchestrator URL (e.g. http://1.2.3.4:8765)
  CLIENT_ID         — stable client id (matches orchestrator slot)
  VAST_INSTANCE_ID  — Vast container label, optional
  CLIENT_WORKERS    — concurrent download workers (default 4)
  COOKIES_URL       — optional: HTTPS or s3:// cookies.txt for yt-dlp
"""
import logging
import os
import queue
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
COOKIES_URL = os.environ.get("COOKIES_URL", "").strip()
COOKIES_FILE = "/tmp/yt_cookies.txt"

LOG = logging.getLogger("client")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    stream=sys.stdout,
)

S3 = {
    "access_key_id": None, "secret_access_key": None, "region": None,
    "bucket": None, "prefix": "",
}
LIMITS = {
    "max_retries_same_video": 5,
    "max_consecutive_failures": 1,
    "client_workers": int(os.environ.get("CLIENT_WORKERS", "4") or 4),
    "client_batch_size": 8,
}


# ---------- yt-dlp common opts ----------
YDL_COMMON = {
    "quiet": True,
    "no_warnings": True,
    "noplaylist": True,
    "no_check_certificate": True,
    # `android_vr` + `tv_simply` give us audio URLs that are (a) playable
    # without po_token and (b) usually NOT DRM-locked. The "web_*" clients
    # often hand back URLs that 403 on download even though metadata
    # extraction succeeds. `formats: missing_pot` keeps formats yt-dlp
    # would otherwise drop because no proof-of-origin token was derived.
    "extractor_args": {
        "youtube": {
            "player_client": ["android_vr", "tv_simply", "ios"],
            "formats": "missing_pot",
        },
    },
    "http_headers": {
        "User-Agent": "com.google.android.youtube/19.09.37 (Linux; U; Android 14)",
    },
    "geo_bypass": True,
    "socket_timeout": 30,
    "force_ipv4": True,
}


# yt-dlp messages that mean "this URL is poisoned for us — retrying with
# the same client/IP will not help". Bail out early on these instead of
# burning 5 retries.
FATAL_DOWNLOAD_ERRORS = (
    "HTTP Error 403",
    "Sign in to confirm",
    "DRM protected",
    "Video unavailable",
    "Private video",
    "members-only",
    "removed by the uploader",
    "country which is not available",
    "Premieres",
    "premiered",
)


# ---------- API helpers ----------
_session = requests.Session()


def _post(path: str, json_body: dict, timeout: int = 30) -> dict:
    r = _session.post(f"{API_BASE_URL}{path}", json=json_body, timeout=timeout,
                      headers={"X-Client-Id": CLIENT_ID})
    r.raise_for_status()
    return r.json()


def _get(path: str, params: dict | None = None, timeout: int = 30) -> dict:
    r = _session.get(f"{API_BASE_URL}{path}", params=params or {}, timeout=timeout,
                     headers={"X-Client-Id": CLIENT_ID})
    r.raise_for_status()
    return r.json()


def register():
    LOG.info(f"REGISTER as {CLIENT_ID} (vast={VAST_INSTANCE_ID})")
    return _post("/register", {"instance_id": VAST_INSTANCE_ID or None})


def fetch_config():
    LOG.info("fetching config from orchestrator")
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
    LOG.info(f"S3 bucket={S3['bucket']} workers={LIMITS['client_workers']} "
             f"batch={LIMITS['client_batch_size']}")


def fetch_cookies():
    if not COOKIES_URL:
        return
    try:
        if COOKIES_URL.startswith("s3://"):
            no_scheme = COOKIES_URL[5:]
            bucket, _, key = no_scheme.partition("/")
            s3_client().download_file(bucket, key, COOKIES_FILE)
        else:
            r = _session.get(COOKIES_URL, timeout=30)
            r.raise_for_status()
            with open(COOKIES_FILE, "wb") as f:
                f.write(r.content)
        YDL_COMMON["cookiefile"] = COOKIES_FILE
        LOG.info(f"loaded cookies from {COOKIES_URL}")
    except Exception as e:
        LOG.warning(f"could not load cookies: {e}")


def get_batch(n: int) -> list[dict]:
    data = _get("/get-next-video-batch", params={"client_id": CLIENT_ID, "n": n})
    if data.get("status") != "success":
        return []
    return data.get("videos", [])


def notify_completion(video_id: str, status: str, message: str = "",
                      has_subtitle: bool | None = None,
                      s3_audio: str | None = None,
                      s3_subtitle: str | None = None) -> dict:
    return _post("/notify-completion", {
        "client_id": CLIENT_ID, "video_id": video_id, "status": status,
        "message": message[:500] if message else "",
        "has_subtitle": has_subtitle,
        "s3_audio": s3_audio, "s3_subtitle": s3_subtitle,
    })


def request_shutdown(reason: str):
    try:
        _post("/shutdown-request", {"client_id": CLIENT_ID, "reason": reason})
    except Exception as e:
        LOG.warning(f"shutdown request failed: {e}")


def heartbeat_loop(stop: threading.Event):
    while not stop.wait(30):
        try:
            _post("/heartbeat", {"client_id": CLIENT_ID}, timeout=10)
        except Exception as e:
            LOG.debug(f"heartbeat fail: {e}")


# ---------- S3 ----------
_s3_local = threading.local()


def s3_client():
    s = getattr(_s3_local, "s3", None)
    if s is None:
        s = boto3.client(
            "s3",
            aws_access_key_id=S3["access_key_id"],
            aws_secret_access_key=S3["secret_access_key"],
            region_name=S3["region"],
        )
        _s3_local.s3 = s
    return s


def s3_object_exists(s3, key: str) -> bool:
    try:
        s3.head_object(Bucket=S3["bucket"], Key=key)
        return True
    except Exception:
        return False


def s3_upload(s3, path: str, key: str) -> str:
    LOG.info(f"S3 UPLOAD start key={key} size={os.path.getsize(path)}")
    t0 = time.time()
    s3.upload_file(path, S3["bucket"], key)
    LOG.info(f"S3 UPLOAD done  key={key} took={time.time()-t0:.1f}s")
    return f"s3://{S3['bucket']}/{key}"


def safe_name(s: str, n: int = 100) -> str:
    return ("".join(c if c.isalnum() or c in " -_()" else "_" for c in s)[:n]
            .strip(" _-") or "untitled")


# ---------- yt-dlp helpers ----------
def probe_video(url: str) -> dict:
    opts = {**YDL_COMMON, "skip_download": True}
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False)


def pick_subtitle_lang(info: dict) -> str | None:
    manual = info.get("subtitles") or {}
    auto = info.get("automatic_captions") or {}
    candidates = list(manual.keys()) or list(auto.keys())
    if not candidates:
        return None
    for pref in ("tr", "en"):
        if pref in candidates:
            return pref
        for c in candidates:
            if c.startswith(pref):
                return c
    return candidates[0]


def download_audio_and_subs(url: str, work_dir: str, video_title: str
                            ) -> tuple[str | None, str | None, bool]:
    base = safe_name(video_title)
    output_template = os.path.join(work_dir, f"{base}.%(ext)s")
    wav_path = os.path.join(work_dir, f"{base}.wav")

    info = probe_video(url)
    sub_lang = pick_subtitle_lang(info)

    audio_opts = {
        **YDL_COMMON,
        "format": "bestaudio[has_drm=false]/bestaudio/best[has_drm=false]",
        "outtmpl": output_template,
        "postprocessors": [{"key": "FFmpegExtractAudio",
                            "preferredcodec": "wav",
                            "preferredquality": "192"}],
        "postprocessor_args": ["-ar", "16000"],
        "retries": 3,
        "fragment_retries": 3,
        "concurrent_fragment_downloads": 4,
    }
    with yt_dlp.YoutubeDL(audio_opts) as ydl:
        ydl.download([url])

    if not os.path.exists(wav_path):
        candidates = list(Path(work_dir).glob(f"{base}*.wav"))
        if candidates:
            wav_path = str(candidates[0])
        else:
            raise RuntimeError("audio file not produced")

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
            LOG.warning(f"subtitle dl failed for {url}: {e}")

    return wav_path, sub_path, sub_lang is not None


def process_video_once(video: dict) -> tuple[str, str, dict]:
    """Single-shot processing — returns (status, message, extras)."""
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

        s3 = s3_client()
        wav_exists = s3_object_exists(s3, wav_key)
        sub_exists = s3_object_exists(s3, sub_key)
        if wav_exists and sub_exists:
            return "exists", "already in s3", {
                "s3_audio": f"s3://{S3['bucket']}/{wav_key}",
                "s3_subtitle": f"s3://{S3['bucket']}/{sub_key}",
                "has_subtitle": True,
            }

        wav_path, sub_path, _ = download_audio_and_subs(url, work_dir, title)

        s3_audio = (s3_upload(s3, wav_path, wav_key) if not wav_exists
                    else f"s3://{S3['bucket']}/{wav_key}")
        s3_sub = None
        if sub_path and not sub_exists:
            s3_sub = s3_upload(s3, sub_path, sub_key)
        elif sub_exists:
            s3_sub = f"s3://{S3['bucket']}/{sub_key}"

        if s3_sub:
            return "success", "ok", {"s3_audio": s3_audio, "s3_subtitle": s3_sub,
                                     "has_subtitle": True}
        return "no_subtitle", "no subtitle available", {
            "s3_audio": s3_audio, "s3_subtitle": None, "has_subtitle": False,
        }
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def process_video_with_retry(video: dict) -> tuple[str, str, dict]:
    last_err = ""
    max_r = LIMITS["max_retries_same_video"]
    for attempt in range(1, max_r + 1):
        try:
            t0 = time.time()
            LOG.info(f"DL  start [{attempt}/{max_r}] "
                     f"video={video['video_id']} url={video['video_url']}")
            status, msg, extras = process_video_once(video)
            LOG.info(f"DL  done  video={video['video_id']} status={status} "
                     f"took={time.time()-t0:.1f}s")
            return status, msg, extras
        except Exception as e:
            last_err = str(e)
            fatal = any(m in last_err for m in FATAL_DOWNLOAD_ERRORS)
            LOG.warning(f"DL  fail  video={video['video_id']} attempt={attempt}"
                        f"{' fatal' if fatal else ''} "
                        f"err={last_err[:200]}")
            if fatal:
                # No point retrying with the same player_client list — let
                # another machine (different IP) take a swing at it.
                return "error", last_err, {}
            time.sleep(min(2 ** attempt, 30) + random.uniform(0, 2))
    return "error", last_err, {}


# ---------- worker model ----------
class Pool:
    def __init__(self, workers: int, batch_size: int):
        self.workers = workers
        self.batch_size = batch_size
        self.queue: queue.Queue[dict] = queue.Queue(maxsize=workers * 2)
        self.stop_event = threading.Event()
        self.shutdown_requested = threading.Event()
        self.completed_lock = threading.Lock()
        self.completed_count = 0
        self.failed_in_a_row = 0

    def worker(self, idx: int):
        threading.current_thread().name = f"w{idx}"
        while not self.stop_event.is_set():
            try:
                video = self.queue.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                status, msg, extras = process_video_with_retry(video)
            except Exception as e:
                status, msg, extras = "error", str(e), {}

            try:
                resp = notify_completion(
                    video["video_id"], status, msg,
                    has_subtitle=extras.get("has_subtitle"),
                    s3_audio=extras.get("s3_audio"),
                    s3_subtitle=extras.get("s3_subtitle"),
                )
            except Exception as e:
                LOG.warning(f"notify fail: {e}")
                resp = {}

            with self.completed_lock:
                if status in ("success", "no_subtitle", "exists"):
                    self.completed_count += 1
                    self.failed_in_a_row = 0
                else:
                    self.failed_in_a_row += 1

            if resp.get("shutdown"):
                LOG.error(f"server requested shutdown: {resp.get('reason')}")
                self.shutdown_requested.set()
                self.stop_event.set()

            self.queue.task_done()

    def feeder(self):
        threading.current_thread().name = "feeder"
        idle_loops = 0
        while not self.stop_event.is_set():
            need = self.queue.maxsize - self.queue.qsize()
            if need <= 0:
                time.sleep(0.5)
                continue
            try:
                videos = get_batch(min(self.batch_size, need))
            except Exception as e:
                LOG.warning(f"get_batch failed: {e}")
                time.sleep(5)
                continue
            if not videos:
                idle_loops += 1
                if idle_loops > 10:  # ~10 * 5s = ~50s idle
                    LOG.info("no more videos available — exiting")
                    self.stop_event.set()
                    return
                time.sleep(5)
                continue
            idle_loops = 0
            for v in videos:
                LOG.info(f"GOT video={v['video_id']} url={v['video_url']} "
                         f"type={v['type']}")
                self.queue.put(v)


def run():
    if not API_BASE_URL:
        LOG.error("API_BASE_URL is not set")
        sys.exit(1)

    for i in range(20):
        try:
            register()
            fetch_config()
            break
        except Exception as e:
            LOG.warning(f"register failed (try {i+1}): {e}")
            time.sleep(min(5 + i * 3, 60))
    else:
        LOG.error("Could not register; exiting")
        sys.exit(2)

    fetch_cookies()

    pool = Pool(workers=LIMITS["client_workers"],
                batch_size=LIMITS["client_batch_size"])

    stop_hb = threading.Event()
    threading.Thread(target=heartbeat_loop, args=(stop_hb,),
                     name="hb", daemon=True).start()

    feeder = threading.Thread(target=pool.feeder, name="feeder", daemon=True)
    feeder.start()

    threads = []
    for i in range(pool.workers):
        t = threading.Thread(target=pool.worker, args=(i,), name=f"w{i}", daemon=True)
        t.start()
        threads.append(t)

    LOG.info(f"started {pool.workers} workers")

    try:
        while not pool.stop_event.is_set():
            time.sleep(2)
    except KeyboardInterrupt:
        pass
    finally:
        pool.stop_event.set()
        if pool.shutdown_requested.is_set():
            request_shutdown("server requested shutdown")
        stop_hb.set()
        for t in threads:
            t.join(timeout=10)
        LOG.info(f"client exiting (completed={pool.completed_count})")


if __name__ == "__main__":
    run()
