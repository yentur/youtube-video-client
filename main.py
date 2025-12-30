import yt_dlp
import os
import csv
import random, time
import boto3
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import tempfile
import threading
from collections import defaultdict
import glob

LOG_FILE = "download_log.csv"

# API Configuration
API_BASE_URL = os.getenv("API_BASE_URL")

# S3 Configuration - Will be fetched from API
S3_BUCKET = None
S3_FOLDER = None
AWS_ACCESS_KEY_ID = None
AWS_SECRET_ACCESS_KEY = None
AWS_REGION = None

# Global progress tracking
class ProgressTracker:
    def __init__(self, total_videos):
        self.total_videos = total_videos
        self.completed = 0
        self.success_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.lock = threading.Lock()
        self.start_time = datetime.now()
    
    def update(self, status):
        with self.lock:
            self.completed += 1
            if status == "success":
                self.success_count += 1
            elif status == "error":
                self.error_count += 1
            elif status == "skipped":
                self.skipped_count += 1
    
    def get_progress_string(self):
        with self.lock:
            elapsed = datetime.now() - self.start_time
            remaining = self.total_videos - self.completed
            
            progress_bar_length = 30
            completed_length = int(progress_bar_length * self.completed / self.total_videos)
            bar = "█" * completed_length + "░" * (progress_bar_length - completed_length)
            
            percentage = (self.completed / self.total_videos) * 100
            
            return (
                f"[{bar}] {self.completed}/{self.total_videos} ({percentage:.1f}%) | "
                f"✅ {self.success_count} | ⏭ {self.skipped_count} | ❌ {self.error_count} | "
                f"⏱ {str(elapsed).split('.')[0]} | 🔄 {remaining} kaldı"
            )

progress_tracker = None

def print_header():
    """Başlık yazdır"""
    print("=" * 80)
    print("🎵 YOUTUBE VIDEO DOWNLOADER & S3 UPLOADER (WAV 16kHz + AUTO SUBTITLES ONLY)")
    print("=" * 80)

def print_status(message, status_type="info"):
    """Renkli status mesajları"""
    status_icons = {
        "info": "ℹ️",
        "success": "✅", 
        "error": "❌",
        "warning": "⚠️",
        "progress": "🔄",
        "skip": "⏭️"
    }
    
    icon = status_icons.get(status_type, "•")
    timestamp = datetime.now().strftime("%H:%M:%S")
    
    if progress_tracker:
        progress = progress_tracker.get_progress_string()
        print(f"\n{progress}")
    
    print(f"[{timestamp}] {icon} {message}")

def progress_hook(d):
    """yt-dlp indirme ilerleme callback"""
    if d['status'] == 'downloading':
        percent = d.get('_percent_str', '').strip()
        speed = d.get('_speed_str', 'N/A')
        print(f"  ⏳ İndiriliyor: {percent} | Hız: {speed}", end="\r")
    elif d['status'] == 'finished':
        print(f"  ✅ İndirme tamamlandı" + " " * 20)

def log_to_csv(user, video_url, status, message=""):
    """Log dosyasına yazar"""
    file_exists = os.path.isfile(LOG_FILE)
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "user", "video_url", "status", "message"])
        writer.writerow([datetime.now().isoformat(), user, video_url, status, message])

def check_s3_file_exists(s3_client, bucket, key):
    """S3'te dosya var mı kontrol et"""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False

def upload_file_to_s3(file_path, s3_key, file_type="WAV"):
    """Dosyayı S3'e yükler"""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        # Dosya boyutunu al
        file_size = os.path.getsize(file_path)
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"  ☁️ {file_type} S3'e yükleniyor... ({file_size_mb:.2f} MB)")

        with open(file_path, 'rb') as f:
            s3_client.upload_fileobj(f, S3_BUCKET, s3_key)

        print(f"  ✅ {file_type} S3'e yüklendi")
        return f"s3://{S3_BUCKET}/{s3_key}"
        
    except Exception as e:
        print(f"  ❌ S3 yükleme hatası ({file_type}): {e}")
        return None

def check_subtitle_availability(video_url):
    """
    SADECE otomatik altyazı durumunu kontrol et (manuel altyazılar görmezden gelinir)
    Returns: (has_auto, languages)
    """
    try:
        ydl_opts = {'quiet': True, 'no_warnings': True}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
            
            # SADECE otomatik altyazıları kontrol et
            auto_subs = info.get('automatic_captions', {})
            
            has_auto = len(auto_subs) > 0
            auto_langs = list(auto_subs.keys()) if has_auto else []
            
            return has_auto, auto_langs
    except Exception as e:
        print(f"  ⚠️ Altyazı kontrolü hatası: {e}")
        return False, []

def download_and_upload_video(video_url, temp_dir, video_index, total_videos):
    """Video indir (WAV 16kHz + SADECE otomatik altyazılar) ve S3'e yükle"""
    time.sleep(random.uniform(1, 3))
    
    try:
        # Video bilgisini al
        print_status(f"[{video_index}/{total_videos}] Video bilgisi alınıyor...", "progress")
        
        ydl_opts_info = {'quiet': True, 'no_warnings': True}
        with yt_dlp.YoutubeDL(ydl_opts_info) as ydl:
            info = ydl.extract_info(video_url, download=False)
            video_title = info.get('title', 'Unknown')
            channel_name = info.get('uploader', 'Unknown')
            duration = info.get('duration', 0)
        
        # Video süresi
        duration_str = f"{duration//60}:{duration%60:02d}" if duration else "N/A"
        
        print_status(f"[{video_index}/{total_videos}] 📺 {video_title[:50]}... ({duration_str}) - {channel_name}", "info")
        
        # SADECE otomatik altyazı kontrolü
        print(f"  🔍 Otomatik altyazı durumu kontrol ediliyor...")
        has_auto, auto_langs = check_subtitle_availability(video_url)
        
        if not has_auto:
            print(f"  ❌ Otomatik altyazı bulunamadı - Video atlanıyor")
            print_status(f"[{video_index}/{total_videos}] ⏭️ Otomatik altyazı yok (atlandi): {video_title[:40]}...", "skip")
            log_to_csv(channel_name, video_url, "skipped", "no_auto_subtitle_available")
            progress_tracker.update("skipped")
            return (video_url, True, "no_auto_subtitle", None)
        
        print(f"  🤖 Otomatik altyazı mevcut: {auto_langs}")
        
        # Güvenli dosya adları
        safe_title = "".join(c if c.isalnum() or c in " -_()" else "_" for c in video_title)[:100]
        safe_channel = "".join(c if c.isalnum() or c in " -_()" else "_" for c in channel_name)[:50]
        
        # S3 yolları
        s3_wav_key = f"{S3_FOLDER}/{safe_channel}/{safe_title}.wav"
        s3_subtitle_key = f"{S3_FOLDER}/{safe_channel}/{safe_title}.srt"
        
        # S3'te var mı kontrol et
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
        wav_exists = check_s3_file_exists(s3_client, S3_BUCKET, s3_wav_key)
        subtitle_exists = check_s3_file_exists(s3_client, S3_BUCKET, s3_subtitle_key)
        
        if wav_exists and subtitle_exists:
            print_status(f"[{video_index}/{total_videos}] ⏭️ Zaten mevcut (WAV+SRT): {video_title[:40]}...", "skip")
            log_to_csv(safe_channel, video_url, "skipped", "exists_in_s3")
            progress_tracker.update("skipped")
            return (video_url, True, "exists", None)
        
        # Geçici dosya yolları
        output_template = os.path.join(temp_dir, f"{safe_title}.%(ext)s")
        wav_file_path = os.path.join(temp_dir, f"{safe_title}.wav")
        
        # Dil seçimi (tr öncelikli, sonra en, sonra diğerleri)
        preferred_lang = None
        if 'tr' in auto_langs:
            preferred_lang = 'tr'
        elif 'en' in auto_langs:
            preferred_lang = 'tr'
        else:
            preferred_lang = auto_langs[0] if auto_langs else None
        
        # Altyazı indirme ayarları - SADECE otomatik altyazı
        ydl_opts_subtitle = {
            'skip_download': True,
            'writesubtitles': False,  # Manuel altyazıları ALMA
            'writeautomaticsub': True,  # SADECE otomatik altyazıları al
            'subtitleslangs': [preferred_lang] if preferred_lang else ['tr'],
            'subtitlesformat': 'srt',
            'outtmpl': output_template,
            'quiet': True,
            'noplaylist': True,
        }
        
        # WAV indirme ayarları - 16kHz sample rate ile
        ydl_opts_audio = {
            'format': 'bestaudio/best',
            'outtmpl': output_template,
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'wav',
                'preferredquality': '192',
            }],
            'postprocessor_args': [
                '-ar', '16000',  # Sample rate 16kHz
            ],
            'quiet': True,
            'noplaylist': True,
            'progress_hooks': [progress_hook],
        }
        
        # 1. Önce otomatik altyazıyı indir
        if not subtitle_exists:
            print(f"  📝 Otomatik altyazı indiriliyor ({preferred_lang})...")
            with yt_dlp.YoutubeDL(ydl_opts_subtitle) as ydl:
                ydl.download([video_url])
            
            # Altyazı dosyası indirildi mi kontrol et
            subtitle_pattern = os.path.join(temp_dir, f"{safe_title}*.srt")
            subtitle_files = glob.glob(subtitle_pattern)
            
            if not subtitle_files:
                print(f"  ❌ Otomatik altyazı indirilemedi - Video atlanıyor")
                print_status(f"[{video_index}/{total_videos}] ⏭️ Otomatik altyazı indirilemedi: {video_title[:40]}...", "skip")
                log_to_csv(safe_channel, video_url, "skipped", "auto_subtitle_download_failed")
                progress_tracker.update("skipped")
                return (video_url, True, "auto_subtitle_failed", None)
        
        # 2. Altyazı başarılıysa, WAV'ı indir (16kHz)
        if not wav_exists:
            print(f"  🎵 WAV indiriliyor (16kHz): {video_title[:40]}...")
            with yt_dlp.YoutubeDL(ydl_opts_audio) as ydl:
                ydl.download([video_url])
        
        # S3'e yükleme
        upload_results = {}
        
        # Altyazı yükle (önce bu)
        if not subtitle_exists:
            subtitle_pattern = os.path.join(temp_dir, f"{safe_title}*.srt")
            subtitle_files = glob.glob(subtitle_pattern)
            
            if subtitle_files:
                subtitle_file = subtitle_files[0]
                s3_subtitle_url = upload_file_to_s3(subtitle_file, s3_subtitle_key, "SRT (AUTO)")
                upload_results['subtitle'] = s3_subtitle_url
                upload_results['subtitle_type'] = 'auto'
                upload_results['subtitle_lang'] = preferred_lang
                
                # Tüm altyazı dosyalarını temizle
                for sf in subtitle_files:
                    try:
                        os.remove(sf)
                    except:
                        pass
            else:
                print(f"  ❌ Altyazı dosyası bulunamadı")
                upload_results['subtitle'] = None
        else:
            upload_results['subtitle'] = f"s3://{S3_BUCKET}/{s3_subtitle_key}"
            print(f"  ⏭️ Altyazı zaten mevcut")
        
        # WAV yükle
        if os.path.exists(wav_file_path) and not wav_exists:
            s3_wav_url = upload_file_to_s3(wav_file_path, s3_wav_key, "WAV (16kHz)")
            upload_results['wav'] = s3_wav_url
            upload_results['sample_rate'] = '16000'
            os.remove(wav_file_path)
        elif wav_exists:
            upload_results['wav'] = f"s3://{S3_BUCKET}/{s3_wav_key}"
            upload_results['sample_rate'] = '16000'
            print(f"  ⏭️ WAV zaten mevcut")
        
        # Sonuç kontrolü
        if upload_results.get('wav') and upload_results.get('subtitle'):
            sub_info = f"auto - {upload_results.get('subtitle_lang', 'unknown')}"
            print_status(f"[{video_index}/{total_videos}] ✅ Başarılı (WAV 16kHz + AUTO SRT [{sub_info}]): {video_title[:40]}...", "success")
            log_to_csv(safe_channel, video_url, "success", json.dumps(upload_results))
            progress_tracker.update("success")
            return (video_url, True, None, upload_results)
        else:
            print_status(f"[{video_index}/{total_videos}] ❌ Yükleme hatası: {video_title[:40]}...", "error")
            log_to_csv(safe_channel, video_url, "error", "upload_failed")
            progress_tracker.update("error")
            return (video_url, False, "Upload failed", None)
            
    except Exception as e:
        print_status(f"[{video_index}/{total_videos}] ❌ Hata: {str(e)[:60]}...", "error")
        log_to_csv("unknown", video_url, "error", str(e))
        progress_tracker.update("error")
        return (video_url, False, str(e), None)

def get_config_from_api():
    """API'den AWS konfigürasyonunu al"""
    global S3_BUCKET, S3_FOLDER, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
    
    try:
        print_status("API'den konfigürasyon alınıyor...", "progress")
        
        response = requests.get(f"{API_BASE_URL}/get-config", timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get("status") != "success":
            print_status(f"❌ Config alınamadı: {data.get('message')}", "error")
            return False
        
        config = data.get("config", {})
        aws_config = config.get("aws", {})
        
        # Global variables'ı güncelle
        S3_BUCKET = aws_config.get("s3_bucket")
        S3_FOLDER = aws_config.get("s3_folder", "youtube_videos")
        AWS_ACCESS_KEY_ID = aws_config.get("access_key_id")
        AWS_SECRET_ACCESS_KEY = aws_config.get("secret_access_key")
        AWS_REGION = aws_config.get("region", "us-east-1")
        
        print_status(f"✅ AWS Config alındı: Bucket={S3_BUCKET}, Region={AWS_REGION}", "success")
        return True
        
    except requests.exceptions.ConnectionError as e:
        print_status(f"API'ye bağlanılamıyor: {API_BASE_URL}", "error")
        return False
    except Exception as e:
        print_status(f"Config alma hatası: {e}", "error")
        return False

def get_next_video_from_api():
    """API'den tek video al"""
    try:
        print_status("API'den video alınıyor...", "progress")
        print_status(f"API URL: {API_BASE_URL}/get-next-video", "info")
        
        response = requests.get(f"{API_BASE_URL}/get-next-video", timeout=30)
        response.raise_for_status()
        
        data = response.json()
        print_status(f"API Response: {json.dumps(data, indent=2, ensure_ascii=False)[:200]}...", "info")
        
        status = data.get("status")
        
        if status == "success":
            video_url = data.get("video_url")
            video_id = data.get("video_id")
            progress_info = data.get("progress", {})
            
            print_status(f"Video alındı: {video_url}", "success")
            print_status(f"İlerleme: {progress_info.get('sent')}/{progress_info.get('total')} ({progress_info.get('remaining')} kaldı)", "info")
            
            return video_url, video_id
        elif status == "no_more_videos":
            message = data.get("message", "Tüm videolar işlendi")
            print_status(f"📭 {message}", "warning")
            print_status(f"   Toplam video: {data.get('total_videos', 0)}", "info")
            print_status(f"   İşlenen: {data.get('sent_videos', 0)}", "info")
            return None, None
        else:
            print_status(f"API'den beklenmeyen status: {status}", "error")
            print_status(f"Mesaj: {data.get('message', 'N/A')}", "error")
            return None, None
    except requests.exceptions.ConnectionError as e:
        print_status(f"API'ye bağlanılamıyor: {API_BASE_URL}", "error")
        print_status(f"Lütfen API sunucusunun çalıştığından emin olun", "error")
        print_status(f"Hata: {e}", "error")
        return None, None
    except requests.exceptions.Timeout:
        print_status(f"API zaman aşımı (30s)", "error")
        return None, None
    except Exception as e:
        print_status(f"API hatası: {e}", "error")
        return None, None

def notify_api_completion(video_id, status, message="", video_count=1):
    """API'ye durum bildir"""
    if not video_id:
        return
        
    try:
        payload = {
            "list_id": video_id,  # API'de list_id bekleniyor (geriye uyumluluk)
            "status": status,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "video_count": video_count
        }
        response = requests.post(f"{API_BASE_URL}/notify-completion", json=payload, timeout=10)
        response.raise_for_status()
        print_status("API'ye durum bildirildi", "success")
    except Exception as e:
        print_status(f"API bildirim hatası: {e}", "warning")

def process_single_video_from_api():
    """API'den tek video al ve işle"""
    global progress_tracker
    
    print_header()
    
    # API'den tek video al
    video_url, video_id = get_next_video_from_api()
    
    if not video_url:
        print_status("Video alınamadı veya tüm videolar işlendi - çıkılıyor", "warning")
        return False  # Daha fazla video yok
    
    # Progress tracker başlat (tek video için)
    progress_tracker = ProgressTracker(1)
    
    print_status(f"Video işlenecek: {video_url}", "info")
    print_status("🤖 SADECE OTOMATİK ALTYAZI MODU AKTİF", "warning")
    print_status("🎵 SES: 16kHz Sample Rate", "info")
    print_status("  ⚠️ Manuel (kanal) altyazıları GÖRMEZDEN GELİNİR", "warning")
    print_status("  ✅ SADECE YouTube otomatik çevirisi kullanılır", "info")
    print_status("  ❌ Otomatik altyazı yoksa video atlanır", "info")
    print_status("İşlem başlatılıyor...", "progress")
    print("-" * 80)

    # Geçici klasör
    temp_dir = tempfile.mkdtemp(prefix="yt_")
    print_status(f"Geçici klasör: {temp_dir}", "info")

    try:
        # Tek videoyu işle
        video_url_result, success, error, s3_data = download_and_upload_video(video_url, temp_dir, 1, 1)
        
        # Temizlik
        try:
            import shutil
            shutil.rmtree(temp_dir)
            print_status("Geçici dosyalar temizlendi", "info")
        except Exception as e:
            print_status(f"Temizlik hatası: {e}", "warning")

        # Özet
        print("\n" + "=" * 80)
        print("🎉 VIDEO İŞLEMİ TAMAMLANDI!")
        print("=" * 80)
        
        elapsed_total = datetime.now() - progress_tracker.start_time
        print(f"⏱️  Süre: {str(elapsed_total).split('.')[0]}")
        
        if success and not error:
            print(f"✅ Başarılı: Video işlendi")
            message = f"Processed successfully (WAV 16kHz+AUTO SRT)"
            final_status = "completed"
        elif error == "no_auto_subtitle":
            print(f"⏭️  Atlanan: Otomatik altyazı yok")
            message = f"Skipped: No auto subtitle"
            final_status = "skipped"
        elif error == "exists":
            print(f"⏭️  Atlanan: Zaten mevcut")
            message = f"Skipped: Already exists"
            final_status = "skipped"
        else:
            print(f"❌ Hatalı: {error}")
            message = f"Error: {error}"
            final_status = "error"

        # API'ye bildir
        notify_api_completion(video_id, final_status, message)
        
        print("=" * 80)
        
        return True  # Başarıyla işlendi (daha fazla video olabilir)
        
    except Exception as e:
        print_status(f"Video işleme hatası: {e}", "error")
        notify_api_completion(video_id, "error", str(e))
        return True  # Hataya rağmen devam et


def continuous_video_processing():
    """Sürekli olarak API'den video al ve işle"""
    print("="*80)
    print("🔄 SÜREKLİ VİDEO İŞLEME MODU")
    print("="*80)
    print("API'den sürekli video alınacak ve işlenecek")
    print("Tüm videolar bitene kadar devam edilecek")
    print("="*80 + "\n")
    
    # Önce config'i al
    print_status("Başlangıç: AWS konfigürasyonu API'den alınıyor...", "info")
    if not get_config_from_api():
        print_status("❌ AWS konfigürasyonu alınamadı - çıkılıyor", "error")
        print_status("   API sunucusunun çalıştığından emin olun", "error")
        print_status(f"   API URL: {API_BASE_URL}", "error")
        return
    
    # Config kontrolü
    if not S3_BUCKET or not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        print_status("❌ AWS credentials eksik - çıkılıyor", "error")
        return
    
    print_status("✅ Konfigürasyon başarıyla alındı, işleme başlanıyor...", "success")
    print()
    
    total_processed = 0
    total_success = 0
    total_skipped = 0
    total_errors = 0
    
    start_time = datetime.now()
    
    while True:
        has_more = process_single_video_from_api()
        
        if not has_more:
            print("\n" + "="*80)
            print("🏁 TÜM VİDEOLAR İŞLENDİ!")
            print("="*80)
            break
        
        total_processed += 1
        
        # Kısa bir bekleme (rate limiting için)
        print_status("Sonraki video için bekleniyor (3s)...", "info")
        time.sleep(3)
    
    # Final özet
    elapsed_total = datetime.now() - start_time
    print(f"\n📊 GENEL ÖZET:")
    print(f"   Toplam işlenen video: {total_processed}")
    print(f"   Toplam süre: {str(elapsed_total).split('.')[0]}")
    print("="*80)

if __name__ == "__main__":
    continuous_video_processing()
