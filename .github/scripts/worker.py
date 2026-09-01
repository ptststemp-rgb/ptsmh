#!/usr/bin/env python3
"""
PTSLibrary - Automated GitHub Actions Transcode & Hugging Face Relay Worker
Downloads source media via aria2c multi-connection engine (fallback to requests),
transcodes to 480p H.265 with fast preset and pipe-deadlock protection,
and uploads to Hugging Face dataset, streaming real-time progress back to Raspberry Pi.
"""

import sys
import os
import re
import time
import json
import shutil
import argparse
import subprocess
import requests

try:
    from huggingface_hub import HfApi
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "huggingface_hub", "requests", "tqdm"])
    from huggingface_hub import HfApi


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 PTSWorker/2.0"


def send_callback(callback_url, callback_secret, payload):
    """Sends a real-time progress update back to the Raspberry Pi hub."""
    if not callback_url:
        print("[Status]", payload, flush=True)
        return

    try:
        headers = {
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
            "x-worker-secret": callback_secret or ""
        }
        resp = requests.post(callback_url, json=payload, headers=headers, timeout=12)
        if resp.status_code != 200:
            print(f"[Callback Notice] HTTP {resp.status_code}: {resp.text}", flush=True)
    except Exception as e:
        print(f"[Callback Warning] Could not notify hub ({e})", flush=True)


def format_bytes(bytes_val):
    if not bytes_val or bytes_val <= 0:
        return "0 B"
    if bytes_val < 1024:
        return f"{bytes_val} B"
    elif bytes_val < 1024 * 1024:
        return f"{bytes_val / 1024:.1f} KB"
    elif bytes_val < 1024 * 1024 * 1024:
        return f"{bytes_val / (1024 * 1024):.1f} MB"
    else:
        return f"{bytes_val / (1024 * 1024 * 1024):.2f} GB"


def format_speed(bps):
    return f"{format_bytes(bps)}/s"


def format_eta(seconds):
    if seconds <= 0 or seconds > 86400:
        return "00s"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h:02d}h {m:02d}m"
    return f"{m:02d}m {s:02d}s"


def download_with_aria2(url, output_path, task_id, callback_url, callback_secret):
    """
    Downloads the remote source using aria2c with 16 parallel connections.
    Saturates available network bandwidth (up to 100+ MB/s) on cloud runners.
    """
    aria2_path = shutil.which("aria2c")
    if not aria2_path:
        raise RuntimeError("aria2c binary not found on runner")

    out_dir, out_file = os.path.split(output_path)
    if not out_dir:
        out_dir = "."

    print(f"[*] Starting high-speed aria2c download (16 streams) for: {url}", flush=True)
    send_callback(callback_url, callback_secret, {
        "taskId": task_id,
        "stage": "gha_downloading",
        "progress": 0,
        "speed": "Connecting...",
        "eta": "Calculating...",
        "message": "Initializing 16x parallel aria2c download engine..."
    })

    cmd = [
        aria2_path,
        "-x", "16",
        "-s", "16",
        "-j", "16",
        "-k", "1M",
        "--file-allocation=none",
        "--summary-interval=1",
        "--console-log-level=warn",
        "--user-agent=" + USER_AGENT,
        "--allow-overwrite=true",
        "--auto-file-renaming=false",
        "--timeout=30",
        "--max-tries=5",
        "--retry-wait=3",
        "-d", out_dir,
        "-o", out_file,
        url
    ]

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    pattern = re.compile(r'\[#[0-9a-fA-F]+\s+([0-9.]+[A-Za-z]+)\/([0-9.]+[A-Za-z]+)(?:\((\d+)%\))?.*?\s+DL:([0-9.]+[A-Za-z]+)(?:.*?\s+ETA:([0-9a-z]+))?')

    last_callback_time = 0

    for line in process.stdout:
        line = line.strip()
        m = pattern.search(line)
        if m:
            downloaded_str = m.group(1)
            total_str = m.group(2)
            pct_str = m.group(3) or "50"
            dl_speed_str = m.group(4) + "/s"
            eta_str = m.group(5) or "--"

            now = time.time()
            if now - last_callback_time >= 1.5:
                send_callback(callback_url, callback_secret, {
                    "taskId": task_id,
                    "stage": "gha_downloading",
                    "progress": int(pct_str),
                    "speed": dl_speed_str,
                    "eta": eta_str,
                    "transferred": downloaded_str,
                    "total": total_str,
                    "message": f"Downloading source (16x streams): {downloaded_str} / {total_str}"
                })
                last_callback_time = now

    rc = process.wait()
    if rc != 0:
        raise RuntimeError(f"aria2c download failed with exit code {rc}")

    if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
        raise RuntimeError("aria2c finished but output file is missing or 0 bytes")

    print(f"[✓] aria2c high-speed download succeeded: {format_bytes(os.path.getsize(output_path))}", flush=True)
    return True


def download_with_requests_fallback(url, output_path, task_id, callback_url, callback_secret):
    """Fallback download engine using requests with streaming and resume support."""
    print(f"[*] Starting requests fallback download from: {url}", flush=True)
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "*/*"
    }

    max_retries = 5
    retry_delay = 3
    downloaded = 0
    total_size = 0

    try:
        head_resp = requests.head(url, headers=headers, timeout=15, allow_redirects=True)
        if head_resp.status_code < 400:
            total_size = int(head_resp.headers.get("content-length", 0))
    except Exception:
        pass

    start_time = time.time()
    last_callback_time = 0

    for attempt in range(1, max_retries + 1):
        try:
            req_headers = dict(headers)
            mode = "wb"
            if downloaded > 0:
                req_headers["Range"] = f"bytes={downloaded}-"
                mode = "ab"
                print(f"[*] Resuming download from byte {downloaded} (attempt {attempt}/{max_retries})...", flush=True)
            else:
                print(f"[*] Connecting to download source (attempt {attempt}/{max_retries})...", flush=True)

            resp = requests.get(url, headers=req_headers, stream=True, timeout=45)
            if resp.status_code not in (200, 206):
                resp.raise_for_status()

            if not total_size:
                if resp.status_code == 206 and "content-range" in resp.headers:
                    cr = resp.headers["content-range"]
                    if "/" in cr:
                        try:
                            total_size = int(cr.split("/")[1])
                        except Exception:
                            pass
                if not total_size:
                    total_size = int(resp.headers.get("content-length", 0)) + (downloaded if resp.status_code == 206 else 0)

            with open(output_path, mode) as out_file:
                # 4MB chunk size for higher throughput in Python
                for chunk in resp.iter_content(chunk_size=4 * 1024 * 1024):
                    if not chunk:
                        continue
                    out_file.write(chunk)
                    downloaded += len(chunk)

                    now = time.time()
                    if now - last_callback_time >= 1.5 or (total_size and downloaded >= total_size):
                        elapsed = now - start_time
                        speed = downloaded / elapsed if elapsed > 0 else 0
                        pct = min(99, max(0, round((downloaded / total_size) * 100))) if total_size > 0 else 50
                        rem_bytes = max(0, total_size - downloaded)
                        eta = rem_bytes / speed if speed > 0 else 0

                        send_callback(callback_url, callback_secret, {
                            "taskId": task_id,
                            "stage": "gha_downloading",
                            "progress": pct,
                            "speed": format_speed(speed),
                            "eta": format_eta(eta),
                            "transferred": format_bytes(downloaded),
                            "total": format_bytes(total_size) if total_size > 0 else "Unknown",
                            "message": f"Downloading source: {format_bytes(downloaded)} / {format_bytes(total_size) if total_size > 0 else '...'}"
                        })
                        last_callback_time = now

            if total_size > 0 and downloaded < total_size:
                print(f"[!] Partial download ({downloaded}/{total_size} bytes). Retrying...", flush=True)
                time.sleep(retry_delay)
                continue

            break

        except Exception as e:
            print(f"[!] Download attempt {attempt} error: {e}", flush=True)
            if attempt == max_retries:
                raise e
            time.sleep(retry_delay)


def download_source(url, output_path, task_id, callback_url, callback_secret):
    """Tries high-speed aria2c first, falling back to requests if needed."""
    try:
        download_with_aria2(url, output_path, task_id, callback_url, callback_secret)
    except Exception as aria_err:
        print(f"[!] aria2c failed or unavailable ({aria_err}). Switching to requests fallback...", flush=True)
        download_with_requests_fallback(url, output_path, task_id, callback_url, callback_secret)

    send_callback(callback_url, callback_secret, {
        "taskId": task_id,
        "stage": "gha_downloading",
        "progress": 100,
        "speed": "Done",
        "eta": "00s",
        "message": "Source download completed successfully."
    })
    print("[✓] Source download completed successfully.", flush=True)


def get_video_duration(file_path):
    """Extracts duration in seconds using ffprobe."""
    try:
        cmd = [
            "ffprobe", "-v", "error", "-show_entries",
            "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", file_path
        ]
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8").strip()
        return float(out)
    except Exception as e:
        print(f"[ffprobe note] {e}", flush=True)
        return 0


def probe_media_streams(file_path):
    """Inspects all streams in media file using ffprobe to detect text subtitles and audio."""
    try:
        cmd = [
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            file_path
        ]
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8")
        data = json.loads(out)
        return data.get("streams", [])
    except Exception as e:
        print(f"[ffprobe probe note] {e}", flush=True)
        return []


def transcode_to_480p_h265(input_path, output_path, work_dir, task_id, callback_url, callback_secret):
    """
    Transcodes video to 480p H.265 (libx265) and compresses all audio tracks to 128kbps AAC.
    Preserves all audio languages, streams, and text subtitles.
    Uses pipe deadlock protection and fast preset to complete 4x-6x faster.
    """
    print("[*] Starting FFmpeg 480p H.265 transcode...", flush=True)
    duration = get_video_duration(input_path)
    print(f"[*] Video duration: {duration:.1f}s", flush=True)

    send_callback(callback_url, callback_secret, {
        "taskId": task_id,
        "stage": "gha_compressing",
        "progress": 0,
        "speed": "0x",
        "eta": "Starting...",
        "message": "Starting video encoder (preset: fast)..."
    })

    is_mp4 = output_path.lower().endswith(('.mp4', '.m4v', '.mov'))
    streams = probe_media_streams(input_path)

    cmd = [
        "ffmpeg", "-y",
        "-threads", "0",
        "-i", input_path,
        "-vf", "scale=-2:480",
        "-c:v", "libx265",
        "-crf", "26",
        "-preset", "fast",
        "-pix_fmt", "yuv420p",
        "-tag:v", "hvc1",
        "-g", "48",
        "-keyint_min", "48",
        "-sc_threshold", "0",
        "-map", "0:v:0",
        "-map", "0:a?",
        "-c:a", "aac",
        "-b:a", "128k",
        "-ac", "2",
        "-ar", "48000"
    ]

    if is_mp4:
        TEXT_SUBTITLE_CODECS = {
            'subrip', 'srt', 'ass', 'ssa', 'mov_text', 'webvtt', 'vtt', 'text', 'ttml'
        }
        text_sub_indexes = []
        for s in streams:
            if s.get("codec_type") == "subtitle":
                codec = (s.get("codec_name") or "").lower()
                idx = s.get("index")
                if codec in TEXT_SUBTITLE_CODECS:
                    text_sub_indexes.append(idx)
                else:
                    print(f"[*] Skipping bitmap/incompatible subtitle stream #{idx} ({codec}) for MP4 container.", flush=True)

        if text_sub_indexes:
            for idx in text_sub_indexes:
                cmd.extend(["-map", f"0:{idx}"])
            cmd.extend(["-c:s", "mov_text"])

        cmd.extend(["-movflags", "+faststart"])
    else:
        cmd.extend([
            "-map", "0:s?",
            "-c:s", "copy",
            "-reserve_index_space", "100k",
            "-cluster_size_limit", "2M",
            "-cluster_time_limit", "2000"
        ])

    cmd.extend([
        "-progress", "pipe:1",
        "-nostats",
        output_path
    ])

    # DEADLOCK FIX: Write stderr directly to a log file on disk so the OS 64KB pipe buffer never deadlocks FFmpeg!
    ffmpeg_log_path = os.path.join(work_dir, "ffmpeg.log")
    with open(ffmpeg_log_path, "w", encoding="utf-8") as ffmpeg_log:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=ffmpeg_log,
            universal_newlines=True
        )

        start_time = time.time()
        last_callback_time = 0

        while True:
            line = process.stdout.readline()
            if not line and process.poll() is not None:
                break

            line = line.strip()
            if line.startswith("out_time_us="):
                try:
                    out_time_us = int(line.split("=")[1])
                    curr_seconds = out_time_us / 1000000.0

                    now = time.time()
                    if now - last_callback_time >= 1.5:
                        pct = 0
                        if duration > 0:
                            pct = min(99, max(0, round((curr_seconds / duration) * 100)))

                        elapsed = now - start_time
                        fps_speed = curr_seconds / elapsed if elapsed > 0 else 0
                        eta_sec = (duration - curr_seconds) / fps_speed if (duration > 0 and fps_speed > 0) else 0

                        send_callback(callback_url, callback_secret, {
                            "taskId": task_id,
                            "stage": "gha_compressing",
                            "progress": pct,
                            "speed": f"{fps_speed:.2f}x",
                            "eta": format_eta(eta_sec),
                            "message": f"Encoding video ({pct}% done, speed: {fps_speed:.2f}x, ETA: {format_eta(eta_sec)})"
                        })
                        last_callback_time = now
                except Exception:
                    pass

        rc = process.wait()
        if rc != 0:
            err_snippet = ""
            if os.path.exists(ffmpeg_log_path):
                with open(ffmpeg_log_path, "r", encoding="utf-8", errors="ignore") as f:
                    err_lines = f.readlines()
                    err_snippet = "".join(err_lines[-30:])
            raise RuntimeError(f"FFmpeg encoding failed with exit code {rc}: {err_snippet}")

    send_callback(callback_url, callback_secret, {
        "taskId": task_id,
        "stage": "gha_compressing",
        "progress": 100,
        "speed": "Done",
        "eta": "00s",
        "message": "FFmpeg transcode completed successfully."
    })
    print("[✓] FFmpeg transcode completed.", flush=True)


def upload_to_huggingface(file_path, hf_repo, hf_token, hf_path, task_id, callback_url, callback_secret):
    """Uploads the transcoded file to Hugging Face dataset."""
    print(f"[*] Uploading to Hugging Face: {hf_repo}/{hf_path}", flush=True)

    file_size = os.path.getsize(file_path)
    send_callback(callback_url, callback_secret, {
        "taskId": task_id,
        "stage": "gha_uploading_hf",
        "progress": 0,
        "speed": "0 KB/s",
        "eta": "Uploading...",
        "total": format_bytes(file_size),
        "message": f"Uploading to Hugging Face dataset ({format_bytes(file_size)})..."
    })

    api = HfApi(token=hf_token)
    api.upload_file(
        path_or_fileobj=file_path,
        path_in_repo=hf_path,
        repo_id=hf_repo,
        repo_type="dataset"
    )

    hf_direct_url = f"https://huggingface.co/datasets/{hf_repo}/resolve/main/{hf_path}"

    send_callback(callback_url, callback_secret, {
        "taskId": task_id,
        "stage": "hf_ready",
        "progress": 100,
        "speed": "Uploaded",
        "eta": "00s",
        "hfFilePath": hf_path,
        "hfDownloadUrl": hf_direct_url,
        "total": format_bytes(file_size),
        "message": "Successfully uploaded to Hugging Face dataset."
    })
    print("[✓] Upload to Hugging Face completed.", flush=True)


def main():
    parser = argparse.ArgumentParser(description="PTSLibrary Transcode & Relay Worker")
    parser.add_argument("--task-id", required=True, help="Task ID")
    parser.add_argument("--download-url", required=True, help="Source media download URL")
    parser.add_argument("--filename", required=True, help="Target output filename")
    parser.add_argument("--hf-repo", required=True, help="Hugging Face Dataset Repo")
    parser.add_argument("--hf-token", required=True, help="Hugging Face Write Token")
    parser.add_argument("--callback-url", default="", help="Raspberry Pi callback webhook URL")
    parser.add_argument("--callback-secret", default="", help="Worker auth secret")

    args = parser.parse_args()

    work_dir = os.path.join(os.getcwd(), f"work_{args.task_id}")
    os.makedirs(work_dir, exist_ok=True)

    input_file = os.path.join(work_dir, "input_source.bin")
    output_file = os.path.join(work_dir, args.filename)
    hf_path = f"temp_transcodes/{args.task_id}_{args.filename}"

    try:
        # Step 1: High-Speed Multi-Connection Download (16 streams via aria2c)
        download_source(args.download_url, input_file, args.task_id, args.callback_url, args.callback_secret)

        # Step 2: Transcode 480p x265 (Fast preset, pipe deadlock protected)
        transcode_to_480p_h265(input_file, output_file, work_dir, args.task_id, args.callback_url, args.callback_secret)

        # Step 3: Upload to Hugging Face
        upload_to_huggingface(output_file, args.hf_repo, args.hf_token, hf_path, args.task_id, args.callback_url, args.callback_secret)

        print("[✓] All workflow worker tasks completed successfully!", flush=True)

    except Exception as e:
        print(f"[Fatal Error] {e}", file=sys.stderr, flush=True)
        send_callback(args.callback_url, args.callback_secret, {
            "taskId": args.task_id,
            "stage": "failed",
            "progress": 0,
            "error": str(e),
            "message": f"Worker Error: {str(e)}"
        })
        sys.exit(1)


if __name__ == "__main__":
    main()
