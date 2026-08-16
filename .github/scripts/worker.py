#!/usr/bin/env python3
"""
PTSLibrary - Automated GitHub Actions Transcode & Hugging Face Relay Worker
Downloads source media, transcodes to 480p H.265 with all audio tracks (128k),
and uploads to Hugging Face dataset, streaming progress back to your Raspberry Pi.
"""

import sys
import os
import time
import json
import argparse
import subprocess
import urllib.request
import urllib.error
from urllib.parse import urlparse

try:
    from huggingface_hub import HfApi
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "huggingface_hub", "requests", "tqdm"])
    from huggingface_hub import HfApi


def send_callback(callback_url, callback_secret, payload):
    """Sends a real-time progress update back to the Raspberry Pi hub."""
    if not callback_url:
        print("[Status]", payload)
        return

    try:
        data = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "X-Worker-Secret": callback_secret or ""
        }
        req = urllib.request.Request(callback_url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            pass
    except Exception as e:
        print(f"[Callback Warning] Could not notify hub ({e})", flush=True)


def format_bytes(bytes_val):
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


def download_source(url, output_path, task_id, callback_url, callback_secret):
    """Downloads the remote source file with real-time speed & ETA tracking."""
    print(f"[*] Starting download from: {url}")
    send_callback(callback_url, callback_secret, {
        "taskId": task_id,
        "stage": "gha_downloading",
        "progress": 0,
        "speed": "0 KB/s",
        "eta": "Calculating...",
        "message": "Initializing remote source download..."
    })

    # If aria2c is available, we can use it or python streaming request
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    req = urllib.request.Request(url, headers=headers)
    
    start_time = time.time()
    last_callback_time = 0
    downloaded = 0

    with urllib.request.urlopen(req) as response, open(output_path, "wb") as out_file:
        total_size = response.headers.get("content-length")
        total_size = int(total_size) if total_size else 0

        chunk_size = 1024 * 512  # 512 KB chunks
        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            out_file.write(chunk)
            downloaded += len(chunk)

            now = time.time()
            if now - last_callback_time >= 1.5 or (total_size and downloaded >= total_size):
                elapsed = now - start_time
                speed = downloaded / elapsed if elapsed > 0 else 0
                pct = round((downloaded / total_size) * 100) if total_size > 0 else 50
                rem_bytes = max(0, total_size - downloaded)
                eta = rem_bytes / speed if speed > 0 else 0

                send_callback(callback_url, callback_secret, {
                    "taskId": task_id,
                    "stage": "gha_downloading",
                    "progress": min(99, pct),
                    "speed": format_speed(speed),
                    "eta": format_eta(eta),
                    "transferred": format_bytes(downloaded),
                    "total": format_bytes(total_size) if total_size > 0 else "Unknown",
                    "message": f"Downloading source: {format_bytes(downloaded)} / {format_bytes(total_size) if total_size > 0 else '...'}"
                })
                last_callback_time = now

    send_callback(callback_url, callback_secret, {
        "taskId": task_id,
        "stage": "gha_downloading",
        "progress": 100,
        "speed": "Done",
        "eta": "00s",
        "message": "Source download completed."
    })
    print("[✓] Source download completed successfully.")


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
        print(f"[ffprobe error] {e}")
        return 0


def transcode_to_480p_h265(input_path, output_path, task_id, callback_url, callback_secret):
    """
    Transcodes video to 480p H.265 (libx265) and compresses all audio tracks to 128kbps AAC.
    Preserves all audio languages, streams, and subtitles.
    """
    print("[*] Starting FFmpeg 480p H.265 transcode...")
    duration = get_video_duration(input_path)
    print(f"[*] Detected video duration: {duration:.1f}s")

    send_callback(callback_url, callback_secret, {
        "taskId": task_id,
        "stage": "gha_compressing",
        "progress": 0,
        "speed": "0x",
        "eta": "Calculating...",
        "message": "Starting 480p H.265 encoder (All audios @ 128k)..."
    })

    # FFmpeg command:
    # -vf "scale=-2:480"
    # -c:v libx265 -crf 26 -preset fast
    # -map 0:v:0 -map 0:a -c:a aac -b:a 128k
    # -map 0:s? -c:s copy
    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-vf", "scale=-2:480",
        "-c:v", "libx265",
        "-crf", "26",
        "-preset", "fast",
        "-map", "0:v:0",
        "-map", "0:a",
        "-c:a", "aac",
        "-b:a", "128k",
        "-map", "0:s?",
        "-c:s", "copy",
        "-progress", "pipe:1",
        "-nostats",
        output_path
    ]

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    
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
                        "message": f"Transcoding 480p x265 ({pct}% done, {format_eta(eta_sec)} remaining)"
                    })
                    last_callback_time = now
            except Exception:
                pass

    rc = process.poll()
    if rc != 0:
        err = process.stderr.read()
        raise RuntimeError(f"FFmpeg encoding failed with code {rc}: {err}")

    send_callback(callback_url, callback_secret, {
        "taskId": task_id,
        "stage": "gha_compressing",
        "progress": 100,
        "speed": "Done",
        "eta": "00s",
        "message": "FFmpeg transcode completed successfully."
    })
    print("[✓] FFmpeg transcode completed.")


def upload_to_huggingface(file_path, hf_repo, hf_token, hf_path, task_id, callback_url, callback_secret):
    """Uploads the transcoded file to Hugging Face dataset."""
    print(f"[*] Uploading to Hugging Face: {hf_repo}/{hf_path}")
    
    file_size = os.path.getsize(file_path)
    send_callback(callback_url, callback_secret, {
        "taskId": task_id,
        "stage": "gha_uploading_hf",
        "progress": 0,
        "speed": "0 KB/s",
        "eta": "Calculating...",
        "total": format_bytes(file_size),
        "message": f"Connecting to Hugging Face Hub dataset ({format_bytes(file_size)})..."
    })

    api = HfApi(token=hf_token)
    
    # Track progress wrapper
    start_time = time.time()
    last_callback_time = 0

    class ProgressReader:
        def __init__(self, fileobj, total_bytes):
            self._fileobj = fileobj
            self._total = total_bytes
            self._read = 0

        def read(self, size=-1):
            chunk = self._fileobj.read(size)
            if chunk:
                self._read += len(chunk)
                nonlocal last_callback_time, start_time
                now = time.time()
                if now - last_callback_time >= 1.5 or self._read >= self._total:
                    elapsed = now - start_time
                    speed = self._read / elapsed if elapsed > 0 else 0
                    pct = min(99, max(0, round((self._read / self._total) * 100)))
                    rem_bytes = max(0, self._total - self._read)
                    eta = rem_bytes / speed if speed > 0 else 0

                    send_callback(callback_url, callback_secret, {
                        "taskId": task_id,
                        "stage": "gha_uploading_hf",
                        "progress": pct,
                        "speed": format_speed(speed),
                        "eta": format_eta(eta),
                        "transferred": format_bytes(self._read),
                        "total": format_bytes(self._total),
                        "message": f"Uploading to HF dataset: {format_bytes(self._read)} / {format_bytes(self._total)}"
                    })
                    last_callback_time = now
            return chunk

        def __getattr__(self, attr):
            return getattr(self._fileobj, attr)

    with open(file_path, "rb") as raw_f:
        wrapped_f = ProgressReader(raw_f, file_size)
        api.upload_file(
            path_or_fileobj=wrapped_f,
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
    print("[✓] Upload to Hugging Face completed.")


def main():
    parser = argparse.ArgumentParser(description="PTSLibrary Transcode & Relay Worker")
    parser.add_argument("--task-id", required=True, help="Task ID")
    parser.add_argument("--download-url", required=True, help="Source media download URL")
    parser.add_argument("--filename", required=True, help="Target output filename")
    parser.add_argument("--hf-repo", required=True, help="Hugging Face Dataset Repo (e.g. username/dataset)")
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
        # Step 1: Download
        download_source(args.download_url, input_file, args.task_id, args.callback_url, args.callback_secret)

        # Step 2: Transcode 480p x265 (All audios 128k)
        transcode_to_480p_h265(input_file, output_file, args.task_id, args.callback_url, args.callback_secret)

        # Step 3: Upload to Hugging Face
        upload_to_huggingface(output_file, args.hf_repo, args.hf_token, hf_path, args.task_id, args.callback_url, args.callback_secret)

        print("[✓] All workflow worker tasks completed successfully!")

    except Exception as e:
        print(f"[Fatal Error] {e}", file=sys.stderr)
        send_callback(args.callback_url, args.callback_secret, {
            "taskId": args.task_id,
            "stage": "failed",
            "progress": 0,
            "error": str(e),
            "message": f"Worker encountered an error: {str(e)}"
        })
        sys.exit(1)


if __name__ == "__main__":
    main()
