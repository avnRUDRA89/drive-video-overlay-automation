# 🎥 Automated Prompt-Overlay Video Processor (Google Drive + FFmpeg)

This project automates the process of **retrieving videos and text prompts from Google Drive**, overlaying text on the videos using **FFmpeg**, and then uploading the final output back to the same Drive folder. It supports recursive folder scanning, robust retry logic, and Google Workspace document handling.

---

## ✅ Features

- 🔁 Recursively scans Google Drive folders and subfolders
- 📥 Downloads videos (`.mp4`, `.mov`, etc.) and associated prompt files (`.txt`, `.docx`, Google Docs)
- 🖋 Overlays:
  - **Name**
  - **Prompt text**
- 🔄 Converts non-MP4 videos to `.mp4` using `ffmpeg-python`
- 📤 Uploads the **final edited video** to Google Drive
- 💾 Optionally downloads the final output locally
- 🔐 Uses a **Google Service Account** with `Drive` and `Sheets` APIs
- 🔁 Retries transient errors with exponential backoff

---

## 🧰 Requirements

Install dependencies with:

```bash
pip install -r requirements.txt
