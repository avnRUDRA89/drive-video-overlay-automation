import os
import io
import re
import time
import random
import logging
import ffmpeg
import subprocess
from pathlib import Path
from typing import Optional, Tuple, Callable, Any, Set, Dict
from functools import wraps

from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

FOLDER_ID = 'YOUR_FOLDER_ID'
SHEET_ID = 'YOUR_SHEET_ID'
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
CREDENTIALS_FILE = "credentials.json"
LOCAL_DESTINATION_PATH = Path("YOUR_FOLDER_PATH")
FINAL_VIDEO_DIR = Path("DESTINATION_FOLDER_PATH")
FONT_PATH = "PATH\\GoogleSans-Regular.ttf"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


CREDS = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=CREDS)
sheet_service = build('sheets', 'v4', credentials=CREDS)

FINAL_VIDEO_DIR.mkdir(parents=True, exist_ok=True)



def retry_with_exponential_backoff(
    max_retries: int = 5,
    base_delay: float = 1,
    max_delay: float = 30,
) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    delay = min(max_delay, base_delay * 2 ** retries) + random.uniform(0, 1)
                    logging.warning(f"⚠️ Error: {e}. Retrying in {delay:.2f} seconds... (Attempt {retries}/{max_retries})")
                    time.sleep(delay)
            raise Exception(f"Failed after {max_retries} retries.")
        return wrapper
    return decorator

def retry_on_transient_errors(func, max_retries=5, retry_delay=5, *args, **kwargs):
    retries = 0
    while retries < max_retries:
        try:
            return func(*args, **kwargs)
        except HttpError as e:
            if e.resp.status in [403, 500, 503]:
                logging.warning(f"⚠️ Transient error {e.resp.status}, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retries += 1
                retry_delay *= 2  
            else:
                raise
        except Exception:
            raise
    raise Exception(f"⚠️ Failed after {max_retries} retries.")

def download_file(drive_service, file_id, filename, is_google_doc=False):
    try:
        if is_google_doc:
            request = drive_service.files().export_media(fileId=file_id, mimeType='text/plain')
        else:
            request = drive_service.files().get_media(fileId=file_id)
        
        with io.FileIO(filename, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
    except HttpError as e:
        logging.warning(f"⚠️ Error downloading file {filename}: {e}")
        raise

def upload_file(drive_service, local_path, folder_id, mimetype='video/mp4'):
    try:
        file_metadata = {'name': os.path.basename(local_path), 'parents': [folder_id]}
        media = MediaFileUpload(local_path, mimetype=mimetype)
        file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file['id']
    except HttpError as e:
        logging.warning(f"⚠️ Error uploading file {local_path}: {e}")
        raise

def check_edited_video_exists(drive_service, folder_id):
    try:
        query = f"'{folder_id}' in parents and name = 'final_video.mp4'"
        results = drive_service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        return len(files) > 0
    except Exception as e:
        logging.warning(f"⚠️ Error checking for edited video in folder {folder_id}: {e}")
        return False

def get_parent_folder_id(drive_service, file_id):
    try:
        file = drive_service.files().get(fileId=file_id, fields='parents').execute()
        parents = file.get('parents', [])
        if parents:
            return parents[0]
        return None
    except HttpError as e:
        if e.resp.status == 404:
            logging.warning(f"⚠️ File not found or inaccessible: {file_id}. Please ensure the file and its folder are shared with the service account.")
        else:
            logging.warning(f"⚠️ An error occurred while accessing file {file_id}: {e}")
        return None

def is_video_file(file) -> bool:
    name = file['name'].lower()
    mime = file['mimeType']
    video_extensions = ['.mp4', '.mov', '.mkv', '.avi', '.flv', '.wmv']
    return 'video' in mime or any(name.endswith(ext) for ext in video_extensions)

def is_prompt_file(file) -> bool:
    name = file['name'].lower()
    mime = file['mimeType']
    known_prompt_mimes = [
        'text/plain',
        'application/octet-stream',
        'application/vnd.google-apps.document',
        'application/vnd.google-apps.form',
        'application/vnd.apple.pages',
        'application/x-appleworks',
        'application/vnd.google-apps.file',
        'application/x-submission-text',
        'application/vnd.ms-word.document.macroEnabled.12',
        'application/msword',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    ]
    known_prompt_extensions = ['.txt', '.text', '.pages', '.doc', '.docx']
    return mime in known_prompt_mimes or any(name.endswith(ext) for ext in known_prompt_extensions)

def convert_video_to_mp4(input_path: Path, output_path: Path) -> bool:
    try:
        if input_path.resolve() == output_path.resolve():
            temp_output = output_path.with_suffix(output_path.suffix + ".tmp.mp4")
            (
                ffmpeg
                .input(str(input_path))
                .output(str(temp_output), vcodec='libx264', acodec='aac', strict='experimental')
                .run(overwrite_output=True)
            )
            temp_output.replace(output_path)
        else:
            (
                ffmpeg
                .input(str(input_path))
                .output(str(output_path), vcodec='libx264', acodec='aac', strict='experimental')
                .run(overwrite_output=True)
            )
        return True
    except ffmpeg.Error as e:
        err_msg = e.stderr.decode() if e.stderr else str(e)
        logging.warning(f"⚠️ ffmpeg error during video conversion: {err_msg}")
        return False

def download_and_convert_file(file: dict, local_path: Path) -> bool:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    is_google_doc = file['mimeType'] == 'application/vnd.google-apps.document'
    temp_download_path = local_path

    if is_google_doc:
        temp_download_path = local_path.with_suffix(local_path.suffix + ".tmp")
    try:
        download_file(drive_service, file['id'], str(temp_download_path), is_google_doc)
    except HttpError as e:
        logging.warning(f"⚠️ Error downloading file {file['name']}: {e}")
        return False

    if is_google_doc:
        try:
            content = temp_download_path.read_text(encoding='utf-8')
            local_path.write_text(content, encoding='utf-8')
            temp_download_path.unlink()
        except Exception as e:
            logging.warning(f"⚠️ Error processing Google Doc file {file['name']}: {e}")
            return False

    elif is_video_file(file):
        ext = local_path.suffix.lower()
        if ext != '.mp4':
            converted_path = local_path.with_suffix('.mp4')
            success = convert_video_to_mp4(temp_download_path, converted_path)
            if success:
                if temp_download_path != local_path:
                    temp_download_path.unlink(missing_ok=True)
                if converted_path != local_path and local_path.exists():
                    local_path.unlink()
                return True
            else:
                return False
    return True

def get_all_subfolders(folder_id: str) -> list:
    subfolders = []
    try:
        query = f"'{folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder'"
        results = drive_service.files().list(q=query, fields="files(id, name)").execute()
        folders = results.get('files', [])
        for folder in folders:
            subfolders.append(folder)
            subfolders.extend(get_all_subfolders(folder['id']))
    except Exception as e:
        logging.warning(f"⚠️ Error getting subfolders of {folder_id}: {e}")
    return subfolders

def download_folder_recursive(folder_id: str, local_path: Path) -> None:
    if not local_path.exists():
        local_path.mkdir(parents=True, exist_ok=True)
    try:
        query = f"'{folder_id}' in parents"
        results = drive_service.files().list(q=query, fields="files(id, name, mimeType)").execute()
        files = results.get('files', [])
        for file in files:
            file_name = file['name']
            file_path = local_path / file_name
            if file['mimeType'] == 'application/vnd.google-apps.folder':
                download_folder_recursive(file['id'], file_path)
            else:
                if is_prompt_file(file):
                    base_name = file_path.stem
                    target_path = local_path / (base_name + '.txt')
                elif is_video_file(file):
                    base_name = file_path.stem
                    target_path = local_path / (base_name + '.mp4')
                else:
                    target_path = file_path
                logging.info(f"Downloading and converting file: {file_name} -> {target_path}")
                success = download_and_convert_file(file, target_path)
                if not success:
                    logging.warning(f"⚠️ Failed to download or convert file: {file_name}")
    except HttpError as e:
        logging.warning(f"⚠️ Error accessing folder {folder_id}: {e}")

def extract_folder_id(url):
    patterns = [
        r'/folders/([a-zA-Z0-9_-]+)',
        r'id=([a-zA-Z0-9_-]+)',
        r'/d/([a-zA-Z0-9_-]+)',
        r'/open\?id=([a-zA-Z0-9_-]+)'
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            extracted_id = match.group(1)
            logging.debug(f"Extracted ID '{extracted_id}' from URL '{url}' using pattern '{pattern}'")
            return extracted_id
    logging.debug(f"No ID extracted from URL '{url}', returning original")
    return url

def overlay_text_on_video(input_path, output_path, name, prompt):
    probe_cmd = [
        'ffprobe', '-v', 'error', '-select_streams', 'v:0',
        '-show_entries', 'stream=width,height', '-of', 'csv=p=0',
        input_path
    ]
    result = subprocess.run(probe_cmd, capture_output=True, text=True)
    logging.debug(f"ffprobe output: '{result.stdout.strip()}'")
    if result.returncode != 0:
        logging.error(f"Error getting video info: {result.stderr}")
        width, height = 1280, 720
    else:
        try:
            width, height = map(int, result.stdout.strip().split(','))
        except Exception as e:
            logging.warning(f"Error parsing video dimensions: {e}")
            width, height = 1280, 720

    if not os.path.isfile(FONT_PATH):
        logging.error(f"Font file not found at {FONT_PATH}. Please check the path.")
        return

    box_padding = 10
    box_color = 'white@0.8'
    fontcolor = 'black'

    fontsize = int(height * 0.035)

    y_pos_prompt = height - fontsize - 40
    y_pos_name = y_pos_prompt - fontsize - 15
    try:
        (
            ffmpeg
            .input(input_path)
            .drawtext(
                text=f"Name: {name}",
                fontfile=FONT_PATH,
                fontsize=fontsize,
                fontcolor=fontcolor,
                x='(w-text_w)/2',
                y=str(y_pos_name),
                box=1,
                boxcolor=box_color,
                boxborderw=box_padding,
                enable='between(t,0,20)'
            )
            .drawtext(
                text=f"Prompt Structure: {prompt}",
                fontfile=FONT_PATH,
                fontsize=fontsize,
                fontcolor=fontcolor,
                x='(w-text_w)/2',
                y=str(y_pos_prompt),
                box=1,
                boxcolor=box_color,
                boxborderw=box_padding,
                enable='between(t,0,20)'
            )
            .output(output_path)
            .run(overwrite_output=True)
        )
    except ffmpeg.Error as e:
        logging.error(f"ffmpeg error: {e.stderr.decode()}")

def process_user(name, prompt, video_file_url):
    logging.debug(f"Processing user '{name}' with video file URL: {video_file_url}")
    video_file_id = extract_folder_id(video_file_url)
    logging.debug(f"Extracted video file ID: {video_file_id}")

    logging.debug(f"Service account email: {CREDS.service_account_email}")

    folder_id = get_parent_folder_id(drive_service, video_file_id)
    if not folder_id:
        logging.error(f"Could not find parent folder for video file ID: {video_file_id}")
        logging.debug("Attempting to list files in the shared folder to diagnose access issues...")
        try:
            query = f"'{FOLDER_ID}' in parents"
            shared_files = drive_service.files().list(q=query, fields="files(id, name)").execute().get('files', [])
            logging.debug(f"Files in shared folder {FOLDER_ID}:")
            for f in shared_files:
                logging.debug(f" - {f['name']} (ID: {f['id']})")
            logging.debug("Recursively listing files in shared folder:")
            list_files_recursively(FOLDER_ID)
        except Exception as e:
            logging.error(f"Error listing files in shared folder {FOLDER_ID}: {e}")
        return
    logging.debug(f"Parent folder ID: {folder_id}")

    query = f"'{folder_id}' in parents"
    files = drive_service.files().list(q=query, fields="files(id, name, mimeType)").execute().get('files', [])
    logging.debug(f"Files found in folder: {[file['name'] for file in files]}")
    logging.debug("Files with IDs:")
    for file in files:
        logging.debug(f" - {file['name']} (ID: {file['id']})")

    video_file = None
    prompt_file = None

    def is_video(file):
        return 'video' in file['mimeType'] or file['name'].lower().endswith(('.mp4', '.mov', '.mkv'))

    def is_prompt(file):
        name = file['name'].lower()
        mime = file['mimeType']
        known_prompt_mimes = [
            'text/plain',
            'application/octet-stream',
            'application/vnd.google-apps.document',
            'application/vnd.google-apps.form',
            'application/vnd.apple.pages',
            'application/x-appleworks',
            'application/vnd.google-apps.file',
            'application/x-submission-text',
            'application/vnd.ms-word.document.macroEnabled.12',
            'application/msword',
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        ]
        known_prompt_extensions = ['.txt', '.text', '.pages', '.doc', '.docx']
        return mime in known_prompt_mimes or any(name.endswith(ext) for ext in known_prompt_extensions)

    for file in files:
        if is_video(file) and not video_file:
            video_file = file
        elif is_prompt(file) and not prompt_file:
            prompt_file = file

    if not video_file or not prompt_file:
        logging.error(f"Missing video or prompt file in folder {folder_id} for user {name}")
        return

    is_google_doc = prompt_file['mimeType'] == 'application/vnd.google-apps.document'

    download_file(drive_service, video_file['id'], 'video.mp4')
    download_file(drive_service, prompt_file['id'], 'prompt.txt', is_google_doc)

    with open('prompt.txt', 'r') as f:
        prompt_text = f.read().strip()

    overlay_text_on_video('video.mp4', 'final_video.mp4', name, prompt_text)
    uploaded_file_id = upload_file(drive_service, 'final_video.mp4', folder_id)

    local_dir = "DESTINATION_FOLDER_PATH"
    if not os.path.exists(local_dir):
        try:
            os.makedirs(local_dir)
            logging.debug(f"Created directory {local_dir} for final video download.")
        except Exception as e:
            logging.error(f"Error creating directory {local_dir}: {e}")
            return
    local_download_path = os.path.join(local_dir, f"{name}_Final_video.mp4")
    logging.debug(f"Downloading final video to local path: {local_download_path}")
    download_file(drive_service, uploaded_file_id, local_download_path)

    logging.info(f"Processed {name} for video {video_file['name']}")

def list_files_recursively(folder_id, indent=0):
    try:
        query = f"'{folder_id}' in parents"
        files = drive_service.files().list(q=query, fields="files(id, name, mimeType)").execute().get('files', [])
        for file in files:
            logging.debug("  " * indent + f"- {file['name']} (ID: {file['id']}, Type: {file['mimeType']})")
            if file['mimeType'] == 'application/vnd.google-apps.folder':
                list_files_recursively(file['id'], indent + 1)
    except Exception as e:
        logging.error(f"Error listing files recursively in folder {folder_id}: {e}")

def process_folder(folder_id):
    def _process():
        files = drive_service.files().list(q=f"'{folder_id}' in parents", fields="files(id, name, mimeType)").execute().get('files', [])
        video_file = None
        prompt_file = None
        for file in files:
            fname = file['name']
            if fname.endswith(".mp4") and not video_file:
                video_file = file
            elif (fname.endswith(".txt") or fname.startswith("Prompt_")) and not prompt_file:
                prompt_file = file

        if not video_file or not prompt_file:
            logging.error(f"Missing video or prompt file in folder {folder_id}")
            return

        folder = drive_service.files().get(fileId=folder_id, fields='name').execute()
        name = folder.get('name', 'Unknown')
        is_google_doc = prompt_file['mimeType'] == 'application/vnd.google-apps.document'

        download_file(drive_service, video_file['id'], 'video.mp4')
        download_file(drive_service, prompt_file['id'], 'prompt.txt', is_google_doc)

        with open('prompt.txt', 'r') as f:
            prompt_text = f.read().strip()

        process_user(name, prompt_text, f"https://drive.google.com/open?id={video_file['id']}")

    try:
        retry_on_transient_errors(_process)
    except Exception as e:
        logging.error(f"Failed to process folder {folder_id}: {e}")

def main():
    root_folder_id = FOLDER_ID
    processed_folders = set()

    logging.info(f"Starting continuous processing from root folder ID: {root_folder_id}")

    while True:
        if root_folder_id not in processed_folders or not check_edited_video_exists(drive_service, root_folder_id):
            logging.info(f"Processing root folder: {root_folder_id}")
            process_folder(root_folder_id)
            processed_folders.add(root_folder_id)

        subfolders = get_all_subfolders(root_folder_id)
        logging.info(f"Found {len(subfolders)} subfolders to process.")

        for folder in subfolders:
            folder_id = folder['id']
            if folder_id not in processed_folders or not check_edited_video_exists(drive_service, folder_id):
                logging.info(f"Processing folder: {folder['name']} (ID: {folder_id})")
                process_folder(folder_id)
                processed_folders.add(folder_id)

        logging.info("Waiting 30 seconds before checking for new entries...")
        time.sleep(30)

if __name__ == "__main__":
    main()
