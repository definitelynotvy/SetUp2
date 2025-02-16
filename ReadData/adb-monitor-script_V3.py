import ast
from datetime import datetime, timedelta
import json
import os
import re
import threading
import time
import subprocess
import sys
from loguru import logger
from collections import defaultdict
import tempfile 
import csv
from mail_sender import MailSender
import pythoncom
import numpy as np
import pandas as pd
from pipeline_log import get_mode, get_patients, post_pipeline_log

sys.path.append("D:\SetUp\ReadData\platform-tools")
print(sys.path)

# Path configurations
ADB_PATH = "./ReadData/platform-tools/adb.exe"
PHONE_FOLDER = "/sdcard/Download/OximeterData"
ANNOTATION_FOLDER = "/sdcard/Download/OximeterData/Annotation"
STREAM_MODEL_FOLDER = "/sdcard/Download/OximeterData/StreamModel"
PC_FOLDER = r"D:/24EIc"

# Global variables
device_found = False
processed_starttimes = set()
current_file_processing = None
last_email_sent = datetime.min
cooldown_minutes = 5

# Configure logging
logger.remove()
logger.add(sys.stdout, 
          format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
          colorize=True)
logger.add("file_watch.log", 
          rotation="10 MB",
          format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")

def get_device_id():
    try:
        result = subprocess.run([ADB_PATH, "devices"], capture_output=True, text=True, check=True)
        lines = [line.strip() for line in result.stdout.split('\n') if line.strip()]
        device_lines = lines[1:]
        
        if not device_lines:
            logger.error("No devices found")
            return None
        elif len(device_lines) > 1:
            logger.warning("Multiple devices found. Using the first one.")
            
        device_id = device_lines[0].split()[0]
        return device_id
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to get device ID: {e}")
        return None
    
def run_adb_command(command):
    device_id = get_device_id()
    if not device_id:
        return None
        
    try:
        result = subprocess.run([ADB_PATH, "-s", device_id] + command, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"ADB command failed: {e}")
        logger.error(f"Command output: {e.output}")
        logger.error(f"Command stderr: {e.stderr}")
        return None

def get_file_list(folder_path):
    """Get file list from a specific folder"""
    return run_adb_command(["shell", f"ls {folder_path}"])

def organize_file_path(filename: str, source_folder: str, base_path: str = "D:\\24EIc") -> str:
    """
    Organize file path based on source folder
    """
    # If file is from StreamModel, save directly to StreamModel folder
    if STREAM_MODEL_FOLDER in source_folder:
        stream_path = os.path.join(base_path, "StreamModel")
        os.makedirs(stream_path, exist_ok=True)
        return os.path.join(stream_path, filename)
    
    # For files from OximeterData, maintain the original organization
    parts = filename.split('_')
    if len(parts) >= 3:
        study_code = parts[1]  # 24EIc-003-0011
        date_parts = parts[2].split('.')  # [02, 12, 2024]
        
        formatted_date = f"{date_parts[0]}{date_parts[1]}{date_parts[2]}"
        folder_name = f"{study_code} {formatted_date}"
        
        study_path = os.path.join(base_path, folder_name)
        os.makedirs(study_path, exist_ok=True)
        
        return os.path.join(study_path, filename)
    
    return os.path.join(base_path, filename)

def extract_research_code(filename):
    pattern = r'SmartCareCsv_([^_]+)_'
    match = re.search(pattern, filename)
    if match:
        return match.group(1)
    return None

def send_email_outlook(subject, msg):
    try:
        pythoncom.CoInitialize()
        mail_sender = MailSender(use_banana_style=True)
        mail_sender.send_mail(os.getenv("DEFAULT_FROM"), subject, msg)
        pythoncom.CoUninitialize()
        logger.debug("Oximeter Drop detected. Email sent.")
    except Exception as e:
        logger.error(f"Error sending email: {e}")
    

def check_drop(df):
    drop_mask = df['spo2_status'].apply(lambda x: '1' in x)
    drop_timestamps = df[drop_mask]['timestamp'].tolist()
    logger.debug(f"Drop timestamps: {drop_timestamps}, count: {len(drop_timestamps)}")
    if len(drop_timestamps) >= 3: 
        return True
    return False

def check_noise(df, threshold=6):
    df['perfusion'] = df['perfusion'].apply(lambda x: ast.literal_eval(x))
    count = len(df[df['perfusion'].apply(lambda x: np.quantile(x, 0.75) > threshold)])
    logger.debug(f"Noise count: {count}")
    if count > 60: return True
    return False

def extract_starttime(filename):
    parts = filename.split('_')
    if len(parts) >= 3:
        start_time = parts[-2]
        date_str = start_time[:10]
        try:
            date_obj = datetime.strptime(date_str, "%d.%m.%Y").date()
            return date_obj
        except ValueError:
            logger.error(f"Invalid date format in filename: {filename}")
    return None

def is_temp_file(filename):
    """Check if file is a temporary file"""
    return "temp" in filename.lower()

def is_current_date_file(filename):
    """Check if file is from current date"""
    try:
        file_datetime = extract_starttime(filename)
        current_date = datetime.now().date()
        return file_datetime == current_date if file_datetime else False
    except:
        return False

def process_data_file(filename, source_folder):
    """Process a data file from the device"""
    global current_file_processing
    
    try:
        #Pull file and organize it based on source
        filepath = organize_file_path(filename, source_folder)
        current_file_processing = filepath
        run_adb_command(["pull", f"{source_folder}/{filename}", filepath])
        
        #Get patient info
        research_code = extract_research_code(filename)
        # patients = get_patients()
        # patient_id = patients.get(research_code)
        
        #Process and check health indicators only for StreamModel files
        if STREAM_MODEL_FOLDER in source_folder:  # Only process StreamModel files
            df = pd.read_csv(filepath)
            if check_drop(df):
                # if get_mode():
                send_email_outlook("Oximeter Drop Detected", "Please check the patient")
                logger.info(f"Drop detected in file {filename}")
            if check_noise(df):
                # if get_mode():
                send_email_outlook("Noise Detected", "Please check the patient")
                logger.info(f"Noise detected in file {filename}")
            # Clear dataframe from memory
            del df
        
        return True
    except Exception as e:
        logger.error(f"Error processing file {filename}: {e}")
        return False

# def pull_annotation_file():
#     """Pull annotation files from device"""
#     file_list = run_adb_command(["shell", f"ls {ANNOTATION_FOLDER}"])
#     if file_list is not None:
#         for filename in file_list.split():
#             if filename.endswith(".csv"):
#                 filepath = organize_file_path(filename)
#                 run_adb_command(["pull", f"{ANNOTATION_FOLDER}/{filename}", filepath])
#                 logger.info(f"Pulled annotation file: {filename}")

def monitor_folders():
    """Monitor both OximeterData and StreamModel folders"""
    last_main_files = set()
    last_stream_files = set()

    while True:
        # Monitor main OximeterData folder
        main_file_list = get_file_list(PHONE_FOLDER)
        if main_file_list is not None:
            current_main_files = set(main_file_list.split())
            new_main_files = current_main_files - last_main_files

            for filename in new_main_files:
                if filename.endswith(".csv"):
                    if not is_temp_file(filename) and is_current_date_file(filename):
                        logger.info(f"New CSV file detected in main folder: {filename}")
                        if process_data_file(filename, PHONE_FOLDER):
                            logger.info(f"Processing completed for file: {filename}")
                            
                            # Pull annotation file after processing
                            # pull_annotation_file()
                    else:
                        if is_temp_file(filename):
                            logger.info(f"Skipping temp file: {filename}")
                        if not is_current_date_file(filename):
                            logger.info(f"Skipping file from different date: {filename}")

            last_main_files = current_main_files

        # Monitor StreamModel folder
        stream_file_list = get_file_list(STREAM_MODEL_FOLDER)
        if stream_file_list is not None:
            current_stream_files = set(stream_file_list.split())
            new_stream_files = current_stream_files - last_stream_files

            for filename in new_stream_files:
                if filename.endswith(".csv") and is_current_date_file(filename):
                    logger.info(f"New CSV file detected in StreamModel folder: {filename}")
                    process_data_file(filename, STREAM_MODEL_FOLDER)
                    logger.info(f"Processing completed for stream file: {filename}")
                else:
                    if not is_current_date_file(filename):
                        logger.info(f"Skipping StreamModel file from different date: {filename}")

            last_stream_files = current_stream_files

        time.sleep(5)

if __name__ == "__main__":
    try:
        logger.info("Starting ADB Monitor Script...")
        logger.info(f"Log file will be saved as: file_watch.log")
        logger.info(f"Monitoring folders:")
        logger.info(f"- Main folder: {PHONE_FOLDER}")
        logger.info(f"- Stream folder: {STREAM_MODEL_FOLDER}")
        # logger.info(f"- Annotation folder: {ANNOTATION_FOLDER}")
        
        device_id = get_device_id()
        if device_id:
            logger.info(f"Connected to device {device_id}...")
            monitor_folders()
        else:
            logger.error("No device found. Please connect a device and try again.")
    except KeyboardInterrupt:
        logger.info("Script stopped by user")
    except Exception as e:
        logger.exception(f"Unexpected error occurred: {e}")