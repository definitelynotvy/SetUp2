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

sys.path.append("D:\SetUp\ReadData\platform-tools")
print(sys.path)

# Path to the ADB executable
ADB_PATH = "./ReadData/platform-tools/adb.exe"

# Path to the folder on the phone where the CSV file will be created
# PHONE_FOLDER = "/sdcard/Download/OximeterData/DataModel"
PHONE_FOLDER = "/sdcard/Download/OximeterData"

ANNOTATION_FOLDER = "/sdcard/Download/OximeterData/Annotation"

# Path to save files on your computer
PC_FOLDER = r"D:/24EIc"

# Dictionary to track data read from each file
file_data_count = defaultdict(int)

# Dictionary to store the line number where we left off
file_line_count = defaultdict(int)

device_found = False

processed_starttimes = set()

current_file_processing = None

last_email_sent = datetime.min  # Initialize with minimum datetime
cooldown_minutes = 5  # Cooldown period in minutes

# Configure loguru to output to both console and file
logger.remove()  # Remove default handler
logger.add(sys.stdout, 
          format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
          colorize=True)
logger.add("file_watch.log", 
          rotation="10 MB",
          format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")

def get_device_id():
    try:
        result = subprocess.run([ADB_PATH, "devices"], capture_output=True, text=True, check=True)
        # Split output into lines and remove empty lines
        lines = [line.strip() for line in result.stdout.split('\n') if line.strip()]
        
        # Remove the first line which is "List of devices attached"
        device_lines = lines[1:]
        
        if not device_lines:
            logger.error("No devices found")
            return None
        elif len(device_lines) > 1:
            logger.warning("Multiple devices found. Using the first one.")
            
        # Extract device ID from the first device line (format: "RF8M12WQKYJ device")
        device_id = device_lines[0].split()[0]
        # logger.info(f"Found device: {device_id}")
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

def get_file_list():
    return run_adb_command(["shell", f"ls {PHONE_FOLDER}"])

def pull_file(filename):
    filepath = organize_file_path(filename)
    # print("text path or folder", filepath)
    run_adb_command(["pull", f"{PHONE_FOLDER}/{filename}", filepath])

def read_lines_excluding_last(filename, start_line=0):
    """
    Read the file from start_line to just before the last line to avoid incomplete data.
    """
    ##folder = organize_file_path(filename)
    #file_path = os.path.join(folder, filename)
    file_path = organize_file_path(filename)
    lines = []

    try:
        with open(file_path, 'r') as file:
            all_lines = file.readlines()
            if len(all_lines) > 1:
                # Return lines from start_line up to the second-last line (excluding the last line)
                lines = all_lines[start_line:-1]
    except IOError as e:
        logger.error(f"Error reading file {file_path}: {e}")
    
    return lines

def organize_file_path(filename: str, base_path: str = "D:\\24EIc") -> str:
    parts = filename.split('_')
    if len(parts) >= 3:
        study_code = parts[1]  # 24EIc-003-0011
        date_parts = parts[2].split('.')  # [02, 12, 2024]
        
        # Format date without dots (DDMMYYYY)
        formatted_date = f"{date_parts[0]}{date_parts[1]}{date_parts[2]}"
        
        # Combine study code and date
        folder_name = f"{study_code} {formatted_date}"
        
        # Create folder structure
        study_path = os.path.join(base_path, folder_name)
        os.makedirs(study_path, exist_ok=True)
        
        return os.path.join(study_path, filename)
    return os.path.join(base_path, filename)


def extract_research_code(filename):
    """Extract research code from filename like SmartCareCsv_24EIc-003-001U_26.12.2024.17.14.02_26.12.2024.17.18.43.csv"""
    pattern = r'SmartCareCsv_([^_]+)_'
    match = re.search(pattern, filename)
    if match:
        return match.group(1)
    return None
# Define the email sending function
def send_email(subject,msg):
    try:
        pythoncom.CoInitialize()  # Initialize COM library
        mail_sender = MailSender(use_banana_style=True)
        mail_sender.send_mail(os.getenv("DEFAULT_FROM"), subject, msg)
        logger.debug("Oximeter Drop detected. Email sent.")
    except Exception as e:
        logger.error(f"Error sending email: {e}")
    finally:
        pythoncom.CoUninitialize()

def log_device_event(event_type, msg, value=None):
    """
    Log device events to a single JSON file
    
    Args:
        event_type (str): Type of event ('drop' or 'noise')
        msg (str): Description message
        value (any, optional): The value associated with the event
    """
    log_file = "device_events.json"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    log_entry = {
        "timestamp": timestamp,
        "type": event_type,
        "message": msg,
        "value": value
    }
    
    try:
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    data = {"events": []}
        else:
            data = {"events": []}
            
        data["events"].append(log_entry)
        
        with open(log_file, 'w') as f:
            json.dump(data, f, indent=4)
            
        logger.debug(f"Event logged: {event_type} - {msg}")
    except Exception as e:
        logger.error(f"Failed to log event: {e}")

# Check for drop in SpO2 value
def check_drop(df):
    count = len(df[df['spo2_status'].apply(lambda x: '1' in x)])
    print("count:", count)
    if count > 3:
        return True
    return False
# Check for noise in perfusion value
def check_noise(df, threshold=6):
    df['perfusion'] = df['perfusion'].apply(lambda x: ast.literal_eval(x))
    count = len(df[df['perfusion'].apply(lambda x: np.quantile(x, 0.75) > threshold)])
    print("count:", count)
    if count > 60:
        return True
    return False
# Define the function to read new data from the file
def read_new_data(filename):
    file_path = organize_file_path(filename)
    global current_file_processing 
    current_file_processing = file_path
    no_new_data_count = 0
    last_size = 0
    
    # Store data in list for batch processing
    data_buffer = []
    headers = [
        "timestamp", "device_id", "battery", "hr", "o2", "spo2_status", 
        "pleth", "red", "ir", "perfusion"
    ]

    while True:
        try:
            pull_file(filename)
        except Exception as e:
            if "No such file or directory" in str(e):
                logger.error(f"File {filename} not found on device.")
                break
            else:
                logger.error(f"Unexpected error pulling file {filename}: {e}")
                break

        current_size = os.path.getsize(file_path)

        if current_size > last_size:
            start_line = file_line_count.get(filename, 0)
            new_lines = read_lines_excluding_last(filename, start_line=start_line)

            if new_lines:
                for line in new_lines:
                    # Extract data                   
                    data = line.split('","')
                    if "timestamp" in data[0].lower(): continue  # Skip header row
                        
                    row_data = [
                        data[0].split('"')[1],  # timestamp
                        data[1],                # device
                        data[2],                # battery
                        data[3],                # hr
                        data[4],                # spo2
                        data[5],                # spo2_status
                        data[6],                # pleth
                        data[7],                # red
                        data[8],                # ir
                        data[9].split('"')[0]     # perfusion
                    ]
                    logger.infor("Writing data to buffer")
                    data_buffer.append(row_data)
                    file_data_count[filename] = file_data_count.get(filename, 0) + 1
                    file_line_count[filename] = file_line_count.get(filename, 0) + 1

                    # When buffer reaches 180 lines, save to CSV
                    if len(data_buffer) >= 180:
                        # Convert buffer to DataFrame
                        df = pd.DataFrame(data_buffer, columns=headers)
                        df.dropna(axis=0, how='any',inplace=True)
                        # Handle drop and noise data 
                        if check_drop(df):
                            log_device_event("drop", f"Drop detected in {filename}")
                            send_email("Oximeter Drop Detected", "Please check the patient")
                        if check_noise(df):
                            log_device_event("noise", f"Noise detected in {filename}")
                            send_email("Data Noise Detected", "Please check the device")


                        # Create output directory if not exists
                        output_dir = "D:/Data/Test"
                        os.makedirs(output_dir, exist_ok=True)
                        
                        # Generate output filename with timestamp
                        research_code = extract_research_code(filename)
                        output_file = os.path.join(output_dir, f"SmartCareCsv_{research_code}_04.11.2024.10.36.00_04.11.2024.10.39.00.csv")
                        
                        # Save DataFrame to CSV in one operation
                        df.to_csv(output_file, index=False)
                        logger.info(f"Generated 3min CSV file")
                        
                        # Clear buffer
                        data_buffer = []

                last_size = current_size
                no_new_data_count = 0
            else:
                logger.info(f"No new lines found in {filename}, waiting...")

        time.sleep(1)

def extract_starttime(filename):
    # Split by underscore to separate the datetime parts
    parts = filename.split('_')
    
    # The start time will be the second to last element (index -2)
    start_time = parts[-2]
    
    if start_time:
        # Extract the date part (first 10 characters)
        date_str = start_time[:10]
        # Convert to datetime object
        date_obj = datetime.strptime(date_str, "%d.%m.%Y").date()
        return date_obj
    return None



def pull_annotation_file():
    last_file_list = set()

    
    file_list = run_adb_command(["shell", f"ls {ANNOTATION_FOLDER}"])
    if file_list is not None:
        current_file_list = set(file_list.split())
        new_files = current_file_list - last_file_list

        for filename in new_files:
            if filename.endswith(".csv"):
                filepath = organize_file_path(filename)
                # print("text path or folder", filepath)
                run_adb_command(["pull", f"{ANNOTATION_FOLDER}/{filename}", filepath])

def monitor_folder():
    last_file_list = set()

    while True:
        file_list = get_file_list()
        if file_list is None:
            logger.warning("Failed to get file list. Retrying in 10 seconds...")
            time.sleep(10)
            continue

        current_file_list = set(file_list.split())
        new_files = current_file_list - last_file_list
        current_date = datetime.now().date()

        for filename in new_files:
            if filename.endswith(".csv"):
                file_datetime = extract_starttime(filename)
 
                if file_datetime and file_datetime == current_date: #
                    logger.info(f"New CSV file detected: {filename}")
                    read_new_data(filename)
                    #after reading the file, we delete file with "temp" in the name
                    directory = os.path.dirname(current_file_processing)
                    print("directory:", directory)
                    for f in os.listdir(directory):
                        if "temp" in f:
                            os.remove(os.path.join(directory, f))
                    #after all we start pull annotation file
                    pull_annotation_file()
                else:
                    logger.info(f"Skipping file from different date: {filename}")
                

        last_file_list = current_file_list
        time.sleep(5)  # Wait for 5 seconds before checking for new files

if __name__ == "__main__":
    try:
        logger.info("Starting ADB Monitor Script...")
        logger.info(f"Log file will be saved as: file_watch.log")
        logger.info(f"Monitoring folder: {PHONE_FOLDER}")
        
        device_id = get_device_id()
        if device_id:
            logger.info(f"Starting to monitor folder {PHONE_FOLDER} on device {device_id}...")
            monitor_folder()
        else:
            logger.error("No device found. Please connect a device and try again.")
    except KeyboardInterrupt:
        logger.info("Script stopped by user")
    except Exception as e:
        logger.exception(f"Unexpected error occurred: {e}") 