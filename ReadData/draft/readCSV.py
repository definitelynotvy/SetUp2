import os
import time
import subprocess
from loguru import logger
from collections import defaultdict

# Path to the ADB executable
# ADB_PATH = r"C:\\Android\\platform-tools\\adb.exe"
ADB_PATH = "./ReadData/platform-tools/adb.exe"

# Device ID for your Galaxy S10e
# DEVICE_ID = "RF8M12WQKYJ"

# Path to the folder on the phone where the CSV file will be created
PHONE_FOLDER = "/sdcard/Download/OximeterData"

# Path to save files on your computer
PC_FOLDER = r"D:\\DataApp"

# Dictionary to track data read from each file
file_data_count = defaultdict(int)

# Dictionary to store the line number where we left off
file_line_count = defaultdict(int)

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
        logger.info(f"Found device: {device_id}")
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
    run_adb_command(["pull", f"{PHONE_FOLDER}/{filename}", PC_FOLDER])

def read_lines_excluding_last(filename, start_line=0):
    """
    Read the file from start_line to just before the last line to avoid incomplete data.
    """
    file_path = os.path.join(PC_FOLDER, filename)
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

def read_new_data(filename):
    file_path = os.path.join(PC_FOLDER, filename)
    no_new_data_count = 0
    last_size = 0

    while True:
        # Pull the file
        pull_file(filename)
        
        # Check if the file size has changed
        current_size = os.path.getsize(file_path)

        if current_size > last_size:
            # Read lines from the last known position up to the second-to-last line
            start_line = file_line_count[filename]
            new_lines = read_lines_excluding_last(filename, start_line=start_line)

            if new_lines:
                # logger.info(f"Reading new lines from {filename}:")
                for line in new_lines:
                    logger.info(f"{line.strip()}")

                    # Process each line and increment the line count
                    file_data_count[filename] += 1
                    file_line_count[filename] += 1  # Keep track of the current read position

                last_size = current_size  # Update last file size
                no_new_data_count = 0  # Reset counter if new data is found
            else:
                logger.info(f"No new lines found in {filename}, waiting...")
        else:
            no_new_data_count += 1
            logger.info(f"No new data for {no_new_data_count} seconds.")

            # If no new data for 3 consecutive checks (3 seconds), consider the file complete
            if no_new_data_count >= 5:
                logger.info(f"File {filename} completed. Total lines read: {file_data_count[filename]}")
                break

        time.sleep(1)  # Check every second for new data

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
        for filename in new_files:
            if filename.endswith(".csv"):
                logger.info(f"New CSV file detected: {filename}")
                read_new_data(filename)

        last_file_list = current_file_list
        time.sleep(5)  # Wait for 5 seconds before checking for new files

if __name__ == "__main__":
    logger.add("file_watch.log", rotation="10 MB")
    
    device_id = get_device_id()
    if device_id:
        print(f"Starting to monitor folder {PHONE_FOLDER} on device {device_id}...")
        logger.info(f"Starting to monitor folder {PHONE_FOLDER} on device {device_id}...")
        monitor_folder()
    else:
        logger.error("No device found. Please connect a device and try again.")
