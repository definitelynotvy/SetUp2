import os
import time
import subprocess
import sys
from loguru import logger
from collections import defaultdict

# Remove the sys.path.append since we'll use the ADB from PATH
# sys.path.append("D:\SetUp\ReadData\platform-tools")

# Path to the ADB executable - modify to use system ADB
ADB_PATH = "adb"  # Use system ADB instead of a specific path

# Path to the folder on the phone where the CSV file will be created
PHONE_FOLDER = "/sdcard/Download/OximeterData"

# Path to save files on your computer
PC_FOLDER = r"D:\\DataApp"  # Keep your existing save location

# Dictionary to track data read from each file
file_data_count = defaultdict(int)

# Dictionary to store the line number where we left off
file_line_count = defaultdict(int)

# Configure loguru to output to both console and file
logger.remove()  # Remove default handler
logger.add(sys.stdout, 
          format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
          colorize=True)
logger.add("file_watch.log", 
          rotation="10 MB",
          format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")

def connect_device(ip_address):
    """Connect to device over network using port 5555"""
    try:
        # First kill-server to clear any existing connections
        subprocess.run([ADB_PATH, "kill-server"], capture_output=True, text=True)
        time.sleep(2)
        
        # Start server
        subprocess.run([ADB_PATH, "start-server"], capture_output=True, text=True)
        time.sleep(2)
        
        # Connect to the device
        result = subprocess.run([ADB_PATH, "connect", f"{ip_address}:5555"], 
                              capture_output=True, text=True, check=True)
        logger.info(f"Connection attempt result: {result.stdout}")
        
        if "connected" in result.stdout.lower():
            logger.info(f"Successfully connected to device at {ip_address}:5555")
            return True
        return False
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to connect to device: {e}")
        return False

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
            
        # Extract device ID from the first device line (format: "IP:5555 device")
        device_id = device_lines[0].split()[0]
        logger.info(f"Found device: {device_id}")
        return device_id
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to get device ID: {e}")
        return None

# Rest of your functions remain the same
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

# Your other functions remain unchanged
def get_file_list():
    return run_adb_command(["shell", f"ls {PHONE_FOLDER}"])

def pull_file(filename):
    run_adb_command(["pull", f"{PHONE_FOLDER}/{filename}", PC_FOLDER])

def read_lines_excluding_last(filename, start_line=0):
    file_path = os.path.join(PC_FOLDER, filename)
    lines = []

    try:
        with open(file_path, 'r') as file:
            all_lines = file.readlines()
            if len(all_lines) > 1:
                lines = all_lines[start_line:-1]
    except IOError as e:
        logger.error(f"Error reading file {file_path}: {e}")
    
    return lines

def read_new_data(filename):
    file_path = os.path.join(PC_FOLDER, filename)
    no_new_data_count = 0
    last_size = 0

    while True:
        pull_file(filename)
        current_size = os.path.getsize(file_path)

        if current_size > last_size:
            start_line = file_line_count[filename]
            new_lines = read_lines_excluding_last(filename, start_line=start_line)

            if new_lines:
                for line in new_lines:
                    logger.info(f"{line.strip()}")
                    file_data_count[filename] += 1
                    file_line_count[filename] += 1

                last_size = current_size
                no_new_data_count = 0
            else:
                logger.info(f"No new lines found in {filename}, waiting...")
        else:
            no_new_data_count += 1
            logger.info(f"No new data for {no_new_data_count} seconds.")

            if no_new_data_count >= 5:
                logger.info(f"File {filename} completed. Total lines read: {file_data_count[filename]}")
                break

        time.sleep(1)

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
        time.sleep(5)

if __name__ == "__main__":
    try:
        logger.info("Starting ADB Monitor Script...")
        logger.info(f"Log file will be saved as: file_watch.log")
        logger.info(f"Monitoring folder: {PHONE_FOLDER}")
        
        # Ask for device IP address
        device_ip = input("Please enter the IP address of your Android device: ")
        
        # Try to connect to the device
        if connect_device(device_ip):
            device_id = get_device_id()
            if device_id:
                logger.info(f"Starting to monitor folder {PHONE_FOLDER} on device {device_id}...")
                monitor_folder()
            else:
                logger.error("No device found after connection. Please check the IP address and try again.")
        else:
            logger.error("Failed to connect to device. Please check the IP address and try again.")
    except KeyboardInterrupt:
        logger.info("Script stopped by user")
    except Exception as e:
        logger.exception(f"Unexpected error occurred: {e}")
