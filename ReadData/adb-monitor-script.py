import ast
from datetime import datetime, timedelta
import os
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


def parse_and_save_data(input_file, output_file):
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # Define headers based on the data format
        headers = [
           "timestamp","device_id","battery","hr","o2","spo2_status","pleth","red","ir","perfusion"
        ]
        writer.writerow(headers)

        for row_num, row in enumerate(reader, start=1):
            # Each row is in a single column A1
            if len(row) < 1:
                logger.error(f"Skipping row {row_num}: empty row")
                continue

            try:
                # The entire row content is in row[0]; we split it into individual values
                # Using csv.reader again to handle complex quoted strings within the cell
                data = next(csv.reader([row[0]]))

                # Parse fixed columns
                timestamp = data[0]
                mac_address = data[1]
                field1 = data[2]
                field2 = data[3]
                field3 = data[4]

                # Parse array columns by converting string representation to Python lists
                array1 = ast.literal_eval(data[5])
                array2 = ast.literal_eval(data[6])
                array3 = ast.literal_eval(data[7])
                array4 = ast.literal_eval(data[8])
                array5 = ast.literal_eval(data[9])

                # Write parsed data to output file
                writer.writerow([timestamp, mac_address, field1, field2, field3, array1, array2, array3, array4, array5])

            except (ValueError, SyntaxError, IndexError) as e:
                logger.error(f"Error parsing row {row_num}: {e}")
                continue

def parse_and_save_data_in_thread(temp_file_name, output_file):
    threading.Thread(target=parse_and_save_data, args=(temp_file_name, output_file)).start()

# Define the email sending function
def send_email():
    try:
        pythoncom.CoInitialize()  # Initialize COM library
        mail_sender = MailSender(use_banana_style=True)
        mail_sender.send_mail(os.getenv("DEFAULT_FROM"), "Oximeter Drop", "Oximeter Drop detected. Please check the device.")
        logger.debug("Oximeter Drop detected. Email sent.")
    except Exception as e:
        logger.error(f"Error sending email: {e}")
    finally:
        pythoncom.CoUninitialize()

def read_new_data(filename):
    # folder = organize_file_path(filename)
    # file_path = os.path.join(folder, filename)
    file_path = organize_file_path(filename)
    global current_file_processing 
    current_file_processing = file_path
    no_new_data_count = 0
    last_size = 0

    # Create a temporary file for writing data
    with tempfile.NamedTemporaryFile('w+', newline='', delete=False) as temp_file:
        temp_file_name = temp_file.name
        csv_writer = csv.writer(temp_file)

        global last_email_sent # Use global variable
        line_write_count = 0

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
                        
                        csv_writer.writerow([line.strip()])
                        
                        if line.split(",")[3] == '"255"':
                            current_time = datetime.now()
                            # Check if cooldown period has passed
                            if current_time - last_email_sent > timedelta(minutes=cooldown_minutes):
                                email_thread = threading.Thread(target=send_email)
                                email_thread.start()
                                last_email_sent = current_time # Update last email sent time

                    
                        logger.info("writting data...")
                        line_write_count += 1
                        file_data_count[filename] = file_data_count.get(filename, 0) + 1
                        file_line_count[filename] = file_line_count.get(filename, 0) + 1

                        # If 180 lines are written, call parse_and_save_data and reset
                        if line_write_count >= 180:
                            temp_file.flush()
                            output_file = os.path.join("D:/24EIc/Test/Data", "Test_04.11.2024.10.36.00_04.11.2024.10.39.00.csv")
                            parse_and_save_data(temp_file_name, output_file)
                            # parse_and_save_data_in_thread(temp_file_name, output_file)
                            # break
                            logger.debug("generate 3min file ")
                            temp_file.seek(0)
                            temp_file.truncate()
                            line_write_count = 0

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

            time.sleep(1)  # Check every second for new data

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

                if file_datetime and file_datetime == current_date:
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