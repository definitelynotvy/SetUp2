from adb_monitor_script import (
    organize_file_path,
    pull_file,
    read_lines_excluding_last,
    extract_starttime
)


sys.path.append("D:\SetUp\ReadData\platform-tools")
print(sys.path)

# Path to the ADB executable
ADB_PATH = "./ReadData/platform-tools/adb.exe"

# Path to the folder on the phone where the CSV file will be created

PHONE_FOLDER = "/sdcard/Download/OximeterData/Annotate"


# Path to save files on your computer
PC_FOLDER = r"D:/24EIc"

# Dictionary to track data read from each file
file_data_count = defaultdict(int)

# Dictionary to store the line number where we left off
file_line_count = defaultdict(int)