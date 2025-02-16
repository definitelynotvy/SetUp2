import requests
from datetime import datetime, date,time
import json

def get_mode(): 
    # API request 
    r = requests.get('http://localhost:8080/api/logs/system_log/') 
    data = r.json() 
    return data['email_mode']
# Get list patient code and id
def get_patients(): 
    # API request 
    r = requests.get('http://localhost:8080/api/patients/') 
    data = r.json() 
    # print(data)
    # Create a dictionary to map reasons to their IDs
    patients_dict = {item['study_code']: item['id'] for item in data}
    # print(patients_dict)
    return patients_dict
# Post model log
def post_pipeline_log(patient_id, content, raw_content):
    # Prepare the JSON data with proper date and time objects
    log_data = {
        "patient_id": int(patient_id),
        "content": str(content),
        "raw_content": json.dumps(raw_content),  # JSON-serialized data
        "date": datetime.now().date().isoformat(),  # Format: YYYY-MM-DD
        "time": datetime.now().time().strftime("%H:%M:%S")  # Format: HH:MM:SS
    }
    header = {"Content-Type": "application/json"}
    # API request
    r = requests.post('http://localhost:8080/api/logs/model_log/', json=log_data, headers=header)
    if r.status_code == 200:
        print("Post model log successfully")
    else:
        print("Error post model log")


if __name__ == "__main__":
    print("This is the pipeline log module")