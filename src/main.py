from fastapi import FastAPI, Request
from schema import Schedule
from mgschedule_marcdhi import MGScheduler, MGJob
import datetime
import json
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

@app.get("/")
def main():
    return {"message": "Hello World"}

def rule_based_api():

    with open('schedule.json', 'r') as file:
        content = file.read()
        if not content:
            existing_data = []
        else:
            existing_data = json.loads(content)
    
    recurrence_weights = {"daily": 1, "weekly": 2, "monthly": 3}
    sorted_jobs = sorted(existing_data, key=lambda x: (recurrence_weights.get(x["recur_at"], 0), x["time"]))
    
    print("Sorted jobs:", sorted_jobs)

    # Check if today is equal to the next run date

    today = datetime.datetime.now()
    # string_today = today.strftime("%d-%m-%Y")
    string_today = "25-01-2024"
    # string_current_time = today.strftime("%I:%M %p")
    string_current_time = "06:00 PM"

    print("Today:", string_today)
    print("Current time:", string_current_time)

    for job in sorted_jobs:

        next_run_day = job["next_run"].split(" ")[0]
        next_run_time = job["next_run"].split(" ")[1] + " " + job["next_run"].split(" ")[2]

        print("Next run day:", next_run_day)
        print("Next run time:", next_run_time)

        if string_today == next_run_day and string_current_time == next_run_time:
            print("Today is the day!")
            data_run = {
                "model": job["model"],
                "client": job["client"],
                "bu": job["bu"],
                "recur_at": job["recur_at"],
                "time": job["time"]
            }
            response = requests.get("http://127.0.0.1:8000/", json=data_run)
            print(response.text)
        elif string_today != next_run_day:
            print("Today is not the day!")

@app.post("/schedule")
def schedule(data: Schedule):
    print(data)

    today = datetime.datetime.now()
    time = data.time

    schedule_time = datetime.datetime.strptime(time, "%I:%M %p")
    cap_model = data.model.upper()
    cap_client = data.client.upper()
    
    mgschedule = MGScheduler(cap_model, cap_client, data.bu, data.recur_at, schedule_time, today)
    dict_of_jobs = mgschedule.list_jobs()
    print(dict_of_jobs)

    job = MGJob(dict_of_jobs)
    job.requested_schedule_type()
    next_run = job.next_run(today)
    print(next_run)

    string_time = next_run.strftime("%I:%M %p")
    string_today = today.strftime("%d-%m-%Y")
    string_next_run = next_run.strftime("%d-%m-%Y %I:%M %p")

    store_payload_in_db = {
            "id":dict_of_jobs["id"],
            "model": dict_of_jobs["model"],
            "client": dict_of_jobs["client"],
            "bu": dict_of_jobs["bu"],
            "recur_at": dict_of_jobs["recur_at"],
            "time": string_time,
            "today": string_today,
            "next_run": string_next_run
    }

    print("Your payload: ", store_payload_in_db)

    # Load existing data from the JSON file
    with open('schedule.json', 'r') as file:
        content = file.read()
        # print("File content:", repr(content))
        if not content:
            existing_data = []
        else:
            existing_data = json.loads(content)


    # Check if the payload's ID already exists
    existing_ids = [item.get('id') for item in existing_data]
    if store_payload_in_db['id'] in existing_ids:
        # Update the existing object
        index = existing_ids.index(store_payload_in_db['id'])
        existing_data[index] = store_payload_in_db
    else:
        # Append the new payload
        existing_data.append(store_payload_in_db)

    with open('schedule.json', 'w') as file:
        json.dump(existing_data, file, indent=2)

    rule_based_api()
    
    return {"Your endpoint will next run on": next_run}