from fastapi import FastAPI
from schema import Schedule
from mgschedule_marcdhi import MGScheduler, MGJob
import datetime
import json

app = FastAPI()

@app.get("/")
def main():
    return {"message": "Hello World"}

def rule_based_api():
    pass

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
    
    return {"Your endpoint will next run on": next_run}