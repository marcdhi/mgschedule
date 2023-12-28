from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from schema import Schedule
from mgschedule_marcdhi import MGScheduler, MGJob
import datetime
import json
import requests
import logging
import os
from fastapi.middleware.cors import CORSMiddleware
import schedule
import time
import threading

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def main():
    return {"message": "Hello World"}

def mgschedule(data):
    print(data)

    today = datetime.datetime.now()
    time = data.time

    schedule_time = datetime.datetime.strptime(time, "%I:%M %p")
    cap_model = data.model.upper()
    cap_client = data.client.upper()
    day_to_schedule_at = data.day
    date_to_schedule_at = data.exact_date
    
    mgschedule = MGScheduler(cap_model, cap_client, data.bu, data.recur_at, day_to_schedule_at, date_to_schedule_at, schedule_time, today)
    dict_of_jobs = mgschedule.list_jobs()
    print(dict_of_jobs)

    job = MGJob(dict_of_jobs)
    job.requested_schedule_type()
    next_run = job.next_run(today)
    print(next_run)

    string_time = next_run.strftime("%I:%M %p")
    string_today_date = today.strftime("%d-%m-%Y")
    string_current_time = today.strftime("%I:%M %p")

    string_exact_today_combined = string_today_date + " " + string_current_time
    string_next_run = next_run.strftime("%d-%m-%Y %I:%M %p")

    store_payload_in_db = {
            "id":dict_of_jobs["id"],
            "model": dict_of_jobs["model"],
            "client": dict_of_jobs["client"],
            "bu": dict_of_jobs["bu"],
            "recur_at": dict_of_jobs["recur_at"],
            "day_at": dict_of_jobs["day_at"],
            "exact_date": dict_of_jobs["exact_date"],
            "time": string_time,
            "today": string_exact_today_combined,
            "next_run": string_next_run,
            "from_schedule": data.from_schedule
    }

    # Create a JSON file if it doesn't exist
    if not os.path.exists('schedule.json'):
        with open('schedule.json', 'w') as file:
            json.dump([], file)

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

    with open('schedule.json', 'r') as file:
        latest_content = file.read()
        
    return latest_content

def rule_based_api(payload):

    payload_array = json.loads(payload)

    today = datetime.datetime.now()
    string_today = today.strftime("%d-%m-%Y")
    string_current_time = today.strftime("%I:%M %p")
    exact_today_with_time = string_today + " " + string_current_time
    datetime_convert_today = datetime.datetime.strptime(exact_today_with_time, "%d-%m-%Y %I:%M %p")

    for job in payload_array:
        int_next_run = datetime.datetime.strptime(job["next_run"], "%d-%m-%Y %I:%M %p")

        if datetime_convert_today == int_next_run:

            print("Running the job.....", job["model"], job["client"])

            # Updating the next run time and today
            if job["recur_at"] == "daily":
                job["next_run"] = (int_next_run + datetime.timedelta(days=1)).strftime("%d-%m-%Y %I:%M %p")
                job["today"] = exact_today_with_time
                print("Updating the next run time to:", job["next_run"])
                with open('schedule.json', 'w') as file:
                    json.dump(payload_array, file, indent=2)
            
            elif job["recur_at"] == "weekly":
                job["next_run"] = (int_next_run + datetime.timedelta(days=7)).strftime("%d-%m-%Y %I:%M %p")
                job["today"] = exact_today_with_time
                print("Updating the next run time to:", job["next_run"])
                with open('schedule.json', 'w') as file:
                    json.dump(payload_array, file, indent=2)
            
            elif job["recur_at"] == "monthly":
                job["next_run"] = (int_next_run + datetime.timedelta(days=30)).strftime("%d-%m-%Y %I:%M %p")
                job["today"] = exact_today_with_time
                print("Updating the next run time to:", job["next_run"])
                with open('schedule.json', 'w') as file:
                    json.dump(payload_array, file, indent=2)

            # Run the Job
            data_run = {
                "model": job["model"],
                "client": job["client"],
                "bu": job["bu"],
                "recur_at": job["recur_at"],
                "time": job["time"]
            }

            hola(data_run)
            
        
        elif datetime_convert_today != int_next_run:

            print("Updating all jobs to match today's date and time.....")
            job["today"] = exact_today_with_time

            # Write the updated payload to the JSON file
            with open('schedule.json', 'w') as file:
                json.dump(payload_array, file, indent=2)

    print("Success!!")

# Schedule the background job to run every minute
def schedule_background_job():
    current_time = datetime.datetime.now()
    print("Current time:", current_time)
    schedule.every(1).minutes.do(background_job)

# Run the scheduled tasks in a separate thread
def run_scheduled_tasks():
    while True:
        schedule.run_pending()
        time.sleep(1)

# Start the scheduled tasks in a separate thread on app startup
@app.on_event("startup")
def startup_event():
    print("Starting background scheduler...")
    schedule_background_job()
    threading.Thread(target=run_scheduled_tasks, daemon=True).start()

def background_job():
    with open('schedule.json', 'r') as file:
        latest_content = file.read()
    rule_based_api(latest_content)
    
# this route is when users wanna schedule a job
@app.post("/schedule")
async def main(data: Schedule):

    payload = mgschedule(data)
    rule_based_api(payload)
    
# this route should be always running in the background      
@app.get("/test")
async def test_job(background_tasks: BackgroundTasks):

    background_tasks.add_task(background_job)

    return JSONResponse(content={"message": "Background job started"}, status_code=200)

def hola(data):
    print(data)
    print("Hola")