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
import asyncio  

# Configure logging to write to a file
logging.basicConfig(filename='dev.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    try:
        logger.info(data)

        today = datetime.datetime.now()

        time = data.time
        schedule_time = datetime.datetime.strptime(time, "%I:%M %p")

        cap_model = data.model.upper()
        cap_client = data.client.upper()
        day_to_schedule_at = data.day
        date_to_schedule_at = data.exact_date
        # using the MGScheduler class to instantiate the object
        mgschedule = MGScheduler(cap_model, cap_client, data.bu, data.recur_at, day_to_schedule_at, date_to_schedule_at, schedule_time, today)

        #####
        # method to list all the jobs & log it
        #####
        dict_of_jobs = mgschedule.list_jobs()
        logger.info(dict_of_jobs)

        #####
        # using the MGJob class to instantiate the object
        #####
        job = MGJob(dict_of_jobs)
        job.requested_schedule_type()
        next_run = job.next_run(today)
        logger.info("Your job has been scheduled to run on %s", next_run)

        # convert the datetime object to string
        string_time = next_run.strftime("%I:%M %p")
        string_today_date = today.strftime("%d-%m-%Y")
        string_current_time = today.strftime("%I:%M %p")

        string_exact_today_combined = string_today_date + " " + string_current_time
        string_next_run = next_run.strftime("%d-%m-%Y %I:%M %p")

        #####
        # Store the payload in a dictionary
        #####

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

        #####
        # Create a JSON file if it doesn't exist
        #####

        if not os.path.exists('schedule.json'):
            with open('schedule.json', 'w') as file:
                json.dump([], file)

        #####
        # Load existing data from the JSON file
        #####
                
        try:
            with open('schedule.json', 'r') as file:
                content = file.read()
                # print("File content:", repr(content))
                if not content:
                    existing_data = []
                else:
                    existing_data = json.loads(content)
        except Exception as e:
            logger.error(f"Error reading schedule file: {str(e)}")
            existing_data = []

        #####
        # Check if the payload's ID already exists
        #####
            
        existing_ids = [item.get('id') for item in existing_data]
        if store_payload_in_db['id'] in existing_ids:
            # Update the existing object
            index = existing_ids.index(store_payload_in_db['id'])
            existing_data[index] = store_payload_in_db
        else:
            # Append the new payload
            existing_data.append(store_payload_in_db)


        #####
        # Write the updated payload to the JSON file
        #####
        try:
            with open('schedule.json', 'w') as file:
                json.dump(existing_data, file, indent=2)
        except Exception as e:
            logger.error(f"Error writing to schedule file: {str(e)}")

        #####
        # Read the latest content from the JSON file
        #####
        try:
            with open('schedule.json', 'r') as file:
                latest_content = file.read()
        except Exception as e:
            logger.error(f"Error reading schedule file: {str(e)}")
            return None
            
        return latest_content
    
    except Exception as e:
        logger.error(f"Error in mgschedule: {str(e)}")
        return JSONResponse(content={"error": "Internal Server Error"}, status_code=500)

def rule_based_api(payload):

    payload_array = json.loads(payload)

    today = datetime.datetime.now()
    string_today = today.strftime("%d-%m-%Y")
    string_current_time = today.strftime("%I:%M %p")
    exact_today_with_time = string_today + " " + string_current_time
    datetime_convert_today = datetime.datetime.strptime(exact_today_with_time, "%d-%m-%Y %I:%M %p")

    for job in payload_array:
        int_next_run = datetime.datetime.strptime(job["next_run"], "%d-%m-%Y %I:%M %p")

        if datetime_convert_today == int_next_run: # this is if the job is due to run today at the scheduled time

            logger.info("Running the job ....... model = %s, client = %s", job["model"], job["client"])

            # Updating the next run time and exact today
            if job["recur_at"] == "daily":

                job["next_run"] = (int_next_run + datetime.timedelta(days=1)).strftime("%d-%m-%Y %I:%M %p")
                job["today"] = exact_today_with_time

                logger.info("Updating the next run time to: %s", job["next_run"])
                logger.info("Updating the exact today to: %s", job["today"])

                with open('schedule.json', 'w') as file:
                    json.dump(payload_array, file, indent=2)
            
            elif job["recur_at"] == "weekly":

                job["next_run"] = (int_next_run + datetime.timedelta(days=7)).strftime("%d-%m-%Y %I:%M %p")
                job["today"] = exact_today_with_time

                logger.info("Updating the next run time to: %s", job["next_run"])
                logger.info("Updating the exact today to: %s", job["today"])

                with open('schedule.json', 'w') as file:
                    json.dump(payload_array, file, indent=2)
            
            elif job["recur_at"] == "monthly":

                job["next_run"] = (int_next_run + datetime.timedelta(days=30)).strftime("%d-%m-%Y %I:%M %p")
                job["today"] = exact_today_with_time

                logger.info("Updating the next run time to: %s", job["next_run"])
                logger.info("Updating the exact today to: %s", job["today"])

                with open('schedule.json', 'w') as file:
                    json.dump(payload_array, file, indent=2)

            # Run the Job
            data_run = {
                "model": job["model"],
                "client": job["client"],
                "bu": job["bu"],
            }

            add_your_prediction_functions_here(data_run)
            
        
        elif datetime_convert_today != int_next_run:

            logger.info("Updating job ID = %s to match today's date = %s and time = %s.....",job["id"], string_today, string_current_time)
            job["today"] = exact_today_with_time # make sure to have this, as it updates in string format

            # Write the updated payload to the JSON file
            with open('schedule.json', 'w') as file:
                json.dump(payload_array, file, indent=2)

    logger.info("Successfully updated the schedule.json file")

# Schedule the background job to run every minute
def schedule_background_job():
    try:
        current_time = datetime.datetime.now()
        logger.info("Current time: %s", current_time)
        schedule.every(1).minutes.do(background_job)
        logger.info("Successfully scheduled the background job")
    except Exception as e:
        logger.error(f"Error in schedule background job: {str(e)}")

# Run the scheduled tasks in a separate thread
async def run_scheduled_tasks_async():
    while True:
        schedule.run_pending()
        await asyncio.sleep(1)

# Start the scheduled tasks in a separate thread on app startup
@app.on_event("startup")
def startup_event():
    try:
        logger.info(" ")
        logger.info(" ")
        logger.info("Starting background scheduler...")
        schedule_background_job()
        asyncio.create_task(run_scheduled_tasks_async())
    except Exception as e:
        logger.error(f"Error in startup event: {str(e)}")
        return JSONResponse(content={"error": "Internal Server Error"}, status_code=500)
    # threading.Thread(target=run_scheduled_tasks, daemon=True).start()

def background_job():
    try:
        with open('schedule.json', 'r') as file:
            latest_content = file.read()
        rule_based_api(latest_content)
    except Exception as e:
        logger.error(f"Error in background job: {str(e)}")

# this route is when users wanna schedule a job
@app.post("/schedule")
async def schedule_entry_main(data: Schedule):
    try:
        payload = mgschedule(data)
        logger.info("Payload: %s", payload)
        payload_array = json.loads(payload)
        return JSONResponse(content=payload_array, status_code=201)
    except Exception as e:
        logger.error(f"Error in schedule main: {str(e)}")
        return JSONResponse(content={"error": "Internal Server Error"}, status_code=500)
    
def add_your_prediction_functions_here(data):
    logger.info(" ")
    logger.info("Running the model for %s, %s", data["model"], data["client"])
    logger.info(" ")
    logger.info(data)


