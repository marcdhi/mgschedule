from typing import Any, Optional
import datetime
import hashlib


class MGScheduler:
    jobs = {}

    def __init__(self, model: str, client:str, bu: str, recur_at: str, time: str, today: str) -> None:
        # Assign the data to the object
        self.model = model
        self.bu = bu
        self.client = client
        self.recur_at = recur_at
        self.time = time
        self.today = today

        # Hash the data to create a unique ID
        to_hash_data = self.model + self.bu + self.client
        hash_object = hashlib.sha256(to_hash_data.encode())
        hex_dig = hash_object.hexdigest()
        print(hex_dig)

        # Add the job to the dict of job
        self.jobs.update({
            "id": hex_dig,
            "model": self.model,
            "bu": self.bu,
            "client": self.client,
            "recur_at": self.recur_at,
            "time": self.time,
            "today": self.today
        })

    def confirm_job(self) -> None:
        print("Your job has been scheduled! Here are your details:", self.jobs)

    @classmethod
    def list_jobs(cls) -> list:
        list_of_all_jobs = cls.jobs
        
        return list_of_all_jobs

    @classmethod
    def sort_jobs(cls) -> list:
        existing_jobs = cls.jobs
        # Assign weights to recurrence types
        recurrence_weights = {"daily": 1, "weekly": 2, "monthly": 3}
        sorted_jobs = sorted(existing_jobs, key=lambda x: (recurrence_weights.get(x["recur_at"], 0), x["time"]))
        
        return sorted_jobs

class MGJob:
    def __init__(self, mgscheduler: Optional[MGScheduler] = None) -> None:
        self.mgscheduler = mgscheduler

    def requested_schedule_type(self) -> None:
        job = self.mgscheduler
        string_time = job["time"].strftime("%I:%M %p")
        when = job["recur_at"] + " " + string_time
        print("Your job is scheduled as -", when)

    def next_run(self, today: str) -> datetime.datetime:
        if self.mgscheduler["recur_at"] == "weekly":
            exact_time = self.mgscheduler["time"]
            when_next_day = today + datetime.timedelta(days=7) 
            when_next = datetime.datetime.combine(when_next_day, exact_time.time())
            
            return when_next

        elif self.mgscheduler["recur_at"] == "monthly":
            exact_time = self.mgscheduler["time"]
            when_next_month = today + datetime.timedelta(days=30)
            when_next = datetime.datetime.combine(when_next_month, exact_time.time())
            
            return when_next

        elif self.mgscheduler["recur_at"] == "daily":
            exact_time = self.mgscheduler["time"]
            when_next_day = today + datetime.timedelta(days=1)
            when_next = datetime.datetime.combine(when_next_day, exact_time.time())
            
            return when_next

# import datetime

# # recur_at = "weekly"
# recur_at = "monthly"
# # recur_at = "daily"

# today = datetime.datetime.now()

# schedule_time = datetime.datetime.strptime("5:00 PM", "%I:%M %p")

# # Example usage
# event_2 = MGScheduler("INCIDENT_PROBABILITY", "MB", "Hoist 11" ,recur_at , schedule_time , today)

# # event_1.confirm_job()
# # MGScheduler.list_jobs()
# sorted_jobs = MGScheduler.sort_jobs()

# job1 = MGJob(sorted_jobs[0])
# job1.requested_schedule_type()
# job_scheduled_at = job1.next_run(today)
# print(job_scheduled_at)