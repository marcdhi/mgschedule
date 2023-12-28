from typing import Any, Optional
import datetime
import hashlib


class MGScheduler:
    jobs = {}

    def __init__(self, model: str, client:str, bu: int, recur_at: str, day: str, exact_date: str, time: str, today: str) -> None:
        # Assign the data to the object
        self.model = model
        self.bu = bu
        self.client = client
        self.recur_at = recur_at
        self.time = time
        self.today = today
        self.day = day
        self.exact_date = exact_date

        # Hash the data to create a unique ID
        to_hash_data = self.model + self.client
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
            "day_at": self.day,
            "exact_date": self.exact_date,
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
        if self.mgscheduler["recur_at"] == "today":
            exact_time = self.mgscheduler["time"]
            when_next = datetime.datetime.combine(today, exact_time.time())
            
            return when_next

        elif self.mgscheduler["recur_at"] == "daily":
            exact_time = self.mgscheduler["time"]
            when_next_day = today + datetime.timedelta(days=1)
            when_next = datetime.datetime.combine(when_next_day, exact_time.time())
            
            return when_next
    
        elif self.mgscheduler["recur_at"] == "weekly":
            exact_time = self.mgscheduler["time"]
            exact_day = self.mgscheduler["day_at"]

        # Convert the string day to a datetime object
            days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            today_index = days_of_week.index(today.strftime("%A"))
            exact_day_index = days_of_week.index(exact_day)

            print(today_index) 
            print(exact_day_index) 

            # Calculate the number of days until the next occurrence
            days_until_next = (exact_day_index - today_index + 7) % 7
            
            # Calculate the next datetime object
            
            when_next = today + datetime.timedelta(days=days_until_next + 7)
            when_next = datetime.datetime.combine(when_next, exact_time.time())
            
            return when_next

        elif self.mgscheduler["recur_at"] == "monthly":
            exact_time = self.mgscheduler["time"]
            exact_date = self.mgscheduler["exact_date"]

            # Convert the string date to a datetime object
            exact_date = datetime.datetime.strptime(exact_date, "%d-%m-%Y")
            when_next_month = exact_date
            when_next = datetime.datetime.combine(when_next_month, exact_time.time())
            
            return when_next

        
