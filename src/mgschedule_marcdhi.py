import math
import datetime
import time
from typing import Any, Optional

class MGScheduler(object):

    jobs = []

    def __init__(self, model, bu, date, time) -> None:

        self.model = model
        self.bu = bu
        self.date = date
        self.time = time

        self.jobs.append({
            "model": self.model,
            "bu": self.bu,
            "date": self.date,
            "time": self.time
        })
        
       
    def confirmJob(self):

        print("Your job has been scheduled! Here are your details: ", self.jobs)
    
    @classmethod
    def listJobs(cls):

        list_of_all_jobs = cls.jobs

        print(list_of_all_jobs)
        
        return list_of_all_jobs
        
    @classmethod
    def sortJobs(cls):

        existing_jobs = cls.jobs

        sorted_jobs = sorted(existing_jobs, key=lambda x: (x["date"], x["time"]))

        print(sorted_jobs)

        return sorted_jobs
    
class MGJob(object):

    def __init__(self, mgscheduler: Optional[MGScheduler] = None):

       # self.at_time = Optional[datetime.time] = None
        #self.at_timezone = None
        #self.start_date = Optional[str] = None

        self.mgscheduler = mgscheduler

    def when(self):

        print("Your job will be scheduled at: ", self.mgscheduler)

event_1 = MGScheduler("LTIF", "Hoist", "31/12/2023", "6:00 PM")

event_2 = MGScheduler("INCIDENT_PROBABILITY", "Hoist 11", "25/12/2023", "5:00 PM")

event_1.confirmJob()

MGScheduler.listJobs()

MGScheduler.sortJobs()

job1 = MGJob(MGScheduler.sortJobs())

job1.when()



