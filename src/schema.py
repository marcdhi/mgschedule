from pydantic import BaseModel

class Schedule(BaseModel):
    model: str 
    client:str 
    bu: int 
    recur_at: str
    exact_date: str = None
    day: str = None
    time: str 
    from_schedule: bool = True
    
     