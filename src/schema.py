from pydantic import BaseModel

class Schedule(BaseModel):
    model: str 
    client:str 
    bu: str 
    recur_at: str
    time: str 
    
     