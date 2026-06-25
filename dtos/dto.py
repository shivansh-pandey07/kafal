from pydantic import BaseModel
from datetime import date, datetime

class ScoreRequest(BaseModel):
    id: str
    uid: str
    date: date
    startTime: datetime
    endTime: datetime
    duration: float
    distance: float