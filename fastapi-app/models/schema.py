from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, root_validator

#StartedSessions, CompletedSessionByUser
class StartedSessionsSchema(BaseModel):
    player_id: str
    session_id: str
    country: Optional[str]
    ts: datetime

class StartedReturnSchema(BaseModel):
    items: list[StartedSessionsSchema]
    count: int

class CompletedSessionsByUserSchema(BaseModel):
    player_id: str
    session_id: str
    country: Optional[str]
    ts: datetime 

class CompletedReturnSchema(BaseModel):
    items: list[CompletedSessionsByUserSchema]
    count: int