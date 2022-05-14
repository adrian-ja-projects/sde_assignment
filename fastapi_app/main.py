
#TO-DO:implement security such as apiKey for authorization
#TO-DO:implement RateLimitExceeded to limit request hits per window
from fastapi import FastAPI, Path, status
from fastapi.responses import JSONResponse
from typing import List

from api_config import config, db
from models.schema import StartedReturnSchema, CompletedReturnSchema
from utils.time_utils import time_delta

settings = config.get_db_setting()

app = FastAPI()

session = None

@app.on_event("startup")
def on_startup():
    global session
    session = db.get_session()

@app.get("/health")
async def health():
    "method to provide the health of the app"
    return JSONResponse({"detail": "Healthy"}, status_code=status.HTTP_200_OK)

@app.get("/models/completed-sessions/{player_id}", response_model=StartedReturnSchema)
async def get_completed_sessions(player_id: str = Path(None, description="Last 20 sessions for a giving payer_id")):
    prepared_statement = session.prepare("""
    SELECT player_id, 
    session_id,  
    country,  
    ts 
    FROM session_events.api_completed_sessions 
    WHERE player_id=? 
    ORDER BY ts DESC LIMIT 20""")
    rows = session.execute(prepared_statement, [player_id])
    data = {'items': []}
    for row in rows:
        data['items'].append(row)
    data['count'] = len(data['items'])
    return data

@app.get("/models/started_sessions/{country}/{hours}", response_model=CompletedReturnSchema)
async def get_started_sessions(hours: int = Path(None, description="Last number of hours to fetch started session MAX 24 hrs from current time", le=24), 
country: str = Path(None, description="Country from where the sessions occurred")):
    prepared_statement = session.prepare("""
    SELECT player_id, 
    session_id,
    country,  
    ts 
    FROM session_events.api_start_session_by_hour 
    WHERE ts>=? AND country=? ALLOW FILTERING""")
    rows = session.execute(prepared_statement, (time_delta(hours), country))
    data = {'items': []}
    for row in rows:
        data['items'].append(row)
    data['count'] = len(data['items'])
    print(time_delta(hours))
    return data