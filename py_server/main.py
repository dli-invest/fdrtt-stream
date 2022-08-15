"""
FastAPI Streaming Log Viewer over WebSockets

1. Read last n-lines from specified log file
2. Stream log data over WebSockets
3. Simple log viewer page
"""

# import libraries
from typing import Union
import uvicorn
import asyncio
import mysql.connector
import os
import pandas as pd
import spacy
nlp = spacy.load("en_core_web_sm")
from pathlib import Path
from fastapi import FastAPI, WebSocket, Request, Query
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# set path and log file name
base_dir = Path(__file__).resolve().parent
log_file = "app.log"

# create fastapi instance
app = FastAPI()

# set template and static file directories for Jinja
templates = Jinja2Templates(directory=str(Path(base_dir, "templates")))
app.mount("/static", StaticFiles(directory="static"), name="static")

def connect_to_db():
    # read database credentials from environment variables
    db_host = os.environ.get("DB_HOST")
    db_user = os.environ.get("DB_USER")
    db_pass = os.environ.get("DB_PASSWORD")
    db_name = os.environ.get("DB_NAME")
    return mysql.connector.connect(
        user=db_user, password=db_pass, host=db_host, database=db_name
    )

async def fetch_sql_data(video_id: str):
    # find all results from dp8PhLsUcFE within the last day
    query = "SELECT * FROM dp8PhLsUcFE WHERE created_at > DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY) ORDER BY created_at DESC"
    # Dont care about previous data
    new_df = pd.read_sql(query, con=connect_to_db())
    print("FETCHING NEW DATA +", len(new_df))
    return new_df

async def log_reader(n=5) -> list:
    """Log reader

    Args:
        n (int, optional): number of lines to read from file. Defaults to 5.

    Returns:
        list: List containing last n-lines in log file with html tags.
    """
    # dataframe 
    df = await fetch_sql_data("test")
    # reverse order of dataframe
    df = df.iloc[::-1]
    log_lines = []
    for line in df["text"].tolist():
        doc = nlp(line)
        html = spacy.displacy.render(doc, style="ent", page=False)
        log_lines.append(html)
    return log_lines


@app.get("/")
async def get(request: Request) -> templates.TemplateResponse:
    """Log file viewer

    Args:
        request (Request): Default web request.

    Returns:
        TemplateResponse: Jinja template with context data.
    """
    PORT = os.environ.get("PORT", 8000)
    HOST = os.environ.get("HOST", "0.0.0.0")
    context = {"title": "FastAPI Streaming Log Viewer over WebSockets", "log_file": log_file, "port": PORT, "host": HOST}
    return templates.TemplateResponse("index.html", {"request": request, "context": context})


# fix app websocket add param
@app.websocket("/ws/log")
async def websocket_endpoint_log(websocket: WebSocket, video_id: Union[str, None] = Query(default=None),) -> None:
    """WebSocket endpoint for client connections

    Args:
        websocket (WebSocket): WebSocket request from client.
    """
    await websocket.accept()

    try:
        while True:
            print("Video ID:", video_id)
            logs = await log_reader(30)
            await websocket.send_text(logs)
            await asyncio.sleep(60)
    except Exception as e:
        print(e)
    finally:
        await websocket.close()

# set parameters to run uvicorn
if __name__ == "__main__":

    # get PORT varaible
    PORT = os.environ.get("PORT", 8000)
    HOST = os.environ.get("HOST", "fdrtt-stream-production.up.railway.app")
    uvicorn.run(
        "main:app",
        host=HOST,
        port=int(PORT),
        log_level="info",
        reload=True,
        workers=1,
        debug=False,
    )
