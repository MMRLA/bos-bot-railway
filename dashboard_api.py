from fastapi import FastAPI
from fastapi.responses import FileResponse
import json
import os

app = FastAPI()

STATE_FILE = "/opt/bos-bot/data/state.json"
DASHBOARD_FILE = "/opt/bos-bot/dashboard.html"


@app.get("/")
def root():
    return {"status": "running"}


@app.get("/state")
def get_state():
    if not os.path.exists(STATE_FILE):
        return {"error": "no data yet"}

    with open(STATE_FILE, "r") as f:
        return json.load(f)


@app.get("/dashboard")
def dashboard():
    return FileResponse(DASHBOARD_FILE)
