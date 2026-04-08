from fastapi import FastAPI
import json
import os

app = FastAPI()

STATE_FILE = "/opt/bos-bot/data/state.json"

@app.get("/")
def root():
    return {"status": "running"}

@app.get("/state")
def get_state():
    if not os.path.exists(STATE_FILE):
        return {"error": "no data yet"}
    
    with open(STATE_FILE) as f:
        return json.load(f)
