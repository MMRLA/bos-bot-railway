from fastapi import FastAPI
from fastapi.responses import FileResponse
import json
import os

app = FastAPI()

STATE_FILE = "/opt/bos-bot/data/state.json"
HISTORY_FILE = "/opt/bos-bot/data/history.jsonl"
DASHBOARD_FILE = "/opt/bos-bot/dashboard.html"
LOG_FILE = "/opt/bos-bot/bos_bot_v6.log"


@app.get("/")
def root():
    return {"status": "running"}


@app.get("/state")
def get_state():
    if not os.path.exists(STATE_FILE):
        return {"error": "no data yet"}

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


@app.get("/history")
def get_history(limit: int = 200):
    if not os.path.exists(HISTORY_FILE):
        return []

    rows = []
    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                pass

    return rows[-limit:]


@app.get("/logs")
def get_logs(lines: int = 100):
    if not os.path.exists(LOG_FILE):
        return {"lines": []}

    with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
        content = f.readlines()

    return {"lines": [x.rstrip("\n") for x in content[-lines:]]}


@app.get("/dashboard")
def dashboard():
    return FileResponse(DASHBOARD_FILE)
