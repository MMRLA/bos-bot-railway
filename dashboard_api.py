from fastapi import FastAPI
from fastapi.responses import FileResponse
import json
import os
import math

app = FastAPI()

STATE_FILE = "/opt/bos-bot/data/state.json"
HISTORY_FILE = "/opt/bos-bot/data/history.jsonl"
DASHBOARD_FILE = "/opt/bos-bot/dashboard.html"
LOG_FILE = "/opt/bos-bot/bos_bot_v6.log"


def sanitize_for_json(obj):
    """
    Convierte NaN / inf / -inf en None para que FastAPI pueda serializar JSON.
    Aplica recursivamente a dicts, listas, etc.
    """
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [sanitize_for_json(x) for x in obj]

    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj

    return obj


@app.get("/")
def root():
    return {"status": "running"}


@app.get("/state")
def get_state():
    if not os.path.exists(STATE_FILE):
        return {"error": "no data yet"}

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    return sanitize_for_json(data)


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
                row = json.loads(line)
                rows.append(sanitize_for_json(row))
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
