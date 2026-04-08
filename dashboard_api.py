from fastapi import FastAPI
import json
import os

from fastapi.responses import FileResponse

@app.get("/dashboard")
def dashboard():
    return FileResponse("dashboard.html")
