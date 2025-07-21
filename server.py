from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException
from fastapi.responses import FileResponse
from typing import List
import json
import os
import re

app = FastAPI()

AUTHORIZED_USERS = {"definitelynotaspren", "trustedadmin1"}
FLAG_REASONS = [
    "Unusual activity",
    "Restricted airspace",
    "Data anomaly",
    "Public safety",
    "Possible privacy concern",
    "Possible residential address",
    "Other",
]

def get_user(user: str = Form(...)):
    if user not in AUTHORIZED_USERS:
        raise HTTPException(status_code=403, detail="Unauthorized")
    return user

def is_residential_address(address: str) -> bool:
    return bool(re.search(r"\d+\s+\w+\s+(Ave|Street|St|Rd|Boulevard|Blvd|Ln|Lane|Ct|Court|Dr|Drive)", address, re.I))

@app.post("/ingest")
async def ingest(
    user: str = Depends(get_user),
    files: List[UploadFile] = File([]),
    urls: str = Form(""),
    api_key: str = Form(""),
    flag_reason: str = Form("")
):
    output_public = "/tmp/public.geojson"
    output_private = "/tmp/private.geojson"
    audit_log_file = "/tmp/audit_log.json"
    entries_for_audit = []
    processed_entries = []

    for entry in processed_entries:
        if flag_reason == "Possible residential address" or is_residential_address(entry.get("address", "")):
            entries_for_audit.append(entry)
        else:
            pass

    if entries_for_audit:
        with open(audit_log_file, "a") as log:
            for entry in entries_for_audit:
                log.write(json.dumps({
                    "user": user,
                    "entry": entry,
                    "flag_reason": flag_reason
                }) + "\n")

    return {"public": output_public, "private": output_private, "audit": audit_log_file}

@app.get("/download/public")
def download_public():
    return FileResponse("/tmp/public.geojson", media_type="application/json", filename="public.geojson")

@app.get("/download/private")
def download_private(user: str):
    if user not in AUTHORIZED_USERS:
        return {"contact": "Request access to private data."}
    return FileResponse("/tmp/private.geojson", media_type="application/json", filename="private.geojson")

@app.get("/audit-log")
def audit_log(user: str):
    if user not in AUTHORIZED_USERS:
        raise HTTPException(status_code=403, detail="Unauthorized")
    if not os.path.exists("/tmp/audit_log.json"):
        return ""
    with open("/tmp/audit_log.json") as log:
        return log.read()
