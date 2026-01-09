from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import os
import traceback 
import database

import generate_leads
import enrich_leads
import generate_messages
import send_messages

app = FastAPI(title="Agentic Sales Bot API")

class LeadParams(BaseModel):
    num_leads: int = 10

class EnrichParams(BaseModel):
    mode: str = "offline" 

class SendParams(BaseModel):
    mode: str = "dry_run"

@app.on_event("startup")
def startup_event():
    """
    Server start aagum bodhu Database ah initialize pannidum.
    """
    try:
        database.init_db()
        print("API Startup: Database Initialized.")
    except Exception as e:
        print(f"API Startup Error: {e}")

@app.post("/generate-leads")
def api_generate_leads(params: LeadParams):
    """Step 1: Generate Leads into SQLite"""
    try:
        leads = generate_leads.generate_leads(params.num_leads)
        return {
            "status": "success", 
            "count": len(leads), 
            "message": f"Successfully generated {len(leads)} leads into Database."
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/enrich-leads")
def api_enrich_leads(params: EnrichParams):
    """Step 2: Enrich Leads (Reads from DB -> Updates DB)"""
    try:
        enrich_leads.enrich_data(mode=params.mode)
        return {
            "status": "success", 
            "mode": params.mode, 
            "message": f"Enrichment completed in {params.mode} mode."
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/generate-messages")
def api_generate_messages():
    """Step 3: Generate Messages (AI/Template -> DB)"""
    try:
        generate_messages.generate_messages()
        return {
            "status": "success", 
            "message": "Messages generated for all ENRICHED leads."
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/send-messages")
def api_send_messages(params: SendParams):
    """Step 4: Send Messages (DB -> SMTP -> Update DB)"""
    try:
        if params.mode == "live":
            print("[API] Requesting LIVE mode sending...")
            
        send_messages.process_sending(mode=params.mode)
        
        return {
            "status": "success", 
            "mode": params.mode, 
            "message": f"Messaging process finished in {params.mode} mode."
        }
    except Exception as e:
        print("CRITICAL ERROR IN SEND_MESSAGES:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/clear-logs")
def api_clear_logs():
    """Clears the log file without deleting it (Avoids Windows Error)"""
    try:
        with open("outreach.log", "w"):
            pass 
        return {"status": "success", "message": "Logs cleared successfully."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/status")
def api_status():
    """
    Get Pipeline Stats directly from SQLite.
    Replaces the old JSON counting logic.
    """
    try:
        conn = database.get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT status, COUNT(*) FROM leads GROUP BY status")
        rows = cursor.fetchall()
        
        stats = {"NEW": 0, "ENRICHED": 0, "MESSAGED": 0, "SENT": 0, "FAILED": 0, "SENT_DRY_RUN": 0}
        
        for row in rows:
            status_name = row['status']
            count = row[1]
            stats[status_name] = count
            
        conn.close()
        return stats
        
    except Exception as e:
        traceback.print_exc()
        return {"error": str(e)}

if __name__ == "__main__":
    print("API Server starting on http://0.0.0.0:8000")

    uvicorn.run(app, host="0.0.0.0", port=8000)
