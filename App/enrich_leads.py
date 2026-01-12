import json
import random
import time
import os
import database
from dotenv import load_dotenv

load_dotenv()

try:
    from groq import Groq
    GROQ_API_KEY = os.getenv("GROQ_API_KEY")
    client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
except ImportError:
    client = None

def determine_offline_persona(role):
    """Determine persona based on role keywords (Offline fallback)"""
    role_lower = role.lower()
    
    if any(x in role_lower for x in ['cto', 'engineering', 'tech', 'developer', 'data', 'architect']):
        return "Technical Decision Maker"
    elif any(x in role_lower for x in ['cfo', 'finance', 'treasurer', 'audit']):
        return "Financial Buyer"
    elif any(x in role_lower for x in ['ceo', 'founder', 'president', 'owner']):
        return "Executive Decision Maker"
    elif any(x in role_lower for x in ['marketing', 'cmo', 'brand']):
        return "Marketing Lead"
    elif any(x in role_lower for x in ['hr', 'people', 'talent']):
        return "HR Decision Maker"
    elif any(x in role_lower for x in ['manager', 'head', 'director', 'lead']):
        return "Department Head"
    else:
        return "Key Influencer"

def enrich_data(mode="offline"):
    print(f"Starting Enrichment (Mode: {mode})...")
    
    conn = database.get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM leads WHERE status='NEW'")
    rows = cursor.fetchall()
    
    if not rows:
        print("No NEW leads found to enrich.")
        conn.close()
        return

    print(f"Processing {len(rows)} leads...")
    
    for row in rows:
        lead = dict(row)
        lead_id = lead['id']
        industry = lead['industry']
        role = lead['role']
        company = lead['company_name']
        
        pain_points = []
        triggers = []
        
        persona = determine_offline_persona(role)
        
        company_size = random.choice(["Mid-Market", "Enterprise", "Startup"])
        confidence = random.randint(75, 98)

        if mode == "ai" and client:
            try:
                prompt = f"""
                Analyze this lead: Role: {role}, Industry: {industry}, Company: {company}.
                Return a valid JSON object with:
                - pain_points (list of 2 specific business challenges)
                - buying_triggers (list of 2 recent events indicating need)
                - persona (string, e.g., 'Technical Decision Maker', 'Financial Buyer', 'Operational Lead')
                """
                completion = client.chat.completions.create(
                    messages=[{"role": "user", "content": prompt}],
                    model="llama-3.3-70b-versatile",
                    response_format={"type": "json_object"}
                )
                
                data = json.loads(completion.choices[0].message.content)
                
                pain_points = data.get("pain_points", [])
                triggers = data.get("buying_triggers", [])
                
                
                if "persona" in data and data["persona"]:
                    persona = data["persona"]
                    
                print(f"AI Enriched: {lead['full_name']} ({persona})")
                time.sleep(1) 
                
            except Exception as e:
                print(f"AI Failed ({e}), falling back to Offline rules.")
                mode = "offline" 

        if not pain_points:
        
            
            if industry == "Technology":
                pain_points = ["Technical debt slowing down release cycles", "High cloud infrastructure costs"]
                triggers = ["Recent CTO hire", "Expanding engineering team"]
            elif industry == "Healthcare":
                pain_points = ["HIPAA compliance data silos", "Manual patient record processing"]
                triggers = ["New hospital wing opening", "Digitization initiative"]
            elif industry == "Finance":
                pain_points = ["Slow manual reconciliation processes", "Regulatory reporting errors"]
                triggers = ["Quarterly audit approaching", "Market expansion news"]
            elif industry == "Retail":
                pain_points = ["Inventory mismanagement", "Low customer retention rates"]
                triggers = ["Opening new store locations", "Holiday season approaching"]
            elif industry == "Manufacturing":
                pain_points = ["Supply chain disruptions", "Machine downtime impacting yield"]
                triggers = ["New factory launch", "Sustainability mandate"]
            else:
                pain_points = ["Operational inefficiencies", "Need for automation"]
                triggers = ["New leadership", "Cost cutting mandate"]
            
            print(f"Offline Enriched: {lead['full_name']} ({persona})")
            time.sleep(0.1)


        cursor.execute('''
            UPDATE leads 
            SET pain_points=?, 
                buying_triggers=?, 
                company_size=?, 
                persona=?, 
                confidence_score=?, 
                status='ENRICHED'
            WHERE id=?
        ''', (
            json.dumps(pain_points), 
            json.dumps(triggers), 
            company_size, 
            persona, 
            confidence, 
            lead_id
        ))

    conn.commit()
    conn.close()
    print("Enrichment Complete. Status updated to 'ENRICHED'.")

if __name__ == "__main__":
    enrich_data(mode="offline")