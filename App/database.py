import sqlite3
import json
import os

DB_NAME = "leads.db"

def get_db_connection():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row 
    return conn

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS leads (
            id TEXT PRIMARY KEY,
            full_name TEXT,
            company_name TEXT,
            role TEXT,
            industry TEXT,
            website TEXT,
            email TEXT,
            linkedin_url TEXT,
            country TEXT,
            status TEXT DEFAULT 'NEW',
            
            -- Enrichment Fields (Stored as JSON Strings)
            pain_points TEXT, 
            buying_triggers TEXT, 
            company_size TEXT,
            persona TEXT,
            confidence_score INTEGER,
            
            -- Message Fields (Stored as JSON Strings)
            generated_messages TEXT, 
            message_source TEXT
        )
    ''')
    
    conn.commit()
    conn.close()
    print(f"Database initialized: {DB_NAME}")

def row_to_dict(row):
    return dict(row)

if __name__ == "__main__":
    init_db()