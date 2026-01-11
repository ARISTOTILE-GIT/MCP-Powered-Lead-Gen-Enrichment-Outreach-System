import streamlit as st
import pandas as pd
import sqlite3
import requests
import time
import plotly.express as px
import plotly.graph_objects as go
import os

API_URL = "http://localhost:8000"
DB_PATH = "leads.db"
LOG_FILE = "outreach.log"

st.set_page_config(
    page_title="Agentic AI Pipeline Monitor",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
        .block-container {padding-top: 1rem;}
        div[data-testid="metric-container"] {
            background-color: #f0f2f6;
            padding: 10px;
            border-radius: 5px;
            border-left: 5px solid #ff4b4b;
        }
        div.stButton > button {
            width: 100%;
        }
        h1 {
            text-align: center; 
        }
    </style>
""", unsafe_allow_html=True)

def get_db_data():
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT * FROM leads", conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

with st.sidebar:
    st.header("⚙️ Pipeline Controls")
    
    st.subheader("1. Sending Mode")
    run_mode = st.radio(
        "Choose Action:",
        ["Dry Run (Test Only)", "Live Run (Send Emails)"],
        index=0,
        key="run_mode"
    )
    mode_value = "live" if "Live" in run_mode else "dry_run"
    
    st.divider()

    st.subheader("2. Enrichment Source")
    enrich_option = st.radio(
        "Choose Intelligence:",
        ["Offline (Rules - Fast)", "AI Agent (Groq LLM)"],
        index=0,
        key="enrich_mode"
    )
    enrich_mode = "ai" if "AI" in enrich_option else "offline"

    st.divider()
    

    st.subheader("3. Lead Generation")
    
    num_leads = st.number_input(
        "Count:", 
        min_value=1, 
        max_value=200, 
        value=10, 
        step=1
    )
    
    if st.button("Run Full Pipeline", type="primary"):
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            status_text.text(f"Generating {num_leads} Leads...")
            requests.post(f"{API_URL}/generate-leads", json={"num_leads": num_leads})
            progress_bar.progress(25)
            
            status_text.text(f"Enriching Leads ({enrich_mode})...")
            requests.post(f"{API_URL}/enrich-leads", json={"mode": enrich_mode})
            progress_bar.progress(50)
            
            status_text.text("Generating Messages...")
            requests.post(f"{API_URL}/generate-messages")
            progress_bar.progress(75)
            
            status_text.text(f"Sending Messages ({mode_value})...")
            requests.post(f"{API_URL}/send-messages", json={"mode": mode_value})
            progress_bar.progress(100)
            
            status_text.success("✅ Complete!")
            time.sleep(1)
            st.rerun()
            
        except Exception as e:
            status_text.error(f"Error: {e}")

    st.divider()
    
    st.subheader("🛠️ Manage Database") 
    
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("🔄 Refresh"):
            st.rerun()
            
    with col_b:
        if st.button("🗑️ Clear DB"):
            try:
                conn = sqlite3.connect(DB_PATH)
                conn.execute("DELETE FROM leads")
                conn.commit()
                conn.close()
                st.success("Cleared!")
            except Exception as e:
                st.error(f"Error: {e}")
            
            time.sleep(1)
            st.rerun()

    st.divider()
    
    st.subheader("Logs & Export")
    
    if st.button("💾 Download Leads (CSV)"):
        try:
            conn = sqlite3.connect(DB_PATH)
            csv_df = pd.read_sql_query("SELECT * FROM leads", conn)
            conn.close()
            csv_data = csv_df.to_csv(index=False).encode('utf-8')
            
            st.download_button(
                label="Click to Download CSV",
                data=csv_data,
                file_name="leads_export.csv",
                mime="text/csv"
            )
        except:
            st.error("No data to export")

    if st.button("🧹 Clear Logs"):
        try:
            response = requests.post(f"{API_URL}/clear-logs")
            if response.status_code == 200:
                st.success("Logs Wiped!")
            else:
                st.error(f"API Error: {response.text}")
        except:
            if os.path.exists(LOG_FILE):
                try:
                    open(LOG_FILE, "w").close()
                    st.success("Logs Cleared!")
                except:
                    st.error("Cannot clear logs.")
        
        time.sleep(1)
        st.rerun()


st.markdown("<h1 style='text-align: center;'>MCP Pipeline Monitor</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: gray;'>Autonomous Lead Generation & Outreach System</p>", unsafe_allow_html=True)
st.divider()

df = get_db_data()

if not df.empty:
    total_leads = len(df)
    enriched_count = len(df[df['status'] != 'NEW'])
    messaged_count = len(df[df['status'].isin(['MESSAGED', 'SENT', 'SENT_DRY_RUN'])])
    sent_count = len(df[df['status'].isin(['SENT', 'SENT_DRY_RUN'])])
    failed_count = len(df[df['status'] == 'FAILED'])
else:
    total_leads = 0
    enriched_count = 0
    messaged_count = 0
    sent_count = 0
    failed_count = 0

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Leads", total_leads)
col2.metric("Enriched", enriched_count)
col3.metric("Messaged", messaged_count)
col4.metric("Sent", sent_count)
col5.metric("Failed", failed_count)

st.divider()

st.subheader("📋 Lead Database")
if not df.empty:
    st.dataframe(
        df[["full_name", "company_name", "role", "industry", "status", "email"]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "status": st.column_config.TextColumn(
                "Status",
                validate="^(NEW|ENRICHED|MESSAGED|SENT|FAILED)$"
            ),
            "linkedin_url": st.column_config.LinkColumn("LinkedIn"),
            "email": st.column_config.LinkColumn("Email")
        }
    )
else:
    st.info("Database is empty. Use the sidebar to generate leads.")

st.divider()

if not df.empty:
    st.subheader("📊 Live Pipeline Analytics")
    
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        stages = ["Total Leads", "Enriched", "Generated Msgs", "Sent Successfully"]
        values = [total_leads, enriched_count, messaged_count, sent_count]
        
        fig_funnel = go.Figure(go.Funnel(
            y=stages,
            x=values,
            textinfo="value+percent initial",
            marker={"color": ["#A9A9A9", "#1E90FF", "#FFA500", "#32CD32"]}
        ))
        fig_funnel.update_layout(title_text="Conversion Funnel", height=400)
        st.plotly_chart(fig_funnel, use_container_width=True)

    with col_chart2:
        if "industry" in df.columns:
            fig_pie = px.pie(
                df, 
                names="industry", 
                title="Lead Distribution by Industry",
                hole=0.4
            )
            fig_pie.update_layout(height=400)
            st.plotly_chart(fig_pie, use_container_width=True)

else:
    st.caption("Charts will appear here once data is generated.")

st.divider()
st.subheader("📜 Recent Outreach Logs (Live)")

log_container = st.container(height=400, border=True)

with log_container:
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            lines = f.readlines()
            if lines:
                for line in reversed(lines):
                    st.text(line.strip())
            else:
                st.caption("Log file is empty. Waiting for pipeline activity...")
    else:
        st.info("No logs generated yet. Run the pipeline to see AI messages here.")