# 1. Introduction

# 🤖 Agentic Sales Bot: MCP-Powered Lead Gen & Outreach System

![Status](https://img.shields.io/badge/Assignment-Complete-green) ![Python](https://img.shields.io/badge/Python-3.10%2B-blue) ![Orchestration](https://img.shields.io/badge/Orchestration-n8n-orange) ![MCP](https://img.shields.io/badge/Protocol-MCP-purple)

A full-stack, autonomous sales pipeline built to satisfy the **MCP-Powered Lead Gen + Outreach** take-home assignment. It uses the **Model Context Protocol (MCP)** to expose tools, **n8n** as the agentic orchestrator, and **Streamlit** for real-time monitoring.

---

### 📋 Assignment Compliance Matrix

| Requirement | Implementation Details | Status |
| :--- | :--- | :--- |
| **1. Lead Gen** | Python `Faker` library generates valid leads with realistic roles/industries. Reproducible via seed. | ✅ Done |
| **2. Enrichment** | Hybrid System: **Offline Mode** (Rules) & **AI Mode** (Groq Llama-3) for pain points & confidence scores. | ✅ Done |
| **3. Personalization** | Generates unique Cold Emails & LinkedIn DMs (A/B variations) referencing enriched data. | ✅ Done |
| **4. Sending** | Supports **Dry Run** (simulated) and **Live Run** (Mock SMTP server) with rate limits. | ✅ Done |
| **5. Frontend** | **Streamlit** dashboard showing funnel metrics, logs, and queue status. | ✅ Done |
| **6. Orchestration** | **n8n** workflow orchestrates the entire pipeline by calling MCP tools via API. | ✅ Done |
| **7. MCP** | **FastAPI** server exposes `generate_leads`, `enrich_leads`, etc., as standard tools. | ✅ Done |

---

## 🏗️ System Architecture

The system follows the required **Micro-Tool Architecture** where n8n acts as the "Brain" (Agent) and Python acts as the "Hands" (Tools).

```mermaid
graph TD
    User["User / Dashboard"] -->|Trigger| n8n["n8n Orchestrator (Agent)"]
    n8n -->|HTTP Request| API["MCP Server (FastAPI)"]
    
    subgraph "MCP Tools (Python)"
    API -->|Tool| Gen[Lead Generator]
    API -->|Tool| Enrich["Lead Enricher (Groq/Rules)"]
    API -->|Tool| Draft[Message Drafter]
    API -->|Tool| Send[Outreach Sender]
    end
    
    Gen --> DB[("SQLite Database")]
    Enrich --> DB
    Draft --> DB
    Send --> DB
    DB --> User
