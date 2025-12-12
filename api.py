import os
import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pathlib import Path
from datetime import datetime, timezone

# Load database connection string from environment or use local fallback
DB_DSN = os.getenv("DB_DSN", "dbname=sunbiz user=postgres password=yourpassword host=localhost port=5432")

# Initialize FastAPI app
app = FastAPI()

# Configure CORS to allow requests from any origin (adjust as needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Helper function to get a new database connection
def get_db_conn():
    return psycopg2.connect(DB_DSN)

# Serve the main frontend (renewal form)
@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    # "renew.html" is expected to be in the same folder as api.py
    index_path = Path("renew.html")
    if not index_path.exists():
        raise HTTPException(status_code=500, detail="renew.html not found")
    return HTMLResponse(content=index_path.read_text(), status_code=200)

# Health check endpoint
@app.get("/api/health")
async def health_check():
    return {"status": "ok"}

# Prefill endpoint: returns company data based on a secure token
@app.get("/api/prefill")
async def get_prefill(t: str):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        # Look up token in prefill_tokens
        cur.execute(
            """
            SELECT doc_number, expires_at, used
            FROM prefill_tokens
            WHERE token = %s
            """,
            (t,)
        )
        token_row = cur.fetchone()
        if not token_row:
            raise HTTPException(status_code=404, detail="Invalid token")
        doc_number, expires_at, used = token_row
        
        # Check expiry
        if expires_at < datetime.now(timezone.utc):
            raise HTTPException(status_code=410, detail="Token expired")
        
        # Look up company data by doc_number
        cur.execute(
            """
            SELECT name, city, status, filing_type
            FROM companies
            WHERE doc_number = %s
            """,
            (doc_number,)
        )
        company_row = cur.fetchone()
        if not company_row:
            raise HTTPException(status_code=404, detail="Company not found for this token")
        name, city, status, filing_type = company_row
        
        # Return prefill data
        return {
            "doc": doc_number,
            "name": name,
            "city": city,
            "status": status,
            "filing_type": filing_type,
        }
    finally:
        cur.close()
        conn.close()
