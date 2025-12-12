import os
import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pathlib import Path
from datetime import datetime, timezone, timedelta

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

# API endpoint to fetch company data based on token
@app.get("/api/prefill")
async def get_prefill_data(t: str):
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()

    # Look up token in prefill_tokens
    cur.execute(
        """
        SELECT doc_number, expires_at, used
        FROM prefill_tokens
        WHERE token = %s
        """,
        (t,),
    )
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Invalid token")

    doc_number, expires_at, used = row
    # Check expiry
    now = datetime.now(timezone.utc)
    if expires_at < now:
        cur.close()
        conn.close()
        raise HTTPException(status_code=410, detail="Token expired")

    # Look up company by doc_number
    cur.execute(
        """
        SELECT name, city, status, filing_type
        FROM companies
        WHERE doc_number = %s
        """,
        (doc_number,),
    )
    company_row = cur.fetchone()
    cur.close()
    conn.close()

    if not company_row:
        raise HTTPException(status_code=404, detail="Company not found for this token")

    name, city, status, filing_type = company_row
    return {
        "doc": doc_number,
        "name": name,
        "city": city,
        "status": status,
        "filing_type": filing_type,
    }

# Database initialization: create tables and insert sample data
def init_db():
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    # Create companies table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            doc_number TEXT PRIMARY KEY,
            name TEXT,
            city TEXT,
            status TEXT,
            filing_type TEXT
        )
    """)
    # Create prefill_tokens table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS prefill_tokens (
            token TEXT PRIMARY KEY,
            doc_number TEXT,
            expires_at TIMESTAMPTZ,
            used BOOLEAN DEFAULT FALSE,
            FOREIGN KEY (doc_number) REFERENCES companies(doc_number)
        )
    """)
    # Insert sample company if not exists
    cur.execute(
        "INSERT INTO companies (doc_number, name, city, status, filing_type) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (doc_number) DO NOTHING",
        ('L23000013604', 'BHMS CONSULTING LLC', 'DELRAY BEACH', 'ACTIVE', 'Florida Limited Liability Company')
    )
    # Insert sample token if not exists
    cur.execute(
        "INSERT INTO prefill_tokens (token, doc_number, expires_at, used) VALUES (%s, %s, %s, %s) ON CONFLICT (token) DO NOTHING",
        ('demotoken123', 'L23000013604', datetime.now(timezone.utc) + timedelta(days=30), False)
    )
    conn.commit()
    cur.close()
    conn.close()

# Run database initialization at startup
@app.on_event("startup")
def startup_event():
    init_db()
