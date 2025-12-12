import os
import psycopg2
import psycopg2.pool
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pathlib import Path
from dotenv import load_dotenv
from entity_details import find_entity
from datetime import datetime, timezone

# 1) Load DB_DSN from environment (with local fallback)
DB_DSN = os.getenv(
    "DB_DSN",
    "dbname=sunbiz user=postgres password=yourpassword host=localhost port=5432"
)

# 2) Use DB_DSN for all connections
CONN_STR = DB_DSN

# 3) If you use a connection pool, define it AFTER DB_DSN exists
# (Only if your code actually has this—if not, skip)
try:
    POOL = psycopg2.pool.SimpleConnectionPool(minconn=1, maxconn=5, dsn=DB_DSN)
except Exception as e:
    print("Failed to initialize connection pool:", e)

# Serve the main frontend page (renewal flow)
@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    # renew.html is in the same folder as api.py
    index_path = Path("renew.html")
    if not index_path.exists():
        raise HTTPException(status_code=500, detail="renew.html not found")
    return HTMLResponse(content=index_path.read_text(encoding="utf-8"))


def q_conn():
    if POOL is None:
        raise RuntimeError("DB pool not initialized")
    return POOL.getconn()


def q_return(conn):
    POOL.putconn(conn)


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/entities/search")
def search(q: str, limit: int = 10):
    """
    Simple search endpoint:
    - if q looks like a document number, search by doc_number
    - otherwise search by name (case-insensitive)
    """
    q = q.strip()
    if not q:
        return []

    conn = q_conn()
    try:
        cur = conn.cursor()

        # Doc number search if it looks like one (starts with a letter + digits)
        is_doc = len(q) >= 6 and q[0].isalpha() and q[1:].replace(" ", "").isdigit()
        if is_doc:
            cur.execute(
                """
                SELECT doc_number, name, filing_type, status, city
                FROM companies
                WHERE doc_number ILIKE %s
                ORDER BY doc_number
                LIMIT %s
                """,
                (f"%{q}%", limit),
            )
        else:
            cur.execute(
                """
                SELECT doc_number, name, filing_type, status, city
                FROM companies
                WHERE lower(name) LIKE lower(%s)
                ORDER BY name
                LIMIT %s
                """,
                (f"%{q}%", limit),
            )

        rows = cur.fetchall()
        return [
            {
                "doc_number": r[0],
                "name": r[1],
                "filing_type": r[2],
                "status": "Active" if r[3] == "A" else "Inactive" if r[3] else None,
                "city": r[4],
            }
            for r in rows
        ]
    finally:
        q_return(conn)


@app.get("/api/prefill")
def get_prefill(t: str):
    """
    Look up a token in prefill_tokens and return company data for that doc_number.
    Currently returns: doc, name, city, status, filing_type.
    """
    conn = psycopg2.connect(CONN_STR)
    cur = conn.cursor()

    # 1) Find the token row
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

    # 2) Check expiry
    now = datetime.now(timezone.utc)
    if expires_at < now:
        cur.close()
        conn.close()
        raise HTTPException(status_code=410, detail="Token expired")

    # 3) Look up the company by doc_number
    cur.execute(
        """
        SELECT name,
               city,
               status,
               filing_type
        FROM companies
        WHERE doc_number = %s
        """,
        (doc_number,),
    )
    company_row = cur.fetchone()

    cur.close()
    conn.close()

    if not company_row:
        # Token is valid but we didn't find the company
        raise HTTPException(status_code=404, detail="Company not found for this token")

    name, city, status, filing_type = company_row

    # 4) Return data needed for prefill (we'll expand later)
    return {
        "doc": doc_number,
        "name": name,
        "city": city,
        "status": status,
        "filing_type": filing_type,
    }
