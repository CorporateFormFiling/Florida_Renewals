import psycopg2
import secrets
from datetime import datetime, timedelta, timezone

# === DB connection – same as in api.py / load_sunbiz.py ===
CONN_STR = "dbname=sunbiz user=postgres password=yourpassword host=localhost port=5432"

# === Base URL of your renewal page on Wix ===
BASE_PUBLIC_URL = "https://corporateformfiling.com/renew"

def create_prefill_token_for_doc(doc_number: str) -> str:
    """Create a secure token for a given doc_number and store it in prefill_tokens."""
    # 1) Generate a random URL-safe token
    token = secrets.token_urlsafe(32)

    # 2) Set expiry (e.g. 30 days from now)
    expires_at = datetime.now(timezone.utc) + timedelta(days=30)

    # 3) Insert into prefill_tokens
    conn = psycopg2.connect(CONN_STR)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO prefill_tokens (token, doc_number, expires_at, used)
        VALUES (%s, %s, %s, FALSE)
        """,
        (token, doc_number, expires_at),
    )

    cur.close()
    conn.close()

    # 4) Build the full URL you’ll email
    url = f"{BASE_PUBLIC_URL}?t={token}"
    return url

if __name__ == "__main__":
    doc = input("Enter doc_number (exactly as stored in the DB, e.g. L23000013604): ").strip()
    if not doc:
        print("No doc_number entered. Exiting.")
    else:
        link = create_prefill_token_for_doc(doc)
        print("\n✅ Token created and saved.")
        print("Send this URL in your email:\n")
        print(link)
