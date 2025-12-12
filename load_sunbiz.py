import os, psycopg2

# --- DB connection (Postgres.app default) ---
CONN_STR = "dbname=sunbiz user=postgres password=yourpassword host=localhost port=5432"

# --- Fixed-width field positions (from Sunbiz layout) ---
SLICE = {
    "doc_number": (0, 12),
    "name":       (12, 162),
    "status":     (162, 163),
    "filing":     (163, 165),
    "city":       (509, 539),
}

def fw(line, key):
    a, b = SLICE[key]
    return line[a:b].strip()

conn = psycopg2.connect(CONN_STR)
cur  = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS companies (
  doc_number  TEXT PRIMARY KEY,
  name        TEXT,
  status      TEXT,
  filing_type TEXT,
  city        TEXT
);
""")
conn.commit()

rows = 0
batch = 0

with open("cordata.txt", "r", errors="ignore") as f:
    for line in f:
        doc = fw(line, "doc_number")
        if not doc:
            continue
        name   = fw(line, "name")
        status = fw(line, "status")
        filing = fw(line, "filing")
        city   = fw(line, "city")

        cur.execute("""
            INSERT INTO companies (doc_number, name, status, filing_type, city)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (doc_number) DO UPDATE
            SET name=EXCLUDED.name,
                status=EXCLUDED.status,
                filing_type=EXCLUDED.filing_type,
                city=EXCLUDED.city;
        """, (doc, name, status, filing, city))
        rows += 1
        batch += 1

        if batch >= 10000:  # commit every 10k
            conn.commit()
            batch = 0
        if rows % 100000 == 0:
            print(f"…loaded {rows:,} rows")

# final commit
conn.commit()

# helpful index for search speed
cur.execute("CREATE INDEX IF NOT EXISTS companies_name_idx ON companies (name);")
conn.commit()

cur.close()
conn.close()
print(f"✅ Done. Total rows processed: {rows:,}")




