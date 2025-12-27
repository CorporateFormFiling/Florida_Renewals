from fastapi import FastAPI, Query
import duckdb
import re

DB_PATH = "api.duckdb"
app = FastAPI()

# Common Florida entity suffixes (end of name). Tweak as you like.
_SUFFIXES = {
    "LLC", "L.L.C", "INC", "I.N.C", "CORP", "C.O.R.P", "CORPORATION",
    "CO", "C.O", "COMPANY", "LTD", "L.T.D", "LIMITED",
    "LP", "L.P", "LLP", "L.L.P", "LLLP", "L.L.L.P",
    "PA", "P.A", "PLLC", "P.L.L.C", "PL", "P.L",
    "PC", "P.C", "PROFESSIONAL", "ASSOCIATION", "P A",
}

_punct_re = re.compile(r"[^A-Z0-9\s]")
_ws_re = re.compile(r"\s+")


def connect():
    con = duckdb.connect(DB_PATH, read_only=True)
    con.execute("PRAGMA threads=2;")
    con.execute("PRAGMA memory_limit='1GB';")
    # FTS macros required for /search; keep server alive even if extension load fails
    try:
        con.execute("LOAD fts;")
    except Exception:
        pass
    return con


def clean(v):
    if v is None:
        return None
    v = str(v).strip()
    return v if v else None


def fmt_mmddyyyy(s: str):
    """Convert MMDDYYYY -> YYYY-MM-DD. Return None if blank/invalid."""
    s = clean(s)
    if not s or len(s) != 8 or not s.isdigit():
        return None
    mm, dd, yyyy = s[0:2], s[2:4], s[4:8]
    return f"{yyyy}-{mm}-{dd}"



def split_zip(z: str):
    """Return zip5/zip4, preserving raw (digits only)."""
    z = clean(z)
    if not z:
        return {"zip5": None, "zip4": None, "raw": None}
    z2 = z.replace("-", "").strip()
    if not z2:
        return {"zip5": None, "zip4": None, "raw": None}
    if len(z2) >= 9 and z2[:9].isdigit():
        return {"zip5": z2[:5], "zip4": z2[5:9], "raw": z2[:9]}
    if len(z2) >= 5 and z2[:5].isdigit():
        return {"zip5": z2[:5], "zip4": None, "raw": z2[:5]}
    # if weird non-numeric, keep raw trimmed
    return {"zip5": z2[:5], "zip4": None, "raw": z2}


def normalize_business_name(name: str):
    """
    Normalize a business name for display/search UX:
    - uppercase
    - remove punctuation
    - collapse whitespace
    - remove trailing entity suffix tokens (LLC, INC, P.A., etc.)
    """
    name = clean(name)
    if not name:
        return None

    s = name.upper()
    s = _punct_re.sub(" ", s)
    s = _ws_re.sub(" ", s).strip()
    if not s:
        return None

    parts = s.split()

    # Remove trailing suffixes iteratively
    while parts:
        last = parts[-1]
        if last in _SUFFIXES:
            parts.pop()
            continue

        # Handle 2-token suffix like "P A"
        if len(parts) >= 2:
            last2 = f"{parts[-2]} {parts[-1]}"
            if last2 in _SUFFIXES:
                parts = parts[:-2]
                continue
        break

    out = " ".join(parts).strip()
    return out if out else s

def looks_like_doc_number(q: str):
    """
    Florida-style doc numbers:
    - Start with letter(s) OR digits
    - Length usually 6–14
    - No spaces
    Examples: L24000146720, P20000095500, 341791
    """
    q = clean(q)
    if not q:
        return False

    q = q.upper()
    if " " in q:
        return False

    if len(q) < 6 or len(q) > 14:
        return False

    return q.isalnum()


def split_person_name(raw: str):
    raw = clean(raw)
    if not raw:
        return {"full": None, "first": None, "middle": None, "last": None, "suffix": None}

    suffixes = {"JR", "SR", "II", "III", "IV", "V"}

    has_comma = "," in raw
    s = raw.replace(",", " ").strip()
    parts = [p for p in s.split() if p]

    suffix = None
    if parts:
        last_token = parts[-1].upper().strip(".")
        if last_token in suffixes:
            suffix = parts.pop(-1)

    if not parts:
        return {"full": raw, "first": None, "middle": None, "last": None, "suffix": suffix}

    # If original had comma, assume "LAST, FIRST M"
    if has_comma and len(parts) >= 2:
        last = parts[0]
        first = parts[1]
        middle = " ".join(parts[2:]) if len(parts) > 2 else None
        return {"full": raw, "first": first, "middle": middle, "last": last, "suffix": suffix}

    # Otherwise assume "FIRST M LAST"
    if len(parts) == 1:
        return {"full": raw, "first": parts[0], "middle": None, "last": None, "suffix": suffix}
    if len(parts) == 2:
        return {"full": raw, "first": parts[0], "middle": None, "last": parts[1], "suffix": suffix}

    return {"full": raw, "first": parts[0], "middle": " ".join(parts[1:-1]), "last": parts[-1], "suffix": suffix}


def addr_obj(prefix: str, d: dict):
    z = split_zip(d.get(f"{prefix}_zip"))
    return {
        "address1": clean(d.get(f"{prefix}_addr1")),
        "address2": clean(d.get(f"{prefix}_addr2")),
        "city": clean(d.get(f"{prefix}_city")),
        "state": clean(d.get(f"{prefix}_state")),
        "zip": z["zip5"],
        "zip4": z["zip4"],
        "zip_raw": z["raw"],
        "country": clean(d.get(f"{prefix}_country")),
    }


def best_address(d: dict):
    m = addr_obj("mail", d)
    p = addr_obj("principal", d)
    return m if m.get("address1") else p


def display_subtitle(d: dict):
    a = best_address(d)
    parts = [a.get("city"), a.get("state")]
    parts = [p for p in parts if p]
    return ", ".join(parts) if parts else None


def ra_obj(d: dict):
    z = split_zip(d.get("ra_zip"))
    return {
        "name": clean(d.get("ra_name")),
        "type": clean(d.get("ra_type")),
        "address": clean(d.get("ra_addr")),
        "city": clean(d.get("ra_city")),
        "state": clean(d.get("ra_state")),
        "zip": z["zip5"],
        "zip4": z["zip4"],
        "zip_raw": z["raw"],
    }


def officers_list(d: dict):
    out = []
    for i in range(1, 7):
        raw_name = clean(d.get(f"officer_{i}_name"))
        title = clean(d.get(f"officer_{i}_title"))
        typ = clean(d.get(f"officer_{i}_type"))
        addr = clean(d.get(f"officer_{i}_addr"))
        city = clean(d.get(f"officer_{i}_city"))
        state = clean(d.get(f"officer_{i}_state"))
        z = split_zip(d.get(f"officer_{i}_zip"))

        if any([raw_name, title, typ, addr, city, state, z["raw"]]):
            nm = split_person_name(raw_name)
            out.append(
                {
                    "index": i,
                    "title": title,
                    "type": typ,
                    "name": nm,  # {full, first, middle, last, suffix}
                    "address": addr,
                    "city": city,
                    "state": state,
                    "zip": z["zip5"],
                    "zip4": z["zip4"],
                    "zip_raw": z["raw"],
                }
            )
    return out


def build_prefill_payload(row, cols):
    d = dict(zip(cols, row))
    raw_name = clean(d.get("name"))

    payload = {
        "document_number": clean(d.get("document_number")),
        "name": raw_name,
        "name_normalized": normalize_business_name(raw_name),
        "display_name": normalize_business_name(raw_name) or raw_name,
        "display_subtitle": display_subtitle(d),

        "status": clean(d.get("status")),
        "filing_type": clean(d.get("filing_type")),
        "email": clean(d.get("email")),

        "principal_address": addr_obj("principal", d),
        "mailing_address": addr_obj("mail", d),
        "best_address": best_address(d),
        "registered_agent": ra_obj(d),

        "file_date_raw": clean(d.get("file_date")),
        "file_date": fmt_mmddyyyy(d.get("file_date")),
        "fei_number": clean(d.get("fei_number")),
        "last_txn_date_raw": clean(d.get("last_txn_date")),
        "last_txn_date": fmt_mmddyyyy(d.get("last_txn_date")),
        "state_country": clean(d.get("state_country")),
        "more_than_6_officers": clean(d.get("more_than_6_officers")),

        "annual_reports": [
            {"year": clean(d.get("report_year_1")), "date_raw": clean(d.get("report_date_1")), "date": fmt_mmddyyyy(d.get("report_date_1"))},
            {"year": clean(d.get("report_year_2")), "date_raw": clean(d.get("report_date_2")), "date": fmt_mmddyyyy(d.get("report_date_2"))},
            {"year": clean(d.get("report_year_3")), "date_raw": clean(d.get("report_date_3")), "date": fmt_mmddyyyy(d.get("report_date_3"))},
        ],

        "officers": officers_list(d),
    }

    payload["annual_reports"] = [x for x in payload["annual_reports"] if x["year"] or x["date_raw"]]
    return payload


@app.get("/by-doc/{doc}")
def by_doc(doc: str):
    con = connect()
    try:
        row = con.execute(
            """
            SELECT
              document_number,
              name,
              status,
              filing_type,

              principal_addr1, principal_addr2, principal_city, principal_state, principal_zip, principal_country,
              mail_addr1, mail_addr2, mail_city, mail_state, mail_zip, mail_country,

              ra_name, ra_type, ra_addr, ra_city, ra_state, ra_zip,

              file_date, fei_number, last_txn_date,
              state_country, more_than_6_officers,

              report_year_1, report_date_1,
              report_year_2, report_date_2,
              report_year_3, report_date_3,

              officer_1_title, officer_1_type, officer_1_name, officer_1_addr, officer_1_city, officer_1_state, officer_1_zip,
              officer_2_title, officer_2_type, officer_2_name, officer_2_addr, officer_2_city, officer_2_state, officer_2_zip,
              officer_3_title, officer_3_type, officer_3_name, officer_3_addr, officer_3_city, officer_3_state, officer_3_zip,
              officer_4_title, officer_4_type, officer_4_name, officer_4_addr, officer_4_city, officer_4_state, officer_4_zip,
              officer_5_title, officer_5_type, officer_5_name, officer_5_addr, officer_5_city, officer_5_state, officer_5_zip,
              officer_6_title, officer_6_type, officer_6_name, officer_6_addr, officer_6_city, officer_6_state, officer_6_zip,

              email
            FROM corp
            WHERE document_number = ?
            LIMIT 1
            """,
            [doc],
        ).fetchone()

        if not row:
            return {"found": False}

        cols = [d[0] for d in con.description]
        data = dict(zip(cols, row))

        raw_name = clean(data.get("name"))
        data["name_normalized"] = normalize_business_name(raw_name)

        # zip/date helpers for flat endpoint too (optional convenience)
        data["file_date_fmt"] = fmt_yyyymmdd(data.get("file_date"))
        data["last_txn_date_fmt"] = fmt_yyyymmdd(data.get("last_txn_date"))

        return {"found": True, "data": data}
    finally:
        con.close()


@app.get("/search")
def search(q: str = Query(..., min_length=2), limit: int = 25):
    con = connect()
    try:
        rows = con.execute(
            """
            SELECT
              document_number,
              name,
              status,
              filing_type,
              email,
              fts_main_corp.match_bm25(document_number, ?) AS score
            FROM corp
            WHERE score IS NOT NULL
            ORDER BY score DESC
            LIMIT ?
            """,
            [q, limit],
        ).fetchall()

        return [
            {
                "document_number": r[0],
                "name": r[1],
                "name_normalized": normalize_business_name(r[1]),
                "display_name": normalize_business_name(r[1]) or r[1],
                "status": r[2],
                "filing_type": r[3],
                "email": r[4],
                "score": r[5],
            }
            for r in rows
        ]
    finally:
        con.close()


@app.get("/prefill/by-doc/{doc}")
def prefill_by_doc(doc: str, raw: bool = False):
    con = connect()
    try:
        row = con.execute(
            """
            SELECT *
            FROM corp
            WHERE document_number = ?
            LIMIT 1
            """,
            [doc],
        ).fetchone()

        if not row:
            return {"found": False}

        cols = [d[0] for d in con.description]
        payload = build_prefill_payload(row, cols)

        if raw:
            payload_raw = dict(zip(cols, row))
            return {"found": True, "data": payload, "raw": payload_raw}

        return {"found": True, "data": payload}
    finally:
        con.close()


@app.get("/prefill/search")
def prefill_search(q: str = Query(..., min_length=2), limit: int = 10, raw: bool = False):
    con = connect()
    try:
        q_clean = clean(q)

        # 🚀 FAST PATH: document number
        if looks_like_doc_number(q_clean):
            row = con.execute(
                "SELECT * FROM corp WHERE document_number = ? LIMIT 1",
                [q_clean.upper()],
            ).fetchone()

            if not row:
                return {"found": False, "results": [], "best": None}

            cols = [d[0] for d in con.description]
            payload = build_prefill_payload(row, cols)

            result_stub = {
                "document_number": payload["document_number"],
                "name": payload["name"],
                "name_normalized": payload["name_normalized"],
                "display_name": payload["display_name"],
                "status": payload["status"],
                "filing_type": payload["filing_type"],
                "email": payload["email"],
                "score": 9999,
            }

            if raw:
                return {
                    "found": True,
                    "results": [result_stub],
                    "best": payload,
                    "best_raw": dict(zip(cols, row)),
                }

            return {
                "found": True,
                "results": [result_stub],
                "best": payload,
            }

        # 🔍 NORMAL FTS SEARCH
        rows = con.execute(
            """
            SELECT
              document_number,
              name,
              status,
              filing_type,
              email,
              fts_main_corp.match_bm25(document_number, ?) AS score
            FROM corp
            WHERE score IS NOT NULL
            ORDER BY score DESC
            LIMIT ?
            """,
            [q_clean, limit],
        ).fetchall()

        results_payload = [
            {
                "document_number": r[0],
                "name": r[1],
                "name_normalized": normalize_business_name(r[1]),
                "display_name": normalize_business_name(r[1]) or r[1],
                "status": r[2],
                "filing_type": r[3],
                "email": r[4],
                "score": r[5],
            }
            for r in rows
        ]

        if not results_payload:
            return {"found": False, "results": [], "best": None}

        best_doc = results_payload[0]["document_number"]
        best_row = con.execute(
            "SELECT * FROM corp WHERE document_number = ? LIMIT 1",
            [best_doc],
        ).fetchone()

        cols = [d[0] for d in con.description]
        best_payload = build_prefill_payload(best_row, cols) if best_row else None

        if raw and best_row:
            return {
                "found": True,
                "results": results_payload,
                "best": best_payload,
                "best_raw": dict(zip(cols, best_row)),
            }

        return {"found": True, "results": results_payload, "best": best_payload}
    finally:
        con.close()

