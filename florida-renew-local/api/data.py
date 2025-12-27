import duckdb
import re
import html
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

# =========================
# CONFIG
# =========================

CORPDATA = Path("/Users/michaelstockamore/florida_renewals/corpdata_with_emails.parquet")

DOC_COL = "document_number"
LINE_COL = "corp_line"
EMAIL_COL = "email"

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI",
    "MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT",
    "VT","VA","WA","WV","WI","WY","DC"
}

TYPE_TOKENS = {"IFLAL","AFLAL","IDOMNP","IDOMN","IDOMP","IDOM","DOMNP","DOMP"}

ROLE_TOKENS = {
    "MGR","AMBR","MEMBER","PRES","VP","VPP","TREA","SEC","DIR","CEO","CFO","COO",
    "MANAGER","AUTHORIZED","TRUSTEE","CHAIR","P","S","D"
}

ADDR2_KEYWORDS = {"SUITE","STE","UNIT","APT","#","FLOOR","FL","BLDG","BUILDING","RM","ROOM"}

STREET_SUFFIX = {
    "AVE","AVE.","AVENUE","BLVD","BLVD.","BOULEVARD","RD","RD.","ROAD",
    "ST","ST.","STREET","DR","DR.","DRIVE","CT","CT.","COURT",
    "LN","LN.","LANE","WAY","HWY","HWY.","PKWY","PKWY.", "TER", "TER.",
    "CIR","CIR."
}

# =========================
# DB
# =========================

def get_con():
    return duckdb.connect(database=":memory:")

def register_tables(con):
    if not CORPDATA.exists():
        raise FileNotFoundError(CORPDATA)
    con.execute(f"""
        CREATE VIEW corpdata AS
        SELECT * FROM read_parquet('{CORPDATA.as_posix()}')
    """)

# =========================
# HELPERS
# =========================

def _clean(s: Optional[str]) -> str:
    return (s or "").strip()

def _normalize_ws(s: str) -> str:
    s = (s or "").strip()

    # Decode HTML entities (e.g., &amp;)
    s = html.unescape(s)

    # Split glued "P1401" -> "P 1401" (critical for registered agent blocks)
    s = re.sub(r"\bP(?=\d)", "P ", s)

    # Fix glued ".FT" etc: "INC.FT" -> "INC. FT"
    s = re.sub(r"\.(?=(FT|ST|N|S|E|W)\b)", ". ", s)

    # Split leading flag glued to date: N10312001 -> N 10312001
    s = re.sub(r"\b([A-Z])(\d{8})\b", r"\1 \2", s)

    # Split date glued to FL year: 10312001FL2025 -> 10312001 FL2025
    s = re.sub(r"\b(\d{8})FL(\d{4})\b", r"\1 FL\2", s)

    # US glued to street number: US2160 -> US 2160
    s = re.sub(r"\bUS(?=\d)", "US ", s)

    # State+zip glued: FL33308 -> FL 33308
    s = re.sub(r"\b([A-Z]{2})(\d{5})(\b)", r"\1 \2", s)

    # date+ein glued: 0105202392-1582122 -> 01052023 92-1582122
    s = re.sub(r"\b(\d{8})(\d{2}-\d{7})\b", r"\1 \2", s)

    # date+FEI9 glued: 07122001141838235 -> 07122001 141838235
    s = re.sub(r"\b(\d{8})(\d{9})\b", r"\1 \2", s)

    # date+year glued: 041620252025 -> 04162025 2025
    s = re.sub(r"\b(\d{8})(\d{4})\b", r"\1 \2", s)

    # date+letters glued: 04042024HALPERIN -> 04042024 HALPERIN
    s = re.sub(r"\b(\d{8})([A-Z])", r"\1 \2", s)

    # commas spacing
    s = re.sub(r",(?=\S)", ", ", s)

    # collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _tokenize(s: str) -> List[str]:
    return _normalize_ws(s).split(" ") if s else []

def _looks_like_zip(t: str) -> bool:
    return bool(re.fullmatch(r"\d{5}(-\d{4})?", t))

def _looks_like_date(t: str) -> bool:
    return bool(re.fullmatch(r"\d{8}", t))

def _looks_like_ein(t: str) -> bool:
    return bool(re.fullmatch(r"\d{2}-\d{7}", t))

def _looks_like_fei9(t: str) -> bool:
    return bool(re.fullmatch(r"\d{9}", t))

def _looks_like_year(t: str) -> bool:
    return bool(re.fullmatch(r"\d{4}", t))

def _looks_like_street_number(t: str) -> bool:
    return bool(re.fullmatch(r"\d{1,6}", t))

def _normalize_person_token(tok: str) -> str:
    # PHALPERIN / PSTOCKAMORE => HALPERIN / STOCKAMORE
    if re.fullmatch(r"P[A-Z]{3,}", tok):
        return tok[1:]
    return tok

def _split_addr(addr_tokens: List[str]) -> Tuple[str, Optional[str]]:
    if not addr_tokens:
        return "", None

    upper = [t.upper().strip(",") for t in addr_tokens]
    split_idx = None
    for i in range(1, len(upper)):
        if upper[i] in ADDR2_KEYWORDS:
            split_idx = i
            break
        # Handle "2ND FLOOR" where "FLOOR" is keyword
        if i + 1 < len(upper) and upper[i + 1] in {"FLOOR", "FL"}:
            split_idx = i
            break

    if split_idx is None:
        return " ".join(addr_tokens).strip(), None

    return " ".join(addr_tokens[:split_idx]).strip(), " ".join(addr_tokens[split_idx:]).strip()

def _find_state_zip(tokens: List[str], start: int, window: int = 120) -> Optional[int]:
    n = len(tokens)
    for j in range(start, min(n - 1, start + window)):
        if tokens[j] in US_STATES and _looks_like_zip(tokens[j + 1]):
            return j
    return None

def _parse_address(tokens: List[str], start: int) -> Tuple[Optional[Dict[str, Any]], int]:
    state_idx = _find_state_zip(tokens, start, window=140)
    if state_idx is None:
        return None, start

    state = tokens[state_idx]
    zipc = tokens[state_idx + 1]
    pre = tokens[start:state_idx]

    def is_city_token(t: str) -> bool:
        tu = t.upper().strip(",")
        if tu in ADDR2_KEYWORDS:
            return False
        if tu in STREET_SUFFIX:
            return False
        if any(ch.isdigit() for ch in t):
            return False
        return True

    # City = last 1–3 city-ish tokens
    city_parts: List[str] = []
    k = len(pre) - 1
    while k >= 0 and len(city_parts) < 3:
        if not is_city_token(pre[k]):
            break
        city_parts.append(pre[k].strip(","))
        k -= 1
    city_parts.reverse()
    city = " ".join(city_parts).strip()

    addr_tokens = pre[:k+1] if city_parts else pre
    address1, address2 = _split_addr(addr_tokens)

    next_i = state_idx + 2
    country = None
    if next_i < len(tokens) and tokens[next_i] in {"US", "USA"}:
        country = "US"
        next_i += 1

    out: Dict[str, Any] = {
        "address1": address1,
        "city": city,
        "state": state,
        "zip": zipc,
    }
    if address2:
        out["address2"] = address2
    if country:
        out["country"] = country

    return out, next_i

def _clean_and_flip_agent_name(parts: List[str]) -> Optional[str]:
    """
    Takes tokens that *should* be the agent name, removes junk (dates/years/FL####/flags),
    and flips LAST FIRST [MIDDLE] -> FIRST [MIDDLE] LAST.
    """
    cleaned: List[str] = []
    for t in parts:
        tu = t.upper()
        if tu in {"N", "Y"}:
            continue
        if _looks_like_date(t) or _looks_like_year(t) or _looks_like_ein(t) or _looks_like_fei9(t):
            continue
        if re.fullmatch(r"FL\d{4}", tu):
            continue
        if re.fullmatch(r"[A-Z]{1,2}\d{6,}", tu):  # weird blobs
            continue
        cleaned.append(_normalize_person_token(t))

    if not cleaned:
        return None

    # If it includes non-person-ish tokens (like "LAW", "GROUP"), don’t flip.
    # For JOGMEL, it’s purely a person name, so we flip.
    if any(x.upper() in {"LLC", "INC", "INC.", "CORP", "CORPORATION", "COMPANY", "GROUP", "LAW"} for x in cleaned):
        return " ".join(cleaned).strip() or None

    if len(cleaned) >= 2:
        last = cleaned[0]
        rest = cleaned[1:]
        return " ".join(rest + [last]).strip()
    return cleaned[0].strip()

# =========================
# CORE PARSER
# =========================

def parse_corp_line(doc: str, corp_line: str) -> Dict[str, Any]:
    raw = _normalize_ws(corp_line)
    doc_clean = _clean(doc)

    s = raw
    if doc_clean and s.upper().startswith(doc_clean.upper()):
        s = s[len(doc_clean):].strip()

    # entity name + type token
    m_type = None
    type_tok = None
    for tok in TYPE_TOKENS:
        m = re.search(rf"\b{re.escape(tok)}\b", s)
        if m and (m_type is None or m.start() < m_type.start()):
            m_type = m
            type_tok = tok

    if m_type:
        entity_name = s[:m_type.start()].strip()
        remainder = s[m_type.start():].strip()
        entity_type_code = type_tok
        remainder = re.sub(rf"^\b{re.escape(type_tok)}\b\s*", "", remainder)
    else:
        entity_name = s[:80].strip()
        remainder = s[len(entity_name):].strip()
        entity_type_code = None

    tokens = _tokenize(remainder)

    # 1) principal + mailing
    principal, idx = _parse_address(tokens, 0)
    mailing, idx = _parse_address(tokens, idx if principal else 0)

    # 2) filing facts
    formation_date = None
    fei_ein = None
    annual_report_year = None
    report_dates: List[str] = []
    j = idx

    while j < len(tokens) and not _looks_like_date(tokens[j]):
        j += 1
    if j < len(tokens) and _looks_like_date(tokens[j]):
        formation_date = tokens[j]
        j += 1

    if j < len(tokens) and (_looks_like_ein(tokens[j]) or _looks_like_fei9(tokens[j])):
        fei_ein = tokens[j]
        j += 1

    # flags like N
    if j < len(tokens) and re.fullmatch(r"[A-Z]", tokens[j]):
        j += 1

    # swallow one extra date if present (e.g., dissolution/other)
    if j < len(tokens) and _looks_like_date(tokens[j]):
        j += 1

    if j < len(tokens):
        m = re.fullmatch(r"FL(\d{4})", tokens[j])
        if m:
            annual_report_year = m.group(1)
            j += 1

    while j < len(tokens):
        if re.fullmatch(r"C\d{1,6}", tokens[j]) or tokens[j] in ROLE_TOKENS:
            break
        if _looks_like_date(tokens[j]):
            report_dates.append(tokens[j])
            j += 1
            continue
        if _looks_like_year(tokens[j]):
            j += 1
            continue
        break

    # 3) registered agent
    registered_agent = None

    # (A) C#### format
    c_idx = None
    for k in range(j, len(tokens)):
        if re.fullmatch(r"C\d{1,6}", tokens[k]):
            c_idx = k
            break

    if c_idx is not None:
        ra_name = _clean_and_flip_agent_name(tokens[j:c_idx])
        ra_tokens = tokens[:]
        ra_tokens[c_idx] = ra_tokens[c_idx][1:]
        ra_addr, new_j = _parse_address(ra_tokens, c_idx)
        registered_agent = {"name": ra_name, "address": ra_addr}
        j = new_j
    else:
        # (B) P marker format: RA NAME ... P <address>
        p_idx = None
        for k in range(j, min(len(tokens), j + 160)):
            if tokens[k] == "P":
                p_idx = k
                break
        if p_idx is not None:
            ra_name = _clean_and_flip_agent_name(tokens[j:p_idx])
            ra_addr, new_j = _parse_address(tokens, p_idx + 1)
            if ra_addr:
                registered_agent = {"name": ra_name, "address": ra_addr}
                j = new_j

    # 4) officers
    officers: List[Dict[str, Any]] = []
    i = j
    while i < len(tokens):
        if tokens[i] in ROLE_TOKENS:
            role = tokens[i]
            i += 1

            if i < len(tokens) and tokens[i] == "P":
                i += 1

            name_parts: List[str] = []
            while i < len(tokens):
                if tokens[i] in ROLE_TOKENS or re.fullmatch(r"C\d{1,6}", tokens[i]):
                    break
                if _looks_like_date(tokens[i]) or _looks_like_year(tokens[i]) or _looks_like_ein(tokens[i]) or _looks_like_fei9(tokens[i]):
                    break
                if _looks_like_street_number(tokens[i]):
                    break
                name_parts.append(_normalize_person_token(tokens[i]))
                i += 1
                if len(name_parts) >= 14:
                    break

            # flip LAST FIRST [MIDDLE] -> FIRST [MIDDLE] LAST
            name = None
            if name_parts:
                if len(name_parts) >= 2:
                    last = name_parts[0]
                    rest = name_parts[1:]
                    name = " ".join(rest + [last]).strip()
                else:
                    name = name_parts[0].strip()

            addr = None
            if i < len(tokens) and _looks_like_street_number(tokens[i]):
                addr, i = _parse_address(tokens, i)
            else:
                addr_try, new_i = _parse_address(tokens, i)
                if addr_try:
                    addr = addr_try
                    i = new_i

            officers.append({"role": role, "name": name, "address": addr})
            continue

        i += 1

    return {
        "document_number": doc_clean,
        "entity_name": entity_name,
        "entity_type_code": entity_type_code,
        "principal_address": principal,
        "mailing_address": mailing,
        "formation_date": formation_date,
        "fei_ein": fei_ein,
        "annual_report_year": annual_report_year,
        "report_dates": report_dates,
        "registered_agent": registered_agent,
        "officers": officers,
        "raw_line": raw,
    }

# =========================
# API QUERIES
# =========================

def search_entities(con, q, limit=10):
    q = _clean(q)
    limit = max(1, min(int(limit or 10), 50))

    rows = con.execute(f"""
        SELECT {DOC_COL},{LINE_COL}
        FROM corpdata
        WHERE lower(CAST({LINE_COL} AS VARCHAR)) LIKE '%'||lower(?)||'%'
           OR lower(CAST({DOC_COL} AS VARCHAR)) LIKE '%'||lower(?)||'%'
        LIMIT ?
    """, [q, q, limit]).fetchall()

    out = []
    for d, l in rows:
        p = parse_corp_line(d, l)
        out.append({
            "document_number": p["document_number"],
            "entity_name": p["entity_name"],
            "entity_type_code": p.get("entity_type_code"),
            "principal_city": (p.get("principal_address") or {}).get("city"),
            "principal_state": (p.get("principal_address") or {}).get("state")
        })

    q_up = q.upper()
    def _rank(item):
        d = (item.get("document_number") or "").upper()
        n = (item.get("entity_name") or "").upper()
        return (0 if d == q_up else 1, 0 if n.startswith(q_up) else 1, n)

    out.sort(key=_rank)
    return out[:limit]

def get_entity_by_doc(con, doc):
    doc = _clean(doc)
    row = con.execute(f"""
        SELECT {DOC_COL},{LINE_COL},{EMAIL_COL}
        FROM corpdata
        WHERE upper(CAST({DOC_COL} AS VARCHAR)) = upper(?)
        LIMIT 1
    """, [doc]).fetchone()

    if not row:
        return None

    d, l, e = row
    p = parse_corp_line(d, l)
    p["email"] = _clean(e) or None
    return p
