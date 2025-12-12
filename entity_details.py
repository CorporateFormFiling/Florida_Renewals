import json

# Corporate data file: fixed length 1440 chars per record
RECORD_LENGTH = 1440

def sl(line: str, start: int, length: int) -> str:
    """
    Slice helper: positions in the spec are 1-based.
    This converts them to Python's 0-based indexing.
    """
    return line[start - 1:start - 1 + length].strip()

# Officer blocks: up to 6 officers/managers
OFFICER_SPECS = [
    # Officer 1
    {"title": 669, "name": 674, "address": 716, "city": 758, "state": 786, "zip": 788},
    # Officer 2
    {"title": 797, "name": 802, "address": 844, "city": 886, "state": 914, "zip": 916},
    # Officer 3
    {"title": 925, "name": 930, "address": 972, "city": 1014, "state": 1042, "zip": 1044},
    # Officer 4
    {"title": 1053, "name": 1058, "address": 1100, "city": 1142, "state": 1170, "zip": 1172},
    # Officer 5
    {"title": 1181, "name": 1186, "address": 1228, "city": 1270, "state": 1298, "zip": 1300},
    # Officer 6
    {"title": 1309, "name": 1314, "address": 1356, "city": 1398, "state": 1426, "zip": 1428},
]

def parse_corporate_record(line: str) -> dict:
    """
    Parse a single 1440-char line from cordata.txt into a structured dict.
    Uses the official Corporate Data File definitions.
    """
    data = {
        "doc_number": sl(line, 1, 12),
        "name": sl(line, 13, 192),
        "status": sl(line, 205, 1),   # "A" or "I"
        "filing_type": sl(line, 206, 15),  # DOMP, FLAL, etc.
        "principal_address": {
            "address1": sl(line, 221, 42),
            "address2": sl(line, 263, 42),
            "city":     sl(line, 305, 28),
            "state":    sl(line, 333, 2),
            "zip":      sl(line, 335, 10),
            "country":  sl(line, 345, 2),
        },
        "mailing_address": {
            "address1": sl(line, 347, 42),
            "address2": sl(line, 389, 42),
            "city":     sl(line, 431, 28),
            "state":    sl(line, 459, 2),
            "zip":      sl(line, 461, 10),
            "country":  sl(line, 471, 2),
        },
        "file_date": sl(line, 473, 8),
        "fei_number": sl(line, 481, 14),
        "last_transaction_date": sl(line, 496, 8),
        "report_years": [
            {
                "year": sl(line, 506, 4),
                "date": sl(line, 511, 8),
            },
            {
                "year": sl(line, 519, 4),
                "date": sl(line, 524, 8),
            },
            {
                "year": sl(line, 532, 4),
                "date": sl(line, 537, 8),
            },
        ],
        "registered_agent": {
            "name":    sl(line, 545, 42),
            "type":    sl(line, 587, 1),   # P = person, C = corporation
            "address": sl(line, 588, 42),
            "city":    sl(line, 630, 28),
            "state":   sl(line, 658, 2),
            "zip":     sl(line, 660, 9),
        },
        "officers": [],
    }

    # Parse up to 6 officers / managers
    officers = []
    for spec in OFFICER_SPECS:
        name = sl(line, spec["name"], 42)
        if not name:
            continue  # no officer here
        officer = {
            "title":   sl(line, spec["title"], 4),      # raw title code
            "name":    name,
            "address": sl(line, spec["address"], 42),
            "city":    sl(line, spec["city"], 28),
            "state":   sl(line, spec["state"], 2),
            "zip":     sl(line, spec["zip"], 9),
        }
        officers.append(officer)

    data["officers"] = officers
    return data

def find_entity(doc_number: str, path: str = "cordata.txt") -> dict | None:
    """
    Scan the cordata.txt file line-by-line until we find the given doc_number.
    Returns the parsed dict, or None if not found.
    """
    target = doc_number.strip().upper()
    with open(path, "r", errors="ignore") as f:
        for line in f:
            if len(line) < 12:
                continue
            this_doc = sl(line, 1, 12).upper()
            if this_doc == target:
                return parse_corporate_record(line)
    return None

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python entity_details.py <DOCUMENT_NUMBER>")
        raise SystemExit(1)
    doc = sys.argv[1]
    record = find_entity(doc)
    if record is None:
        print("Not found")
    else:
        print(json.dumps(record, indent=2))
