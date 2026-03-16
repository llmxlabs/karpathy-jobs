"""
Fetch BLS Occupational Employment and Wage Statistics (OEWS) data per SOC code.

Uses the BLS OES internal data tool API at data.bls.gov/OESServices/ —
the same backend powering https://data.bls.gov/oes/ — because:
  - BLS flat file zips (oesm24nat.zip, oesm24st.zip) return HTTP 403
  - BLS Public API v2 (api.bls.gov) does not serve OES series data
  - The OESServices REST API is public, unauthenticated, and returns JSON

Run with:
    uv run python ingest/fetch_oews.py

Output: data/oews_geographic.json
"""

import csv
import json
import sys
import time
from pathlib import Path

import httpx as requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://data.bls.gov/OESServices"
RELEASE_DATE = "2024A01"       # May 2024 release (latest available)
DATATYPE_EMP = "01"            # Employment
DATATYPE_WAGE = "04"           # Annual mean wage

HEADERS = {
    "Content-Type": "application/json",
    "Referer": "https://data.bls.gov/oes/",
    "User-Agent": "Mozilla/5.0 (compatible; jobs-data-pipeline/1.0)",
}

# OES state area codes: FIPS code * 100000, zero-padded to 7 digits.
# e.g. California FIPS 06 → "0600000", New York 36 → "3600000"
# Full list fetched dynamically from /OESServices/statesmultiselect
NATIONAL_AREA_CODE = "0000000"

ROOT = Path(__file__).parent.parent
OCCUPATIONS_CSV = ROOT / "occupations.csv"
OUTPUT_JSON = ROOT / "data" / "oews_geographic.json"

# Request pacing — OESServices is a public tool; be a polite client
REQUEST_DELAY_SECONDS = 0.25


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def soc_to_occ_code(soc: str) -> str:
    """Convert SOC format '13-2011' or '13-2011.00' to OES occupation code '132011'."""
    return soc.replace("-", "").replace(".", "")[:6]


def parse_value(raw: str) -> int | None:
    """Strip whitespace from OES value string and return int, or None if suppressed."""
    v = raw.strip()
    if not v or v in ("*", "#", "**"):
        return None
    try:
        return int(float(v))
    except ValueError:
        return None


def get_session() -> requests.Client:
    session = requests.Client(headers=HEADERS, follow_redirects=True)
    return session


# ---------------------------------------------------------------------------
# OESServices API calls
# ---------------------------------------------------------------------------

def fetch_all_states(session: requests.Client) -> list[dict]:
    """
    Return list of {areaCode, areaName} for all 50 states + DC.
    Excludes the synthetic 'All states in this list' entry (areaCode='xxxxxxx').
    """
    url = f"{BASE_URL}/statesmultiselect/areaTypeCode/S/occCode/000000"
    resp = session.get(url)
    resp.raise_for_status()
    states = [s for s in resp.json() if s["areaCode"] != "xxxxxxx"]
    return states


def fetch_release_date(session: requests.Client, occ_code: str) -> str | None:
    """Return the latest release date code for a given occupation, or None."""
    url = (
        f"{BASE_URL}/releasedates"
        f"/areaTypeCode/N"
        f"/areaCode/{NATIONAL_AREA_CODE}"
        f"/industryCode/000000"
        f"/occupationCode/{occ_code}"
        f"/datatype/{DATATYPE_EMP}"
    )
    resp = session.get(url)
    if resp.status_code != 200:
        return None
    data = resp.json()
    if not data:
        return None
    # First entry is the most recent
    return data[0].get("releaseDate", RELEASE_DATE).replace(" ", "").replace("-", "")


def _query_oesservices(
    session: requests.Client,
    area_type: str,
    area_codes: list[str],
    occ_code: str,
    release_date_code: str,
) -> list[dict]:
    """
    POST to /OESServices/resultsoccgeo and return the resultsOccGeoVO list.
    Returns [] on any error or non-data response.
    """
    payload = {
        "areaTypeCode": area_type,
        "areaCode": area_codes,
        "industryCode": "000000",
        "occupationCode": occ_code,
        "datatype": [DATATYPE_EMP, DATATYPE_WAGE],
        "releaseDateCode": [release_date_code],
        "outputType": "1",
    }
    try:
        resp = session.post(f"{BASE_URL}/resultsoccgeo", json=payload)
        resp.raise_for_status()
        body = resp.json()
        return body.get("resultsOccGeoVO") or []
    except Exception:
        return []


def fetch_national(
    session: requests.Client,
    occ_code: str,
    release_date_code: str,
) -> tuple[int | None, int | None]:
    """Return (national_employment, mean_annual_wage) for an occupation."""
    results = _query_oesservices(
        session, "N", [NATIONAL_AREA_CODE], occ_code, release_date_code
    )
    if not results:
        return None, None
    for result in results:
        for area in (result.get("areas") or []):
            if area["areaCode"] == NATIONAL_AREA_CODE:
                emp = wage = None
                for v in area.get("values", []):
                    if v["dataTypeCode"] == DATATYPE_EMP:
                        emp = parse_value(v["value"])
                    elif v["dataTypeCode"] == DATATYPE_WAGE:
                        wage = parse_value(v["value"])
                return emp, wage
    return None, None


def fetch_states_batch(
    session: requests.Client,
    occ_code: str,
    state_area_codes: list[str],
    release_date_code: str,
    batch_size: int = 20,
) -> dict[str, dict]:
    """
    Fetch employment + wage for all states in batches.
    Returns {areaCode: {employment, mean_wage}}.
    """
    results_map: dict[str, dict] = {}
    for i in range(0, len(state_area_codes), batch_size):
        batch = state_area_codes[i : i + batch_size]
        time.sleep(REQUEST_DELAY_SECONDS)
        results = _query_oesservices(session, "S", batch, occ_code, release_date_code)
        for result in results:
            for area in (result.get("areas") or []):
                code = area["areaCode"]
                emp = wage = None
                for v in area.get("values", []):
                    if v["dataTypeCode"] == DATATYPE_EMP:
                        emp = parse_value(v["value"])
                    elif v["dataTypeCode"] == DATATYPE_WAGE:
                        wage = parse_value(v["value"])
                if emp is not None or wage is not None:
                    results_map[code] = {"employment": emp, "mean_wage": wage}
    return results_map


# ---------------------------------------------------------------------------
# Core fips helper
# ---------------------------------------------------------------------------

def area_code_to_fips(area_code: str) -> str:
    """Convert OES area code '0600000' → FIPS '06'."""
    return str(int(area_code[:2])).zfill(2)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def load_soc_codes() -> list[str]:
    """Load unique SOC codes from occupations.csv."""
    soc_codes = []
    with open(OCCUPATIONS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get("soc_code", "").strip()
            if code:
                soc_codes.append(code)
    return soc_codes


def main() -> None:
    print("=== BLS OEWS Geographic Data Fetch ===")
    print(f"Source: {OCCUPATIONS_CSV}")
    print(f"Output: {OUTPUT_JSON}")
    print(f"API:    {BASE_URL}")
    print()

    session = get_session()

    # 1. Fetch all state area codes once
    print("Fetching state area codes...")
    states = fetch_all_states(session)
    state_by_code = {s["areaCode"]: s["areaName"] for s in states}
    all_state_codes = [s["areaCode"] for s in states]
    print(f"  Found {len(states)} states/territories")

    # 2. Load SOC codes
    soc_codes = load_soc_codes()
    print(f"  Loaded {len(soc_codes)} SOC codes from occupations.csv")
    print()

    output: list[dict] = []
    matched = 0
    skipped = 0

    for idx, soc in enumerate(soc_codes, 1):
        occ_code = soc_to_occ_code(soc)
        print(f"[{idx:3d}/{len(soc_codes)}] {soc} → {occ_code}", end="", flush=True)

        # 3a. Get national data
        time.sleep(REQUEST_DELAY_SECONDS)
        nat_emp, nat_wage = fetch_national(session, occ_code, RELEASE_DATE)

        if nat_emp is None and nat_wage is None:
            print(" — no national data, skipping")
            skipped += 1
            continue

        print(f"  emp={nat_emp:,}  wage=${nat_wage:,}" if nat_emp and nat_wage else "", end="")

        # 3b. Get state-level data
        time.sleep(REQUEST_DELAY_SECONDS)
        state_data = fetch_states_batch(
            session, occ_code, all_state_codes, RELEASE_DATE
        )

        # 3c. Compute top 3 states by employment
        ranked_states = sorted(
            [
                {
                    "state": state_by_code[code],
                    "fips": area_code_to_fips(code),
                    "employment": data["employment"],
                    "mean_wage": data["mean_wage"],
                }
                for code, data in state_data.items()
                if data.get("employment") is not None
            ],
            key=lambda x: x["employment"],
            reverse=True,
        )
        top_states = ranked_states[:3]

        # 3d. Location quotient range
        # LQ = (state_share_of_occ / state_share_of_total_emp)
        # Approximate using relative employment share across states
        # Since we don't have total state employment, use max/min of
        # state employment as a concentration proxy
        lq_max = None
        if nat_emp and ranked_states:
            state_emps = [s["employment"] for s in ranked_states if s["employment"]]
            if state_emps and nat_emp > 0:
                # Rough LQ: highest-state share vs lowest-state share ratio
                # (true LQ needs total state workforce denominators)
                max_share = max(state_emps) / nat_emp
                min_share = min(state_emps) / nat_emp
                # Normalise: if perfectly distributed among ~51 areas, share = 1/51 ≈ 0.0196
                avg_share = 1 / max(len(state_emps), 1)
                lq_max = round(max_share / avg_share, 2) if avg_share > 0 else None

        record = {
            "soc_code": soc,
            "national_employment": nat_emp,
            "mean_annual_wage": nat_wage,
            "top_states": top_states,
            "location_quotient_max": lq_max,
        }
        output.append(record)
        matched += 1
        print(f"  top={top_states[0]['state'] if top_states else 'n/a'}")

    # 4. Write output
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print()
    print("=== Summary ===")
    print(f"  Total SOC codes:    {len(soc_codes)}")
    print(f"  Matched with data:  {matched}")
    print(f"  Skipped (no data):  {skipped}")
    print(f"  Match rate:         {matched / len(soc_codes) * 100:.1f}%")
    print(f"  Output written to:  {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
