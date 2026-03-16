"""
fetch_onet.py — Download and process O*NET occupation database to produce
skill/automation enrichment data per SOC code.

Downloads the O*NET 29.1 text database zip, extracts Work Activities and
Technology Skills files, computes cognitive/social/physical domain scores
and technology skill counts, then writes the results to:
    data/onet_enrichment.json

Usage:
    uv run python ingest/fetch_onet.py

Output schema per SOC (6-digit, e.g. "13-2011"):
    cognitive_score   float  0-10  average importance of cognitive work activities
    social_score      float  0-10  average importance of social work activities
    physical_score    float  0-10  average importance of physical work activities
    tech_skills_count int          distinct technology commodity titles
    hot_tech_count    int          subset flagged as Hot Technology
"""

import csv
import io
import json
import os
import shutil
import urllib.request
import zipfile
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ONET_ZIP_URL = "https://www.onetcenter.org/dl_files/database/db_29_1_text.zip"
ZIP_PATH = Path("/tmp/onet_db.zip")
EXTRACT_DIR = Path("/tmp/onet_db")
DB_DIR = EXTRACT_DIR / "db_29_1_text"

OUTPUT_PATH = Path("/mnt/i/opt-github/jobs/data/onet_enrichment.json")

# Domain keyword maps — matched against Element Name (case-insensitive)
DOMAIN_KEYWORDS = {
    "cognitive": [
        "information", "thinking", "processing", "analyzing", "reasoning",
        "decision", "evaluating", "documenting", "identifying",
    ],
    "social": [
        "communicating", "coordinating", "assisting", "resolving",
        "performing for", "establishing", "selling", "coaching", "staffing",
    ],
    "physical": [
        "physical", "handling", "operating", "controlling", "repairing",
        "inspecting", "moving", "monitoring processes",
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_soc(onet_code: str) -> str:
    """Strip the .XX suffix from an O*NET 8-digit code -> 6-digit SOC."""
    return onet_code.split(".")[0]


def classify_activity(element_name: str) -> str | None:
    """Return domain name for an activity element, or None if unclassified."""
    name_lower = element_name.lower()
    for domain, keywords in DOMAIN_KEYWORDS.items():
        for kw in keywords:
            if kw in name_lower:
                return domain
    return None


def download_zip() -> None:
    print(f"Downloading O*NET database from {ONET_ZIP_URL} ...")
    with urllib.request.urlopen(ONET_ZIP_URL, timeout=120) as response:
        data = response.read()
    ZIP_PATH.write_bytes(data)
    mb = len(data) / 1_048_576
    print(f"  Downloaded {mb:.1f} MB -> {ZIP_PATH}")


def extract_zip() -> None:
    print(f"Extracting to {EXTRACT_DIR} ...")
    EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(ZIP_PATH) as zf:
        zf.extractall(EXTRACT_DIR)
    print("  Extraction complete.")


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def parse_work_activities() -> dict[str, dict[str, list[float]]]:
    """
    Returns {soc_6: {"cognitive": [...IM scores], "social": [...], "physical": [...]}}.
    Original IM scale is 1-5; we store raw values and normalise to 0-10 at output time.
    """
    path = DB_DIR / "Work Activities.txt"
    print(f"Parsing {path.name} ...")

    # {soc_6: {domain: [im_values]}}
    scores: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    rows_read = rows_used = 0

    with path.open(encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            rows_read += 1
            if row.get("Scale ID", "").strip() != "IM":
                continue
            raw_val = row.get("Data Value", "").strip()
            if not raw_val:
                continue
            try:
                value = float(raw_val)
            except ValueError:
                continue
            soc = normalize_soc(row["O*NET-SOC Code"].strip())
            domain = classify_activity(row.get("Element Name", ""))
            if domain is None:
                continue
            scores[soc][domain].append(value)
            rows_used += 1

    print(f"  Rows read: {rows_read:,}  |  IM rows classified: {rows_used:,}  |  SOCs: {len(scores):,}")
    return scores


def parse_technology_skills() -> dict[str, dict[str, int]]:
    """
    Returns {soc_6: {"tech_skills_count": N, "hot_tech_count": M}}.
    """
    path = DB_DIR / "Technology Skills.txt"
    print(f"Parsing {path.name} ...")

    # Collect distinct commodity titles per SOC, and hot-tech count
    tech: dict[str, set[str]] = defaultdict(set)
    hot: dict[str, int] = defaultdict(int)
    rows_read = 0

    with path.open(encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        # Detect actual column names on first pass
        fieldnames = reader.fieldnames or []
        # Hot Technology column may appear twice; DictReader will suffix it
        # Typical columns: O*NET-SOC Code, Example, Hot Technology, Commodity Code,
        #                  Commodity Title, Hot Technology (second occurrence -> _1 or similar)
        hot_col = None
        for fn in reversed(fieldnames):          # last Hot Technology column
            if "hot technology" in fn.lower():
                hot_col = fn
                break
        title_col = next((f for f in fieldnames if "commodity title" in f.lower()), None)
        print(f"  Columns detected: {fieldnames}")
        print(f"  Using hot_col={hot_col!r}  title_col={title_col!r}")

        for row in reader:
            rows_read += 1
            soc = normalize_soc(row["O*NET-SOC Code"].strip())
            if title_col:
                title = row.get(title_col, "").strip()
                if title:
                    tech[soc].add(title)
            if hot_col and row.get(hot_col, "").strip().upper() == "Y":
                hot[soc] += 1

    print(f"  Rows read: {rows_read:,}  |  SOCs: {len(tech):,}")
    return {
        soc: {"tech_skills_count": len(titles), "hot_tech_count": hot.get(soc, 0)}
        for soc, titles in tech.items()
    }


# ---------------------------------------------------------------------------
# Merge & output
# ---------------------------------------------------------------------------

def build_enrichment(
    activity_scores: dict[str, dict[str, list[float]]],
    tech_data: dict[str, dict[str, int]],
) -> list[dict]:
    all_socs = set(activity_scores.keys()) | set(tech_data.keys())
    records = []

    for soc in sorted(all_socs):
        domains = activity_scores.get(soc, {})

        def avg_score(domain: str) -> float | None:
            vals = domains.get(domain)
            if not vals:
                return None
            # Original scale 1-5 -> normalise to 0-10: (mean - 1) / 4 * 10
            mean = sum(vals) / len(vals)
            return round((mean - 1) / 4 * 10, 2)

        td = tech_data.get(soc, {})
        record = {
            "soc_code": soc,
            "cognitive_score": avg_score("cognitive"),
            "social_score": avg_score("social"),
            "physical_score": avg_score("physical"),
            "tech_skills_count": td.get("tech_skills_count", 0),
            "hot_tech_count": td.get("hot_tech_count", 0),
        }
        records.append(record)

    return records


def write_output(records: list[dict]) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as fh:
        json.dump(records, fh, indent=2)
    print(f"\nWrote {len(records):,} records -> {OUTPUT_PATH}")


def print_summary(records: list[dict]) -> None:
    has_cognitive = sum(1 for r in records if r["cognitive_score"] is not None)
    has_social = sum(1 for r in records if r["social_score"] is not None)
    has_physical = sum(1 for r in records if r["physical_score"] is not None)
    has_tech = sum(1 for r in records if r["tech_skills_count"] > 0)
    avg_tech = sum(r["tech_skills_count"] for r in records) / max(len(records), 1)

    print("\n--- Summary ---")
    print(f"  Total SOC codes enriched : {len(records):,}")
    print(f"  With cognitive score     : {has_cognitive:,}")
    print(f"  With social score        : {has_social:,}")
    print(f"  With physical score      : {has_physical:,}")
    print(f"  With tech skills         : {has_tech:,}")
    print(f"  Avg tech skills per SOC  : {avg_tech:.1f}")

    # Show a sample record
    sample = next((r for r in records if r["cognitive_score"] is not None), records[0])
    print(f"\n  Sample record: {json.dumps(sample, indent=4)}")


def cleanup() -> None:
    print("\nCleaning up temporary files ...")
    if ZIP_PATH.exists():
        ZIP_PATH.unlink()
    if EXTRACT_DIR.exists():
        shutil.rmtree(EXTRACT_DIR)
    print("  Done.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    download_zip()
    extract_zip()

    try:
        activity_scores = parse_work_activities()
        tech_data = parse_technology_skills()
        records = build_enrichment(activity_scores, tech_data)
        write_output(records)
        print_summary(records)
    finally:
        cleanup()


if __name__ == "__main__":
    main()
