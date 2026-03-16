"""
Fetch Census ACS 2022 occupation demographics and match to occupations.csv.

Usage:
    uv run python ingest/fetch_census.py

Outputs:
    data/census_demographics.json

Sources:
    - B24114: Detailed Occupation for the Civilian Employed Population 16+
    - B24116: Detailed Occupation for the Civilian Employed Female Population 16+
    - B24121: Detailed Occupation by Median Earnings (Full-Time, Year-Round)

Strategy:
    1. Fetch all ~566 detailed occupation variables from three ACS tables.
    2. Build a lookup: occupation_label -> {total, female, median_earnings}.
    3. Fuzzy-match (token overlap) each of the 342 occupations in occupations.csv
       against the ACS labels.
    4. Only emit matches with confidence "high" (Jaccard >= 0.5) or "medium"
       (Jaccard >= 0.3). Anything below is dropped.
    5. Save results to data/census_demographics.json.
"""

import csv
import json
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY = "429e5b3c4aeff5946ee19fb92706388a1ac66f30"
BASE_URL = "https://api.census.gov/data/2022/acs/acs5"
ROOT = Path(__file__).parent.parent
OCCUPATIONS_CSV = ROOT / "occupations.csv"
OUTPUT_JSON = ROOT / "data" / "census_demographics.json"

# Census API hard limit: 50 variables per GET request (NAME counts as one).
# We use 49 data vars + NAME = 50 total per call.
MAX_VARS_PER_CALL = 49


# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_json(url: str, retries: int = 3) -> list:
    """GET a Census API URL, return parsed JSON. Retries on transient errors."""
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"HTTP {exc.code} fetching {url}") from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"Network error fetching {url}: {exc}") from exc


def fetch_group_variables(group: str) -> dict[str, str]:
    """
    Return {var_code: leaf_label} for all estimate variables in an ACS group.
    Leaf label = last segment after '!!' in the full label.
    """
    url = f"{BASE_URL}/groups/{group}.json"
    data = fetch_json(url)
    variables = data.get("variables", {})
    result: dict[str, str] = {}
    for code, meta in variables.items():
        if not code.endswith("E") or code in ("NAME",):
            continue
        label = meta.get("label", "")
        leaf = label.split("!!")[-1].strip().rstrip(":")
        result[code] = leaf
    return result


def fetch_table(group: str, var_codes: list[str]) -> dict[str, int | None]:
    """
    Fetch a Census ACS table for us:* geography.
    Returns {var_code: value} (value may be None if suppressed).
    Census limit: 500 variables per call. We batch if needed.
    """
    combined: dict[str, int | None] = {}

    # Batch into chunks of MAX_VARS_PER_CALL
    for start in range(0, len(var_codes), MAX_VARS_PER_CALL):
        chunk = var_codes[start : start + MAX_VARS_PER_CALL]
        get_param = ",".join(["NAME"] + chunk)
        url = f"{BASE_URL}?get={get_param}&for=us:1&key={API_KEY}"
        rows = fetch_json(url)

        if len(rows) < 2:
            continue

        header = rows[0]
        values = rows[1]  # single US-level row

        for col_name, val in zip(header, values):
            if col_name in chunk:
                try:
                    combined[col_name] = int(val) if val not in (None, "-1", "-666666666") else None
                except (ValueError, TypeError):
                    combined[col_name] = None

    return combined


# ── Normalisation for fuzzy matching ─────────────────────────────────────────

_STOPWORDS = frozenset({
    "and", "or", "the", "a", "an", "of", "for", "in", "to", "by",
    "at", "on", "other", "all", "related", "workers", "occupations",
    "except", "not", "elsewhere", "classified", "nec",
})


def tokenise(text: str) -> frozenset[str]:
    """Lower-case, strip punctuation, remove stop-words, return token set."""
    tokens = re.sub(r"[^a-z0-9 ]", " ", text.lower()).split()
    return frozenset(t for t in tokens if t not in _STOPWORDS and len(t) > 1)


def jaccard(a: frozenset, b: frozenset) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def confidence_label(score: float) -> str:
    if score >= 0.5:
        return "high"
    if score >= 0.3:
        return "medium"
    return "low"


# ── Main ──────────────────────────────────────────────────────────────────────

def load_occupations() -> list[dict]:
    """Load occupations.csv and return list of {slug, title, tokens}."""
    occupations = []
    with open(OCCUPATIONS_CSV, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            title = row.get("title", "").strip()
            slug = row.get("slug", "").strip()
            if title:
                occupations.append({
                    "slug": slug,
                    "title": title,
                    "tokens": tokenise(title),
                })
    return occupations


def build_acs_lookup() -> dict[str, dict]:
    """
    Fetch B24114 (total), B24116 (female), B24121 (median earnings).
    Return {var_suffix: {label, total, female, median_earnings}}.
    var_suffix is the numeric part (e.g. "002") shared across tables.
    """
    print("Fetching variable metadata for B24114 (total employed)...")
    total_vars = fetch_group_variables("B24114")   # {B24114_002E: "Chief executives", ...}

    print("Fetching variable metadata for B24116 (female employed)...")
    female_vars = fetch_group_variables("B24116")  # same structure, female counts

    print("Fetching variable metadata for B24121 (median earnings)...")
    earn_vars = fetch_group_variables("B24121")    # same structure, median $

    # Build suffix -> label map from B24114 (canonical occupation labels)
    # Skip the aggregate _001E total
    suffix_to_label: dict[str, str] = {}
    for code, label in total_vars.items():
        if code == "B24114_001E":
            continue
        suffix = code[len("B24114_"):]  # e.g. "002E"
        suffix_to_label[suffix] = label

    print(f"  Found {len(suffix_to_label)} occupation variables in B24114.")

    # Fetch actual counts from Census API
    total_codes = [f"B24114_{s}" for s in suffix_to_label]
    female_codes = [f"B24116_{s}" for s in suffix_to_label]
    earn_codes   = [f"B24121_{s}" for s in suffix_to_label]

    print(f"Fetching B24114 counts ({len(total_codes)} vars, may require batching)...")
    total_data = fetch_table("B24114", total_codes)

    print(f"Fetching B24116 counts ({len(female_codes)} vars)...")
    female_data = fetch_table("B24116", female_codes)

    print(f"Fetching B24121 median earnings ({len(earn_codes)} vars)...")
    earn_data = fetch_table("B24121", earn_codes)

    # Assemble lookup keyed by canonical label (lower-cased)
    lookup: dict[str, dict] = {}
    for suffix, label in suffix_to_label.items():
        total_val  = total_data.get(f"B24114_{suffix}")
        female_val = female_data.get(f"B24116_{suffix}")
        earn_val   = earn_data.get(f"B24121_{suffix}")

        lookup[label.lower()] = {
            "label": label,
            "total": total_val,
            "female": female_val,
            "median_earnings": earn_val,
            "tokens": tokenise(label),
        }

    return lookup


def match_occupations(
    occupations: list[dict],
    acs_lookup: dict[str, dict],
) -> list[dict]:
    """
    For each occupation in occupations.csv, find the best ACS label match.
    Only retain matches with Jaccard >= 0.3.
    """
    acs_entries = list(acs_lookup.values())
    results = []
    matched = 0

    for occ in occupations:
        occ_tokens = occ["tokens"]
        best_score = 0.0
        best_entry = None

        for entry in acs_entries:
            score = jaccard(occ_tokens, entry["tokens"])
            if score > best_score:
                best_score = score
                best_entry = entry

        conf = confidence_label(best_score)
        if conf == "low":
            continue  # skip low-confidence matches

        total = best_entry["total"]
        female = best_entry["female"]

        if total and total > 0 and female is not None:
            pct_female = round(female / total * 100, 1)
            pct_male = round(100.0 - pct_female, 1)
        else:
            pct_female = None
            pct_male = None

        result = {
            "slug": occ["slug"],
            "title": occ["title"],
            "acs_label": best_entry["label"],
            "match_score": round(best_score, 3),
            "match_confidence": conf,
            "total_employed": total,
            "pct_female": pct_female,
            "pct_male": pct_male,
            "median_earnings": best_entry["median_earnings"],
        }
        results.append(result)
        matched += 1

    return results, matched


def main():
    print("=" * 60)
    print("Census ACS 2022 Occupation Demographics Fetcher")
    print("=" * 60)

    print(f"\nLoading {OCCUPATIONS_CSV.name}...")
    occupations = load_occupations()
    print(f"  Loaded {len(occupations)} occupations.")

    print("\nBuilding ACS occupation lookup...")
    acs_lookup = build_acs_lookup()
    print(f"  ACS lookup contains {len(acs_lookup)} detailed occupations.")

    print("\nMatching occupations to ACS labels...")
    results, matched = match_occupations(occupations, acs_lookup)

    total = len(occupations)
    print(f"\nMatch results:")
    print(f"  Total occupations in CSV : {total}")
    print(f"  Matched (conf >= medium) : {matched}")
    print(f"  Match rate               : {matched/total*100:.1f}%")

    high = sum(1 for r in results if r["match_confidence"] == "high")
    med  = sum(1 for r in results if r["match_confidence"] == "medium")
    print(f"  High confidence          : {high}")
    print(f"  Medium confidence        : {med}")

    # Show a few sample matches
    print("\nSample matches:")
    for r in results[:5]:
        print(f"  '{r['title']}' -> '{r['acs_label']}' "
              f"(score={r['match_score']}, conf={r['match_confidence']}, "
              f"pct_female={r['pct_female']}%, earnings=${r['median_earnings']:,}"
              if r['median_earnings'] else
              f"  '{r['title']}' -> '{r['acs_label']}' "
              f"(score={r['match_score']}, conf={r['match_confidence']}, "
              f"pct_female={r['pct_female']}%, earnings=N/A)")

    # Save output
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    # Emit clean output (drop internal tokens field used for matching)
    clean_results = [
        {k: v for k, v in r.items() if k != "tokens"}
        for r in results
    ]

    with open(OUTPUT_JSON, "w", encoding="utf-8") as fh:
        json.dump(clean_results, fh, indent=2)

    print(f"\nSaved {len(clean_results)} records to {OUTPUT_JSON}")
    print("Done.")


if __name__ == "__main__":
    main()
