"""
Build a compact JSON for the website by merging CSV stats with AI exposure scores
and all enrichment datasets.

Sources merged:
  occupations.csv       — BLS OOH stats (pay, jobs, education, outlook)
  scores.json           — AI exposure scores (LLM-generated)
  data/onet_enrichment.json     — O*NET cognitive/social/physical skill scores + tech count
  data/oews_geographic.json     — BLS OEWS mean wage, top states per occupation
  data/census_demographics.json — Census ACS gender split + median earnings
  data/oecd_skills.json         — OECD US skill shortage index per BLS category

Usage:
    uv run python build_site_data.py
"""

import csv
import json
import os
from datetime import datetime, timezone


def load_json(path):
    if not os.path.exists(path):
        print(f"  [WARN] Missing enrichment file: {path}")
        return []
    with open(path) as f:
        return json.load(f)


def main():
    # ── Core sources ────────────────────────────────────────────────────
    with open("scores.json") as f:
        scores = {s["slug"]: s for s in json.load(f)}

    with open("occupations.csv") as f:
        rows = list(csv.DictReader(f))

    # ── Enrichment sources ───────────────────────────────────────────────
    # O*NET: keyed by soc_code
    onet_raw = load_json("data/onet_enrichment.json")
    onet = {r["soc_code"]: r for r in onet_raw}

    # OEWS geographic: keyed by soc_code
    oews_raw = load_json("data/oews_geographic.json")
    oews = {r["soc_code"]: r for r in oews_raw}

    # Census ACS: keyed by slug
    census_raw = load_json("data/census_demographics.json")
    census = {r["slug"]: r for r in census_raw}

    # OECD: category-level, keyed by category name
    # occupations.csv uses slug-style categories; map them to full names used in oecd_skills.json
    CAT_SLUG_TO_FULL = {
        "architecture-and-engineering":         "Architecture and Engineering",
        "arts-and-design":                       "Arts and Design",
        "building-and-grounds-cleaning":         "Building and Grounds Cleaning and Maintenance",
        "business-and-financial":                "Business and Financial Operations",
        "community-and-social-service":          "Community and Social Service",
        "computer-and-information-technology":   "Computer and Information Technology",
        "construction-and-extraction":           "Construction and Extraction",
        "education-training-and-library":        "Education, Training, and Library",
        "entertainment-and-sports":              "Entertainment and Sports",
        "farming-fishing-and-forestry":          "Farming, Fishing, and Forestry",
        "food-preparation-and-serving":          "Food Preparation and Serving Related",
        "healthcare":                            "Healthcare Practitioners and Technical",
        "installation-maintenance-and-repair":   "Installation, Maintenance, and Repair",
        "legal":                                 "Legal",
        "life-physical-and-social-science":      "Life, Physical, and Social Science",
        "management":                            "Management",
        "math":                                  "Mathematical Science",
        "media-and-communication":               "Media and Communication",
        "military":                              "Military",
        "office-and-administrative-support":     "Office and Administrative Support",
        "personal-care-and-service":             "Personal Care and Service",
        "production":                            "Production",
        "protective-service":                    "Protective Service",
        "sales":                                 "Sales and Related",
        "transportation-and-material-moving":    "Transportation and Material Moving",
    }

    oecd_data = load_json("data/oecd_skills.json")
    oecd_by_cat = {}
    if isinstance(oecd_data, dict):
        for cat_row in oecd_data.get("category_skill_balance", []):
            oecd_by_cat[cat_row["category"]] = cat_row

    # ── Merge ────────────────────────────────────────────────────────────
    data = []
    onet_hits = oews_hits = census_hits = oecd_hits = 0

    for row in rows:
        slug = row["slug"]
        soc  = row["soc_code"]
        score = scores.get(slug, {})
        o = onet.get(soc, {})
        g = oews.get(soc, {})
        c = census.get(slug, {})
        cat_full = CAT_SLUG_TO_FULL.get(row["category"], row["category"])
        q = oecd_by_cat.get(cat_full, {})

        if o: onet_hits += 1
        if g: oews_hits += 1
        if c: census_hits += 1
        if q: oecd_hits += 1

        entry = {
            # ── original fields ──────────────────────────────────────────
            "title":              row["title"],
            "slug":               slug,
            "category":           CAT_SLUG_TO_FULL.get(row["category"], row["category"]),
            "pay":                int(row["median_pay_annual"]) if row["median_pay_annual"] else None,
            "jobs":               int(row["num_jobs_2024"]) if row["num_jobs_2024"] else None,
            "outlook":            int(row["outlook_pct"]) if row["outlook_pct"] else None,
            "outlook_desc":       row["outlook_desc"],
            "education":          row["entry_education"],
            "exposure":           score.get("exposure"),
            "exposure_rationale": score.get("rationale"),
            "url":                row.get("url", ""),

            # ── O*NET skill profile ──────────────────────────────────────
            "cognitive_score":    o.get("cognitive_score"),
            "social_score":       o.get("social_score"),
            "physical_score":     o.get("physical_score"),
            "tech_skills_count":  o.get("tech_skills_count"),
            "hot_tech_count":     o.get("hot_tech_count"),

            # ── OEWS geographic ──────────────────────────────────────────
            "oews_mean_wage":     g.get("mean_annual_wage"),
            "top_states":         g.get("top_states"),        # [{state, fips, employment, mean_wage}]
            "lq_max":             g.get("location_quotient_max"),

            # ── Census ACS demographics ──────────────────────────────────
            "pct_female":         c.get("pct_female"),
            "pct_male":           c.get("pct_male"),
            "census_earnings":    c.get("median_earnings"),

            # ── OECD skill shortage (category-level) ─────────────────────
            "skill_shortage_index": round(q["shortage_index"], 3) if q.get("shortage_index") is not None else None,
            "skill_status":         q.get("status"),          # "shortage" | "surplus" | "balanced"
            "skill_interpretation": q.get("interpretation"),
        }
        data.append(entry)

    # ── Write ────────────────────────────────────────────────────────────
    os.makedirs("site", exist_ok=True)
    payload = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data": data,
    }
    with open("site/data.json", "w") as f:
        json.dump(payload, f, separators=(",", ":"))

    total_jobs = sum(d["jobs"] for d in data if d["jobs"])
    print(f"Wrote {len(data)} occupations to site/data.json")
    print(f"Total jobs represented: {total_jobs:,}")
    print(f"Enrichment hits — O*NET: {onet_hits}, OEWS: {oews_hits}, Census: {census_hits}, OECD: {oecd_hits}")


if __name__ == "__main__":
    main()
