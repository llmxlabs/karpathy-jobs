"""
Process OECD Skills for Jobs CSV to extract US skill shortage/surplus data
and map it to BLS occupation categories.

Usage:
    uv run python ingest/process_oecd.py
"""

import csv
import json
import os
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
OECD_CSV = ROOT / "data" / "OECD,DF_S4J2022,+all.csv"
OUTPUT_JSON = ROOT / "data" / "oecd_skills.json"

# ── Thresholds ────────────────────────────────────────────────────────────────
SHORTAGE_THRESHOLD = 0.05
SURPLUS_THRESHOLD = -0.05

# ── High-AI-exposure BLS categories (for interpretation note) ─────────────────
HIGH_AI_EXPOSURE_CATEGORIES = {
    "computer-and-information-technology",
    "math",
    "media-and-communication",
    "office-and-administrative-support",
    "business-and-financial",
    "legal",
    "life-physical-and-social-science",
}

# ── BLS category definitions (slug → display name) ───────────────────────────
BLS_CATEGORIES = {
    "architecture-and-engineering": "Architecture and Engineering",
    "arts-and-design": "Arts and Design",
    "building-and-grounds-cleaning": "Building and Grounds Cleaning and Maintenance",
    "business-and-financial": "Business and Financial Operations",
    "community-and-social-service": "Community and Social Service",
    "computer-and-information-technology": "Computer and Information Technology",
    "construction-and-extraction": "Construction and Extraction",
    "education-training-and-library": "Education, Training, and Library",
    "entertainment-and-sports": "Entertainment and Sports",
    "farming-fishing-and-forestry": "Farming, Fishing, and Forestry",
    "food-preparation-and-serving": "Food Preparation and Serving Related",
    "healthcare": "Healthcare Practitioners and Technical",
    "healthcare-support": "Healthcare Support",
    "installation-maintenance-and-repair": "Installation, Maintenance, and Repair",
    "legal": "Legal",
    "life-physical-and-social-science": "Life, Physical, and Social Science",
    "management": "Management",
    "math": "Mathematical Science",
    "media-and-communication": "Media and Communication",
    "military": "Military",
    "office-and-administrative-support": "Office and Administrative Support",
    "personal-care-and-service": "Personal Care and Service",
    "production": "Production",
    "protective-service": "Protective Service",
    "sales": "Sales and Related",
    "transportation-and-material-moving": "Transportation and Material Moving",
}

# ── OECD skill → BLS category slug mapping ────────────────────────────────────
# Each entry: substring to match in skill name → list of BLS slugs
SKILL_CATEGORY_MAP = [
    # Digital / ICT
    ("computer programming",          ["computer-and-information-technology"]),
    ("digital content creation",      ["computer-and-information-technology", "media-and-communication", "arts-and-design"]),
    ("digital data processing",       ["computer-and-information-technology", "math"]),
    ("ict safety",                    ["computer-and-information-technology"]),
    ("office tools",                  ["office-and-administrative-support"]),
    ("web development",               ["computer-and-information-technology"]),
    ("digital skills",                ["computer-and-information-technology"]),
    ("telecommunications",            ["computer-and-information-technology", "installation-maintenance-and-repair"]),
    # Arts / Humanities
    ("fine arts",                     ["arts-and-design", "entertainment-and-sports"]),
    ("history and archaeology",       ["life-physical-and-social-science", "education-training-and-library"]),
    ("philosophy and theology",       ["life-physical-and-social-science", "education-training-and-library"]),
    ("arts and humanities",           ["arts-and-design", "education-training-and-library"]),
    # Business / Management
    ("clerical",                      ["office-and-administrative-support"]),
    ("customer and personal service", ["personal-care-and-service", "sales"]),
    ("sales and marketing",           ["sales", "business-and-financial"]),
    ("business processes",            ["business-and-financial", "management"]),
    # Attitudes / Soft skills
    ("adaptability",                  ["management", "community-and-social-service"]),
    ("motivation",                    ["management"]),
    ("self-management",               ["management"]),
    ("values",                        ["community-and-social-service"]),
    ("attitudes",                     ["management", "community-and-social-service"]),
    # Cognitive
    ("learning",                      ["education-training-and-library", "life-physical-and-social-science"]),
    ("originality",                   ["arts-and-design", "life-physical-and-social-science"]),
    ("quantitative abilities",        ["math"]),
    ("reasoning and problem-solving", ["math", "life-physical-and-social-science"]),
    ("cognitive skills",              ["math", "life-physical-and-social-science"]),
    # Communication
    ("active listening",              ["community-and-social-service", "education-training-and-library"]),
    ("communications and media",      ["media-and-communication"]),
    ("reading comprehension",         ["education-training-and-library", "media-and-communication"]),
    ("speaking",                      ["media-and-communication", "community-and-social-service"]),
    ("writing",                       ["media-and-communication", "office-and-administrative-support"]),
    ("communication skills",         ["media-and-communication"]),
    # Law / Safety
    ("law and government",            ["legal"]),
    ("public safety and security",    ["protective-service", "military"]),
    ("law and public safety",         ["legal", "protective-service"]),
    # Medicine / Health
    ("medicine and dentistry",        ["healthcare"]),
    ("psychology",                    ["healthcare", "community-and-social-service"]),
    ("medicine knowledge",            ["healthcare"]),
    # Physical / Motor
    ("auditory and speech",           ["healthcare-support", "community-and-social-service"]),
    ("physical abilities",            ["healthcare-support", "building-and-grounds-cleaning"]),
    ("psychomotor abilities",         ["production", "construction-and-extraction"]),
    ("physical skills",               ["building-and-grounds-cleaning", "construction-and-extraction"]),
    # Engineering / Construction / Trades
    ("building and construction",     ["construction-and-extraction", "building-and-grounds-cleaning"]),
    ("design",                        ["architecture-and-engineering", "arts-and-design"]),
    ("engineering, mechanics",        ["architecture-and-engineering", "installation-maintenance-and-repair"]),
    ("food production",               ["food-preparation-and-serving", "farming-fishing-and-forestry"]),
    ("installation and maintenance",  ["installation-maintenance-and-repair"]),
    ("production and processing",     ["production"]),
    ("quality control",               ["production", "architecture-and-engineering"]),
    ("transportation",                ["transportation-and-material-moving"]),
    ("technical skills",              ["architecture-and-engineering", "installation-maintenance-and-repair"]),
    # Science / Nature
    ("biology",                       ["life-physical-and-social-science", "farming-fishing-and-forestry"]),
    ("natural science",               ["life-physical-and-social-science"]),
    ("mathematics",                   ["math"]),
    ("science knowledge",             ["life-physical-and-social-science"]),
    # Social
    ("social perceptiveness",         ["community-and-social-service", "healthcare-support"]),
    ("social skills",                 ["community-and-social-service"]),
    ("instruction and teaching",      ["education-training-and-library"]),
    ("training and education",        ["education-training-and-library"]),
]


def label_status(shortage_index: float) -> str:
    if shortage_index > SHORTAGE_THRESHOLD:
        return "shortage"
    if shortage_index < SURPLUS_THRESHOLD:
        return "surplus"
    return "balanced"


def map_skill_to_categories(skill_name: str) -> list[str]:
    """Return list of BLS category slugs matching the given skill name."""
    skill_lower = skill_name.lower()
    matched = set()
    for keyword, slugs in SKILL_CATEGORY_MAP:
        if keyword in skill_lower:
            matched.update(slugs)
    return sorted(matched)


def build_interpretation(category_slug: str, shortage_index: float, status: str) -> str:
    is_high_ai = category_slug in HIGH_AI_EXPOSURE_CATEGORIES
    category_name = BLS_CATEGORIES.get(category_slug, category_slug)

    if status == "shortage" and is_high_ai:
        return (
            f"Workers in {category_name} have skills in shortage despite high AI exposure, "
            "suggesting demand currently outpaces automation displacement."
        )
    if status == "shortage":
        return (
            f"Workers in {category_name} have skills in shortage; "
            "demand exceeds supply in the US labor market."
        )
    if status == "surplus" and is_high_ai:
        return (
            f"Workers in {category_name} face a skill surplus compounded by high AI exposure, "
            "indicating elevated displacement risk."
        )
    if status == "surplus":
        return (
            f"Workers in {category_name} face a skill surplus; "
            "supply exceeds demand in the US labor market."
        )
    return (
        f"Workers in {category_name} show balanced skill supply and demand."
    )


def load_us_skills() -> list[dict]:
    """Parse OECD CSV and return all USA rows."""
    us_skills = []
    with open(OECD_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["LOCATION"] != "USA":
                continue
            try:
                obs = float(row["OBS_VALUE"])
            except (ValueError, KeyError):
                continue
            us_skills.append({
                "skill_code": row["SKILL"].strip(),
                "skill": row["Skills"].strip().lower(),
                "shortage_index": round(obs, 4),
                "status": label_status(obs),
            })
    return us_skills


def compute_category_balances(us_skills: list[dict]) -> list[dict]:
    """Compute weighted-average shortage_index per BLS category."""
    # accumulate: slug → (sum_of_values, list_of_skill_names)
    accum: dict[str, dict] = {}

    for s in us_skills:
        slugs = map_skill_to_categories(s["skill"])
        for slug in slugs:
            if slug not in accum:
                accum[slug] = {"total": 0.0, "count": 0, "skills": []}
            accum[slug]["total"] += s["shortage_index"]
            accum[slug]["count"] += 1
            accum[slug]["skills"].append(s["skill"])

    results = []
    for slug, data in sorted(accum.items()):
        avg = round(data["total"] / data["count"], 4)
        status = label_status(avg)
        results.append({
            "category": BLS_CATEGORIES.get(slug, slug),
            "category_slug": slug,
            "shortage_index": avg,
            "status": status,
            "relevant_skills": data["skills"],
            "interpretation": build_interpretation(slug, avg, status),
        })

    # Sort: shortages first, then balanced, then surpluses
    order = {"shortage": 0, "balanced": 1, "surplus": 2}
    results.sort(key=lambda x: (order[x["status"]], -x["shortage_index"]))
    return results


def print_summary(us_skills: list[dict], category_balances: list[dict]) -> None:
    print("=" * 60)
    print("OECD Skills for Jobs — US Analysis")
    print("=" * 60)
    print(f"\nTotal US skills processed: {len(us_skills)}")

    status_counts = {"shortage": 0, "balanced": 0, "surplus": 0}
    for s in us_skills:
        status_counts[s["status"]] += 1
    print(f"  Shortage  (index > +{SHORTAGE_THRESHOLD}): {status_counts['shortage']}")
    print(f"  Balanced  (index in +-{SHORTAGE_THRESHOLD}): {status_counts['balanced']}")
    print(f"  Surplus   (index < -{SURPLUS_THRESHOLD}): {status_counts['surplus']}")

    print(f"\nBLS categories with mapped skills: {len(category_balances)}")
    print("\n--- Category Skill Balance ---")
    for c in category_balances:
        bar = "+" if c["shortage_index"] > 0 else "-"
        print(
            f"  [{c['status']:8s}] {c['shortage_index']:+.4f}  {bar}  {c['category']}"
        )

    shortages = [c for c in category_balances if c["status"] == "shortage"]
    surpluses  = [c for c in category_balances if c["status"] == "surplus"]
    balanced   = [c for c in category_balances if c["status"] == "balanced"]
    print(f"\nSummary: {len(shortages)} shortage, {len(balanced)} balanced, {len(surpluses)} surplus categories")

    if shortages:
        print(f"\nHighest shortage:  {shortages[0]['category']}  ({shortages[0]['shortage_index']:+.4f})")
    if surpluses:
        worst = min(surpluses, key=lambda x: x["shortage_index"])
        print(f"Deepest surplus:   {worst['category']}  ({worst['shortage_index']:+.4f})")


def main() -> None:
    us_skills = load_us_skills()
    category_balances = compute_category_balances(us_skills)

    output = {
        "us_skills": us_skills,
        "category_skill_balance": [
            {k: v for k, v in c.items() if k != "category_slug"}
            for c in category_balances
        ],
    }

    os.makedirs(OUTPUT_JSON.parent, exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print_summary(us_skills, category_balances)
    print(f"\nOutput written to: {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
