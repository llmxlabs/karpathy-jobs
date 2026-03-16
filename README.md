# AI Exposure of the US Job Market

Analyzing how susceptible every occupation in the US economy is to AI and automation, using data from the Bureau of Labor Statistics [Occupational Outlook Handbook](https://www.bls.gov/ooh/) (OOH).

**Live demo: [ai-jobs-stats.llmxlabs.com](https://ai-jobs-stats.llmxlabs.com/)** — archived and extended version maintained by [LLMXLabs](https://llmxlabs.com) (the original karpathy.ai/jobs was taken down March 2026)

![AI Exposure Treemap](jobs.png)

## What's here

The BLS OOH covers **342 occupations** spanning every sector of the US economy, with detailed data on job duties, work environment, education requirements, pay, and employment projections. We scraped all of it, scored each occupation's AI exposure using an LLM, and built an interactive visualization with enriched data from four additional sources.

## Data pipeline

1. **Scrape** (`scrape.py`) — Playwright (non-headless, BLS blocks bots) downloads raw HTML for all 342 occupation pages into `html/`.
2. **Parse** (`parse_detail.py`, `process.py`) — BeautifulSoup converts raw HTML into clean Markdown files in `pages/`.
3. **Tabulate** (`make_csv.py`) — Extracts structured fields (pay, education, job count, growth outlook, SOC code) into `occupations.csv`.
4. **Score** (`score.py`) — Sends each occupation's Markdown description to an LLM (Gemini Flash via OpenRouter) with a scoring rubric. Each occupation gets an AI Exposure score from 0–10 with a rationale. Results saved to `scores.json`.
5. **Enrich O\*NET** (`ingest/onet.py`) — Joins on SOC code to add cognitive, social, and physical skill scores plus hot tech skill counts.
6. **Enrich OEWS** (`ingest/oews.py`) — Joins on SOC code to add top 3 states by employment and mean wage per occupation.
7. **Enrich Census ACS** (`ingest/census_acs.py`) — Joins on SOC code to add workforce gender split and median earnings.
8. **Enrich OECD** (`ingest/oecd.py`) — Joins on SOC code to add skill shortage/surplus/balanced signal and a one-line interpretation.
9. **Build site data** (`build_site_data.py`) — Merges CSV stats, AI exposure scores, and all enrichment data into a compact `site/data.json` for the frontend.
10. **Website** (`site/index.html`) — Interactive visualization with treemap, scatter plot, and table views.

## Data enrichment

Four external datasets are joined to `occupations.csv` on SOC code after the scoring step. Ingest scripts live in `ingest/` and cache raw data in `ingest/data/`.

### Sources

| Source | Vintage | Ingest script | What it adds |
|--------|---------|--------------|-------------|
| O\*NET 28.3 | Dec 2024 | `ingest/onet.py` | Cognitive / Social / Physical skill scores (0–10), hot tech skill count, total tech skills |
| BLS OEWS | 2023 survey (May 2024 release) | `ingest/oews.py` | Top 3 states by employment + mean wage per occupation |
| US Census ACS | 2022 5-year estimates | `ingest/census_acs.py` | % female, % male, median earnings per occupation |
| OECD Skills for Jobs | 2022 | `ingest/oecd.py` | Skill shortage / surplus / balanced signal + one-line interpretation |

### Coverage

| Source | Occupations matched |
|--------|-------------------|
| O\*NET | 238 / 342 (70%) |
| BLS OEWS | 285 / 342 (83%) |
| Census ACS | 311 / 342 (91%) |
| OECD | 342 / 342 (100%) |

## Key files

| File | Description |
|------|-------------|
| `occupations.json` | Master list of 342 occupations with title, URL, category, slug |
| `occupations.csv` | Summary stats: pay, education, job count, growth projections |
| `scores.json` | AI exposure scores (0–10) with rationales for all 342 occupations |
| `prompt.md` | All data in a single file, designed to be pasted into an LLM for analysis |
| `html/` | Raw HTML pages from BLS (source of truth, ~40MB) |
| `pages/` | Clean Markdown versions of each occupation page |
| `site/` | Static website (treemap, scatter, and table visualizations) |
| `ingest/onet.py` | Enriches occupations with O\*NET skill scores |
| `ingest/oews.py` | Enriches occupations with BLS OEWS state employment data |
| `ingest/census_acs.py` | Enriches occupations with Census ACS workforce demographics |
| `ingest/oecd.py` | Enriches occupations with OECD skill shortage/surplus signals |
| `ingest/data/` | Cached enrichment data files |

## AI exposure scoring

Each occupation is scored on a single **AI Exposure** axis from 0 to 10, measuring how much AI will reshape that occupation. The score considers both direct automation (AI doing the work) and indirect effects (AI making workers so productive that fewer are needed).

A key signal is whether the job's work product is fundamentally digital — if the job can be done entirely from a home office on a computer, AI exposure is inherently high. Conversely, jobs requiring physical presence, manual skill, or real-time human interaction have a natural barrier.

**Calibration examples from the dataset:**

| Score | Meaning | Examples |
|-------|---------|---------|
| 0–1 | Minimal | Roofers, janitors, construction laborers |
| 2–3 | Low | Electricians, plumbers, nurses aides, firefighters |
| 4–5 | Moderate | Registered nurses, retail workers, physicians |
| 6–7 | High | Teachers, managers, accountants, engineers |
| 8–9 | Very high | Software developers, paralegals, data analysts, editors |
| 10 | Maximum | Medical transcriptionists |

Average exposure across all 342 occupations: **5.3/10**.

## Visualization

The site offers multiple views of the same 342 occupations:

- **Treemap** — area proportional to employment, color indicates AI exposure (green to red), grouped by BLS category; click to zoom into a category, drag to pan
- **Scatter plot** — pay vs. AI exposure with category color coding
- **Table** — sortable list of all 342 occupations with inline search
- **Global fuzzy search** — find any occupation instantly across all views

Clicking any occupation opens a **detail panel** with enriched data displayed in organized cards:

- **O\*NET Skill Profile** — color-coded bars for Cognitive (blue), Social (green), and Physical (orange) skills scored 0–10, plus hot tech skill count
- **Workforce Demographics** — gender split bar (% female / % male) with median earnings from Census ACS
- **Top States by Employment** — top 3 states with employment count and mean wage from BLS OEWS
- **Skill Market Signal** — shortage / surplus / balanced badge with plain-English interpretation from OECD 2022

Additional toggle: **AI Robot perspective** re-scores all occupations by weighting physical robot capabilities, letting you compare standard AI exposure against robotic automation risk.

## LLM prompt

[`prompt.md`](prompt.md) packages all the data — aggregate statistics, tier breakdowns, exposure by pay/education, BLS growth projections, and all 342 occupations with their scores and rationales — into a single file (~45K tokens) designed to be pasted into an LLM. This lets you have a data-grounded conversation about AI's impact on the job market without needing to run any code. Regenerate it with `uv run python make_prompt.py`.

## Viewing the site locally

The `site/` directory is a fully self-contained static site — no build step required. Just serve it with any HTTP server.

**Mac/Linux:**
```bash
./start.sh
```

**Windows:**
```bat
start.bat
```

Both scripts start a Python HTTP server on port 8080 (override with `PORT=…`) and open your browser automatically.

Or manually:
```bash
cd site && python -m http.server 8080
```

## Vercel deployment

[LLMXLabs](https://llmxlabs.com) adapted this project for Vercel hosting. The setup is minimal because the site is already static:

- **`vercel.json`** sets `outputDirectory` to `site/` and uses `echo` as a no-op build command (the pre-built `site/data.json` is committed alongside `index.html`)
- No Node.js, no bundler — Vercel just serves `site/` as-is
- To deploy your own fork: install the [Vercel CLI](https://vercel.com/docs/cli), run `vercel` from the repo root, and it will pick up `vercel.json` automatically

## Setup (data pipeline)

```
uv sync
uv run playwright install chromium
```

Requires an OpenRouter API key in `.env`:
```
OPENROUTER_API_KEY=your_key_here
```

## Usage (data pipeline)

```bash
# Scrape BLS pages (only needed once, results are cached in html/)
uv run python scrape.py

# Generate Markdown from HTML
uv run python process.py

# Generate CSV summary
uv run python make_csv.py

# Score AI exposure (uses OpenRouter API)
uv run python score.py

# Enrich with O*NET, OEWS, Census ACS, OECD (run after make_csv.py)
uv run python ingest/onet.py
uv run python ingest/oews.py
uv run python ingest/census_acs.py
uv run python ingest/oecd.py

# Build website data
uv run python build_site_data.py
```
