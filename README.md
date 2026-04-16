# Weekly Metrics Dashboard

A Streamlit dashboard that automates the weekly marketing metrics call for [Maxim AI](https://getmaxim.ai) / [Bifrost](https://getbifrost.ai). Pulls data from Google Search Console, Google Analytics (GA4), SEMrush, and Profound to generate SEO and GEO analysis with LLM-powered insights. Data is stored in PostgreSQL.

## Setup

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

Copy the example env file and fill in your values:

```bash
cp .env.example .env
```

| Variable | Description |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string (e.g. Neon: `postgresql://user:pass@ep-xxx.neon.tech/db?sslmode=require`) |
| `OPENAI_API_KEY` | OpenAI API key for LLM insight summaries and call summary generation |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Path to a Google Cloud service account JSON key file |
| `GSC_PROPERTY` | Google Search Console property (e.g. `sc-domain:example.com`) |
| `GA4_PROPERTY_ID` | GA4 property ID (numeric) |

`DATABASE_URL` is required. GSC/GA4 variables are independent — if not configured, sections fall back to CSV file upload. OpenAI key is optional (enables AI insights).

### Database setup

Run migrations to create the tables:

```bash
uv run alembic upgrade head
```

## Usage

```bash
uv run streamlit run main.py
```

The sidebar lets you switch between four sections:

1. **Search Impressions (GSC)** — Impressions by page category, weekly/monthly trends, period-over-period % changes, pages appearing in search
2. **Traffic Analytics (GA4)** — Sessions by source and medium, page category breakdown, per-source drill-down, GEO (AI-referred) traffic with % changes
3. **Keyword Performance** — SEMrush Position Tracking rank data, daily positions, result type distribution, landing page categorization
4. **GEO Performance (Profound)** — Prompt appearance rates across ChatGPT, AI Overview, and Perplexity; cross-platform overlap, owned articles cited, competitor mentions

### Data flow

- **GSC + GA4**: Click "Fetch GSC + GA4 Data" on the Search Impressions or Traffic Analytics page to pull data via Google APIs. Data is upserted into PostgreSQL. Fetch windows are aligned to ISO weeks (Mon-Sun), and the app checks if data already exists before calling APIs.
- **Keywords + Profound**: Upload CSVs through the file uploader in each section. Data is parsed and inserted into the database. Re-uploading the same data is safe (idempotent upserts).
- **LLM insights**: Each section has an "AI Insights" expander that calls OpenAI. The main page has a "Generate Call Summary" button that combines all available data into a structured markdown call doc with a download option.

### Main page features

- **Stale data warning** — Shows a warning if the newest GSC/GA4 data is older than 7 days
- **Generate Call Summary** — One-click LLM-powered summary across all sections, downloadable as markdown

## Adding new data sources

1. Create a new Alembic migration: `uv run alembic revision -m "add my_table"`
2. Write the `CREATE TABLE` SQL in the migration's `upgrade()` function
3. Add upsert and query helpers in `db.py`
4. Create a new section module in `sections/`

## Project structure

```
main.py                Streamlit entry point, stale data warning, call summary
db.py                  psycopg connection pool, query helpers, upsert functions
config.py              Page category mapping, Google API config
google_api.py          GSC and GA4 Data API integration
llm.py                 OpenAI SDK integration for insights and call summaries
alembic.ini            Alembic configuration
migrations/
  env.py               Reads DATABASE_URL from environment
  versions/
    001_initial_schema.py  Creates gsc, ga4, keyword_rankings, profound tables
sections/
  fetch_button.py      Shared "Fetch GSC + GA4 Data" button component
  search_impressions.py
  traffic_analytics.py
  keyword_performance.py
  geo_profound.py
```
