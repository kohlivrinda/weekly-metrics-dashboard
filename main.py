"""Weekly Metrics Dashboard — Streamlit entry point."""

import atexit
from datetime import date

import streamlit as st

from config import is_ga4_configured, is_gsc_configured
from db import get_pool, close_pool, latest_data_date

st.set_page_config(
    page_title="Weekly Metrics Dashboard",
    page_icon=":bar_chart:",
    layout="wide",
)

# Initialize DB connection pool
get_pool()
atexit.register(close_pool)

st.title("Weekly Metrics Dashboard")

# --- Stale data warning ---
def _data_age_days(table: str) -> int | None:
    """Return age in days of the newest row in the table, or None if empty."""
    latest = latest_data_date(table)
    if latest is None:
        return None
    return (date.today() - latest).days

gsc_age = _data_age_days("gsc")
ga4_age = _data_age_days("ga4")

stale = []
if gsc_age is not None and gsc_age > 7:
    stale.append(f"GSC data is **{gsc_age} days old**")
elif gsc_age is None and is_gsc_configured():
    stale.append("No GSC data yet")
if ga4_age is not None and ga4_age > 7:
    stale.append(f"GA4 data is **{ga4_age} days old**")
elif ga4_age is None and is_ga4_configured():
    stale.append("No GA4 data yet")

if stale:
    st.warning(f"{' | '.join(stale)} — consider re-fetching before the call.")

# --- Section navigation ---
SECTIONS = {
    "Search Impressions (GSC)": "search_impressions",
    "Traffic Analytics (GA4)": "traffic_analytics",
    "Keyword Performance": "keyword_performance",
    "GEO Performance (Profound)": "geo_profound",
}

selected = st.sidebar.radio("Section", list(SECTIONS.keys()))

if selected == "Search Impressions (GSC)":
    from sections.search_impressions import render
elif selected == "Traffic Analytics (GA4)":
    from sections.traffic_analytics import render
elif selected == "Keyword Performance":
    from sections.keyword_performance import render
elif selected == "GEO Performance (Profound)":
    from sections.geo_profound import render

render()
