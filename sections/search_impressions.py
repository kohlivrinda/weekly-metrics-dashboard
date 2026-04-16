"""Search Impressions — Google Search Console data via API."""

import pandas as pd
import plotly.express as px
import streamlit as st

from config import categorize_page, is_ga4_configured, is_gsc_configured
from db import query_df, upsert_gsc
from llm import render_chart_insight
from sections.fetch_button import render_fetch_button as _render_fetch_button

EXPECTED_COLUMNS = ["date", "page", "query", "clicks", "impressions", "ctr", "position"]


def _enrich_gsc_df(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns to a GSC dataframe."""
    df["date"] = pd.to_datetime(df["date"])
    df["page_category"] = df["page"].apply(
        lambda p: categorize_page(pd.Series([p]).str.extract(r"https?://[^/]+(/.*)")[0].iloc[0])
    )
    return df


def _load_all_gsc_data() -> pd.DataFrame | None:
    """Load all GSC data from the database."""
    df = query_df("SELECT date, page, query, clicks, impressions, ctr, position FROM gsc ORDER BY date")
    if df.empty:
        return None
    return _enrich_gsc_df(df)


def render():
    st.header("Search Impressions (GSC)")

    # --- Fetch button (shared: fetches both GSC + GA4) ---
    if is_gsc_configured() or is_ga4_configured():
        _render_fetch_button()
    else:
        st.info(
            "Google APIs not configured. Set `GOOGLE_SERVICE_ACCOUNT_JSON`, "
            "`GSC_PROPERTY`, and/or `GA4_PROPERTY_ID` in `.env`."
        )

    # --- Load data from DB or CSV upload fallback ---
    df = _load_all_gsc_data()

    if df is None:
        uploaded = st.file_uploader(
            "Or upload a GSC CSV",
            type=["csv"],
            key="gsc_upload",
            help=f"Expected columns: {', '.join(EXPECTED_COLUMNS)}",
        )
        if uploaded is not None:
            raw = pd.read_csv(uploaded)
            raw.columns = [c.strip().lower() for c in raw.columns]
            missing = [c for c in EXPECTED_COLUMNS if c not in raw.columns]
            if missing:
                st.error(f"Missing columns: {missing}")
                return
            upsert_gsc(raw[EXPECTED_COLUMNS].to_dict("records"))
            st.success(f"Inserted {len(raw):,} rows.")
            st.rerun()
        return

    if df is None:
        return

    # --- Date range filter ---
    dates = sorted(df["date"].dt.date.unique())
    date_range = st.date_input(
        "Date range",
        value=(min(dates), max(dates)),
        min_value=min(dates),
        max_value=max(dates),
        key="gsc_dates",
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        df = df[(df["date"].dt.date >= date_range[0]) & (df["date"].dt.date <= date_range[1])]

    # --- View toggle ---
    view = st.radio("View", ["Weekly", "Monthly"], horizontal=True, key="gsc_view")

    if view == "Weekly":
        df["period"] = df["date"].dt.to_period("W").apply(lambda r: r.start_time)
    else:
        df["period"] = df["date"].dt.to_period("M").apply(lambda r: r.start_time)

    # --- Period comparison setup ---
    periods = sorted(df["period"].unique())
    latest_period = periods[-1]
    prev_period = periods[-2] if len(periods) >= 2 else None

    df_latest = df[df["period"] == latest_period]
    df_prev = df[df["period"] == prev_period] if prev_period is not None else None

    # --- Top-level metrics (latest period with delta) ---
    period_label = "Week" if view == "Weekly" else "Month"
    st.subheader(f"Latest {period_label}")

    latest_impressions = df_latest["impressions"].sum()
    latest_clicks = df_latest["clicks"].sum()
    latest_ctr = latest_clicks / latest_impressions * 100 if latest_impressions else 0

    imp_delta = click_delta = ctr_delta = None
    if df_prev is not None:
        prev_impressions = df_prev["impressions"].sum()
        prev_clicks = df_prev["clicks"].sum()
        prev_ctr = prev_clicks / prev_impressions * 100 if prev_impressions else 0
        if prev_impressions:
            imp_delta = f"{(latest_impressions - prev_impressions) / prev_impressions * 100:+.0f}%"
        if prev_clicks:
            click_delta = f"{(latest_clicks - prev_clicks) / prev_clicks * 100:+.0f}%"
        ctr_delta = f"{latest_ctr - prev_ctr:+.1f}pp"

    # Pages appearing in search (proxy for indexed pages)
    pages_in_search = df_latest["page"].nunique()
    pages_delta = None
    if df_prev is not None:
        prev_pages = df_prev["page"].nunique()
        if prev_pages:
            pages_delta = f"{pages_in_search - prev_pages:+d}"

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Impressions", f"{latest_impressions:,.0f}", delta=imp_delta)
    m2.metric("Clicks", f"{latest_clicks:,.0f}", delta=click_delta)
    m3.metric("CTR", f"{latest_ctr:.1f}%", delta=ctr_delta)
    m4.metric("Pages in Search", f"{pages_in_search:,}", delta=pages_delta)

    # --- Impressions over time ---
    st.subheader(f"Impressions Trend ({view})")

    trend = df.groupby("period").agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
    ).reset_index()
    trend["period_label"] = trend["period"].dt.strftime("%b %d")

    fig_trend = px.bar(
        trend,
        x="period_label",
        y="impressions",
        title=f"{view} Impressions",
        labels={"period_label": "Period", "impressions": "Impressions"},
    )
    st.plotly_chart(fig_trend, use_container_width=True)

    trend_summary = "\n".join(f"  {r['period_label']}: {r['impressions']:,.0f}" for _, r in trend.iterrows())
    render_chart_insight("gsc_trend", trend_summary, "What's driving the week-over-week impressions trend?")

    # --- Page category breakdown (latest period with change vs previous) ---
    st.subheader(f"Impressions by Page Category (Latest {period_label})")

    cat_latest = (
        df_latest.groupby("page_category")
        .agg(impressions=("impressions", "sum"), clicks=("clicks", "sum"))
        .reset_index()
    )
    cat_latest["ctr"] = (cat_latest["clicks"] / cat_latest["impressions"] * 100).round(1)
    cat_latest["% of total"] = (
        cat_latest["impressions"] / latest_impressions * 100
    ).round(1)

    if df_prev is not None:
        cat_prev = (
            df_prev.groupby("page_category")
            .agg(impressions_prev=("impressions", "sum"), clicks_prev=("clicks", "sum"))
            .reset_index()
        )
        cat_latest = cat_latest.merge(cat_prev, on="page_category", how="left").fillna(0)
        cat_latest["imp_change"] = cat_latest.apply(
            lambda r: f"{(r['impressions'] - r['impressions_prev']) / r['impressions_prev'] * 100:+.0f}%"
            if r["impressions_prev"] > 0 else "new",
            axis=1,
        )
        cat_latest["ctr_prev"] = (
            cat_latest["clicks_prev"] / cat_latest["impressions_prev"] * 100
        ).round(1).fillna(0)

    cat_latest = cat_latest.sort_values("impressions", ascending=False)

    display_cols = {
        "page_category": "Page Category",
        "impressions": "Impressions",
        "clicks": "Clicks",
        "ctr": "CTR %",
        "% of total": "% of Total",
    }
    if df_prev is not None:
        display_cols["imp_change"] = "Change"
    st.dataframe(
        cat_latest[list(display_cols.keys())].rename(columns=display_cols),
        hide_index=True,
        use_container_width=True,
    )

    # --- Page category trend over time ---
    st.subheader(f"Page Category Trend ({view})")

    cat_trend = (
        df.groupby(["period", "page_category"])["impressions"]
        .sum()
        .reset_index()
    )
    fig_cat_trend = px.area(
        cat_trend,
        x="period",
        y="impressions",
        color="page_category",
        title=f"Impressions by Page Category ({view})",
    )
    st.plotly_chart(fig_cat_trend, use_container_width=True)
