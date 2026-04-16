"""Traffic Analytics — Google Analytics (GA4) data via API."""

import pandas as pd
import plotly.express as px
import streamlit as st

from config import categorize_page, is_ga4_configured, is_gsc_configured
from db import query_df, upsert_ga4
from llm import render_chart_insight
from sections.fetch_button import render_fetch_button as _render_fetch_button

EXPECTED_COLUMNS = [
    "date",
    "page_path",
    "session_source",
    "sessions",
]

# Sources of particular interest for GEO traffic
GEO_SOURCES = ["chatgpt.com", "claude.ai", "perplexity.ai", "gemini.google.com"]

# Key session sources to always show
KEY_SOURCES = ["google", "github", "(direct)", "youtube", "reddit", "linkedin"]


def _enrich_ga4_df(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns to a GA4 dataframe."""
    df["date"] = pd.to_datetime(df["date"])
    df["page_category"] = df["page_path"].apply(categorize_page)
    df["sessions"] = pd.to_numeric(df["sessions"], errors="coerce").fillna(0)
    df["source_normalized"] = df["session_source"].str.lower().str.strip()
    return df


def _load_all_ga4_data() -> pd.DataFrame | None:
    """Load all GA4 data from the database."""
    df = query_df(
        "SELECT date, page_path, session_source, session_medium, sessions FROM ga4 ORDER BY date"
    )
    if df.empty:
        return None
    return _enrich_ga4_df(df)


def render():
    st.header("Traffic Analytics (GA4)")

    # --- Fetch button (shared: fetches both GSC + GA4) ---
    if is_gsc_configured() or is_ga4_configured():
        _render_fetch_button()
    else:
        st.info(
            "Google APIs not configured. Set `GOOGLE_SERVICE_ACCOUNT_JSON`, "
            "`GSC_PROPERTY`, and/or `GA4_PROPERTY_ID` in `.env`."
        )

    # --- Load data from DB or CSV upload fallback ---
    df = _load_all_ga4_data()

    if df is None:
        uploaded = st.file_uploader(
            "Or upload a GA4 CSV",
            type=["csv"],
            key="ga4_upload",
            help=f"Expected columns: {', '.join(EXPECTED_COLUMNS)}",
        )
        if uploaded is not None:
            raw = pd.read_csv(uploaded)
            raw.columns = [c.strip().lower().replace(" ", "_") for c in raw.columns]
            missing = [c for c in EXPECTED_COLUMNS if c not in raw.columns]
            if missing:
                st.error(f"Missing columns: {missing}")
                return
            if "session_medium" not in raw.columns:
                raw["session_medium"] = ""
            upsert_ga4(raw[["date", "page_path", "session_source", "session_medium", "sessions"]].to_dict("records"))
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
        key="ga4_dates",
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        df = df[(df["date"].dt.date >= date_range[0]) & (df["date"].dt.date <= date_range[1])]

    view = st.radio("View", ["Weekly", "Monthly"], horizontal=True, key="ga4_view")
    if view == "Weekly":
        df["period"] = df["date"].dt.to_period("W").apply(lambda r: r.start_time)
    else:
        df["period"] = df["date"].dt.to_period("M").apply(lambda r: r.start_time)

    # --- Period comparison setup ---
    periods = sorted(df["period"].unique())
    latest_period = periods[-1]
    prev_period = periods[-2] if len(periods) >= 2 else None
    period_label = "Week" if view == "Weekly" else "Month"

    df_latest = df[df["period"] == latest_period]
    df_prev = df[df["period"] == prev_period] if prev_period is not None else None

    # --- Overview (latest period with delta) ---
    st.subheader(f"Latest {period_label}")
    latest_sessions = df_latest["sessions"].sum()

    session_delta = None
    if df_prev is not None:
        prev_sessions = df_prev["sessions"].sum()
        if prev_sessions:
            session_delta = f"{(latest_sessions - prev_sessions) / prev_sessions * 100:+.0f}%"

    m1, m2 = st.columns(2)
    m1.metric("Total Sessions", f"{latest_sessions:,.0f}", delta=session_delta)
    m2.metric("Unique Sources", df_latest["session_source"].nunique())

    # --- Traffic by Medium ---
    st.subheader(f"Traffic by Medium (Latest {period_label})")

    medium_col = "session_medium"
    if medium_col in df.columns:
        med_latest = (
            df_latest.groupby(medium_col)["sessions"]
            .sum()
            .reset_index()
            .sort_values("sessions", ascending=False)
        )

        if df_prev is not None:
            med_prev = (
                df_prev.groupby(medium_col)["sessions"]
                .sum()
                .reset_index()
                .rename(columns={"sessions": "sessions_prev"})
            )
            med_latest = med_latest.merge(med_prev, on=medium_col, how="left").fillna(0)
            med_latest["change"] = med_latest.apply(
                lambda r: f"{(r['sessions'] - r['sessions_prev']) / r['sessions_prev'] * 100:+.0f}%"
                if r["sessions_prev"] > 0 else "new",
                axis=1,
            )

        med_display = {medium_col: "Medium", "sessions": "Sessions"}
        if df_prev is not None:
            med_display["change"] = "Change"
        st.dataframe(
            med_latest[list(med_display.keys())].rename(columns=med_display),
            hide_index=True,
            use_container_width=True,
        )

    # --- Sessions by source (latest period with change) ---
    st.subheader(f"Sessions by Source (Latest {period_label})")

    source_latest = (
        df_latest.groupby("session_source")["sessions"]
        .sum()
        .reset_index()
        .sort_values("sessions", ascending=False)
    )

    if df_prev is not None:
        source_prev = (
            df_prev.groupby("session_source")["sessions"]
            .sum()
            .reset_index()
            .rename(columns={"sessions": "sessions_prev"})
        )
        source_latest = source_latest.merge(source_prev, on="session_source", how="left").fillna(0)
        source_latest["change"] = source_latest.apply(
            lambda r: f"{(r['sessions'] - r['sessions_prev']) / r['sessions_prev'] * 100:+.0f}%"
            if r["sessions_prev"] > 0 else "new",
            axis=1,
        )

    src_display = {"session_source": "Source", "sessions": "Sessions"}
    if df_prev is not None:
        src_display["change"] = "Change"
    st.dataframe(
        source_latest.head(15)[list(src_display.keys())].rename(columns=src_display),
        hide_index=True,
        use_container_width=True,
    )

    # --- Source trend over time ---
    st.subheader(f"Source Trend ({view})")

    top_sources = source_latest.head(8)["session_source"].tolist()
    source_trend = (
        df[df["session_source"].isin(top_sources)]
        .groupby(["period", "session_source"])["sessions"]
        .sum()
        .reset_index()
    )
    fig_source_trend = px.line(
        source_trend,
        x="period",
        y="sessions",
        color="session_source",
        title=f"Top Sources Over Time ({view})",
        markers=True,
    )
    st.plotly_chart(fig_source_trend, use_container_width=True)

    src_trend_summary = source_trend.groupby("session_source")["sessions"].agg(["first", "last"]).reset_index()
    src_trend_text = "\n".join(f"  {r['session_source']}: {int(r['first']):,} → {int(r['last']):,}" for _, r in src_trend_summary.iterrows())
    render_chart_insight("source_trend", src_trend_text, "What's driving changes in traffic sources?")

    # --- Page category breakdown (latest period with change) ---
    st.subheader(f"Sessions by Page Category (Latest {period_label})")

    cat_latest = (
        df_latest.groupby("page_category")["sessions"]
        .sum()
        .reset_index()
        .sort_values("sessions", ascending=False)
    )

    if df_prev is not None:
        cat_prev = (
            df_prev.groupby("page_category")["sessions"]
            .sum()
            .reset_index()
            .rename(columns={"sessions": "sessions_prev"})
        )
        cat_latest = cat_latest.merge(cat_prev, on="page_category", how="left").fillna(0)
        cat_latest["change"] = cat_latest.apply(
            lambda r: f"{(r['sessions'] - r['sessions_prev']) / r['sessions_prev'] * 100:+.0f}%"
            if r["sessions_prev"] > 0 else "new",
            axis=1,
        )

    cat_display = {"page_category": "Page Category", "sessions": "Sessions"}
    if df_prev is not None:
        cat_display["change"] = "Change"
    st.dataframe(
        cat_latest[list(cat_display.keys())].rename(columns=cat_display),
        hide_index=True,
        use_container_width=True,
    )

    # --- Per-source landing page drill-down ---
    st.subheader("Landing Page Breakdown by Source")

    available_sources = source_latest["session_source"].tolist()
    drill_source = st.selectbox(
        "Select source to drill down",
        available_sources[:20],
        key="drill_source",
    )

    drill_df = (
        df_latest[df_latest["session_source"] == drill_source]
        .groupby("page_category")["sessions"]
        .sum()
        .reset_index()
        .sort_values("sessions", ascending=False)
    )

    fig_drill = px.bar(
        drill_df,
        x="page_category",
        y="sessions",
        text="sessions",
        title=f"Page Category Breakdown — {drill_source}",
        color="page_category",
    )
    fig_drill.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig_drill.update_layout(showlegend=False)
    st.plotly_chart(fig_drill, use_container_width=True)

    # --- GEO Traffic (AI sources) ---
    st.subheader(f"GEO Traffic — AI Sources (Latest {period_label})")

    geo_latest = df_latest[df_latest["source_normalized"].isin(GEO_SOURCES)]
    geo_prev = df_prev[df_prev["source_normalized"].isin(GEO_SOURCES)] if df_prev is not None else None

    if geo_latest.empty:
        st.info("No traffic from AI sources (chatgpt.com, claude.ai, perplexity.ai, gemini.google.com) detected.")
    else:
        geo_by_source = (
            geo_latest.groupby("session_source")["sessions"]
            .sum()
            .reset_index()
            .sort_values("sessions", ascending=False)
        )

        if geo_prev is not None and not geo_prev.empty:
            geo_prev_agg = (
                geo_prev.groupby("session_source")["sessions"]
                .sum()
                .reset_index()
                .rename(columns={"sessions": "sessions_prev"})
            )
            geo_by_source = geo_by_source.merge(geo_prev_agg, on="session_source", how="left").fillna(0)
            geo_by_source["change"] = geo_by_source.apply(
                lambda r: f"{(r['sessions'] - r['sessions_prev']) / r['sessions_prev'] * 100:+.0f}%"
                if r["sessions_prev"] > 0 else "new",
                axis=1,
            )

        # Per-source metrics with delta
        geo_cols = st.columns(len(geo_by_source))
        for col, (_, row) in zip(geo_cols, geo_by_source.iterrows()):
            delta = row.get("change") if "change" in geo_by_source.columns else None
            col.metric(row["session_source"], f"{int(row['sessions']):,}", delta=delta)

        # GEO trend (full data)
        geo_all = df[df["source_normalized"].isin(GEO_SOURCES)]
        geo_trend = (
            geo_all.groupby(["period", "session_source"])["sessions"]
            .sum()
            .reset_index()
        )
        fig_geo_trend = px.line(
            geo_trend,
            x="period",
            y="sessions",
            color="session_source",
            title=f"AI Source Traffic Trend ({view})",
            markers=True,
        )
        st.plotly_chart(fig_geo_trend, use_container_width=True)

        geo_trend_text = "\n".join(
            f"  {r['session_source']}: {int(r['sessions']):,}"
            for _, r in geo_by_source.iterrows()
        )
        render_chart_insight("geo_trend", geo_trend_text, "What's the AI traffic trajectory and what does it mean?")
