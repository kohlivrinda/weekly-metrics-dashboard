"""Traffic Analytics — Google Analytics (GA4) data via API.

CSV columns: date, page_path, session_source, session_medium, sessions
"""

import pandas as pd
import plotly.express as px
import streamlit as st

from config import categorize_page, find_ga4_csvs, is_ga4_configured, is_gsc_configured
from llm import render_llm_insights
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


def _parse_ga4_df(df: pd.DataFrame) -> pd.DataFrame | None:
    """Validate and enrich a raw GA4 dataframe."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        st.error(
            f"CSV is missing expected columns: {missing}. "
            f"Expected at minimum: {EXPECTED_COLUMNS}"
        )
        return None

    df["date"] = pd.to_datetime(df["date"])
    df["page_category"] = df["page_path"].apply(categorize_page)
    df["sessions"] = pd.to_numeric(df["sessions"], errors="coerce").fillna(0)
    df["source_normalized"] = df["session_source"].str.lower().str.strip()

    return df


def _load_all_ga4_data() -> pd.DataFrame | None:
    """Load and merge all GA4 CSVs in the data directory."""
    paths = find_ga4_csvs()
    if not paths:
        return None
    frames = []
    for p in paths:
        df = _parse_ga4_df(pd.read_csv(p))
        if df is not None:
            frames.append(df)
    if not frames:
        return None
    combined = pd.concat(frames, ignore_index=True)
    dedup_cols = ["date", "page_path", "session_source"]
    if "session_medium" in combined.columns:
        dedup_cols.append("session_medium")
    combined = combined.drop_duplicates(subset=dedup_cols)
    return combined


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

    # --- Load data: all CSVs on disk or file upload fallback ---
    df = _load_all_ga4_data()

    if df is None:
        uploaded = st.file_uploader(
            "Or upload a GA4 CSV",
            type=["csv"],
            key="ga4_upload",
            help=f"Expected columns: {', '.join(EXPECTED_COLUMNS)}",
        )
        if uploaded is None:
            return
        df = _parse_ga4_df(pd.read_csv(uploaded))

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

        col_chart, col_table = st.columns([2, 1])
        with col_chart:
            fig_med = px.bar(
                med_latest,
                x=medium_col,
                y="sessions",
                text="sessions",
                title="Sessions by Medium",
                color=medium_col,
            )
            fig_med.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
            fig_med.update_layout(showlegend=False)
            st.plotly_chart(fig_med, use_container_width=True)

        with col_table:
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

    fig_sources = px.bar(
        source_latest.head(15),
        x="sessions",
        y="session_source",
        orientation="h",
        title="Top 15 Session Sources",
        text="sessions",
    )
    fig_sources.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig_sources.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_sources, use_container_width=True)

    # Source table with change column
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

    col_chart, col_table = st.columns([2, 1])
    with col_chart:
        fig_cats = px.bar(
            cat_latest,
            x="page_category",
            y="sessions",
            text="sessions",
            title=f"Sessions by Page Category (Latest {period_label})",
            color="page_category",
        )
        fig_cats.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
        fig_cats.update_layout(showlegend=False)
        st.plotly_chart(fig_cats, use_container_width=True)

    with col_table:
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

        # GEO per-source page category breakdown
        geo_cat = (
            geo_latest.groupby(["session_source", "page_category"])["sessions"]
            .sum()
            .reset_index()
        )
        fig_geo_cat = px.bar(
            geo_cat,
            x="session_source",
            y="sessions",
            color="page_category",
            title="AI Source Traffic by Page Category",
            barmode="stack",
            text="sessions",
        )
        st.plotly_chart(fig_geo_cat, use_container_width=True)

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

    # --- LLM Insights ---
    top_5_sources = source_latest.head(5)
    change_info = ""
    if df_prev is not None and "change" in source_latest.columns:
        change_info = " | ".join(
            f"{r['session_source']}: {r['change']}"
            for _, r in top_5_sources.iterrows()
        )
        change_info = f"\nSource changes vs prev {period_label.lower()}: {change_info}"

    geo_summary = ""
    if not geo_latest.empty:
        geo_lines = []
        for _, r in geo_by_source.iterrows():
            line = f"{r['session_source']}: {int(r['sessions']):,}"
            if "change" in geo_by_source.columns:
                line += f" ({r['change']})"
            geo_lines.append(line)
        geo_summary = f"\nAI source traffic: {', '.join(geo_lines)}"

    data_summary = f"""Latest {period_label} sessions: {latest_sessions:,.0f}{f' ({session_delta} vs prev)' if session_delta else ''}
Top sources: {', '.join(f"{r['session_source']}({int(r['sessions']):,})" for _, r in top_5_sources.iterrows())}{change_info}
Page categories: {', '.join(f"{r['page_category']}({int(r['sessions']):,})" for _, r in cat_latest.head(5).iterrows())}{geo_summary}"""

    render_llm_insights("Traffic Analytics", data_summary)
