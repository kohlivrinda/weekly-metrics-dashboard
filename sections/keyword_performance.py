"""Keyword Performance — SEMrush Position Tracking data.

Expected input: SEMrush Position Tracking "Rankings Overview" CSV export.

The CSV has a 5-line metadata header, then columns:
    Keyword, Tags, Intents,
    <domain>_<YYYYMMDD>,  <domain>_<YYYYMMDD>_type,  <domain>_<YYYYMMDD>_landing,
    ... (repeated per day),
    <domain>_difference, Search Volume, CPC, Keyword Difficulty
"""

import re
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from config import categorize_page
from db import query_df, upsert_keywords
from llm import render_chart_insight

# Maps raw intent codes to human-readable labels.
INTENT_LABELS = {
    "i": "Informational",
    "c": "Commercial",
    "n": "Navigational",
    "t": "Transactional",
}


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _skip_metadata(uploaded_file) -> int:
    """Return the number of header rows to skip before the real CSV header."""
    uploaded_file.seek(0)
    lines = uploaded_file.read().decode("utf-8", errors="replace").splitlines()
    uploaded_file.seek(0)
    for i, line in enumerate(lines):
        if line.startswith("Keyword,"):
            return i
    return 0


def _parse_dates_from_columns(columns: list[str]) -> list[str]:
    """Extract sorted unique YYYYMMDD date strings from column names."""
    dates = set()
    for col in columns:
        m = re.search(r"_(\d{8})$", col)
        if m:
            dates.add(m.group(1))
    return sorted(dates)


def _parse_position_tracking_csv(uploaded_file) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse the SEMrush Position Tracking CSV.

    Returns:
        keywords_df: One row per keyword with static metadata.
        daily_df: Long-format dataframe with one row per keyword × date,
                  columns: keyword, date, rank, result_type, landing_page.
    """
    skip = _skip_metadata(uploaded_file)
    df = pd.read_csv(uploaded_file, skiprows=skip)
    df.columns = [c.strip() for c in df.columns]

    # --- Extract dates from column names ---
    dates = _parse_dates_from_columns(df.columns)

    # --- Find the domain prefix (everything before _YYYYMMDD) ---
    sample_col = [c for c in df.columns if re.search(r"_\d{8}$", c)]
    if not sample_col:
        st.error("Could not find date-stamped position columns in the CSV.")
        return pd.DataFrame(), pd.DataFrame()
    domain_prefix = re.sub(r"_\d{8}$", "", sample_col[0])

    # --- Build long-format daily data ---
    daily_rows = []
    for _, row in df.iterrows():
        kw = row["Keyword"]
        for d in dates:
            rank_col = f"{domain_prefix}_{d}"
            type_col = f"{domain_prefix}_{d}_type"
            land_col = f"{domain_prefix}_{d}_landing"

            rank_raw = row.get(rank_col, "-")
            rank = pd.to_numeric(rank_raw, errors="coerce")

            result_type = str(row.get(type_col, "")).strip()
            landing = str(row.get(land_col, "")).strip()

            daily_rows.append({
                "keyword": kw,
                "date": pd.to_datetime(d, format="%Y%m%d"),
                "rank": rank,
                "result_type": result_type if pd.notna(rank) else "",
                "landing_page": landing if pd.notna(rank) else "",
            })

    daily_df = pd.DataFrame(daily_rows)

    # --- Build keyword metadata table ---
    diff_col = f"{domain_prefix}_difference"
    keywords_df = df[["Keyword"]].copy()
    keywords_df = keywords_df.rename(columns={"Keyword": "keyword"})

    if "Tags" in df.columns:
        keywords_df["tags"] = df["Tags"].fillna("")
    if "Intents" in df.columns:
        keywords_df["intents"] = df["Intents"].fillna("")
    if "Search Volume" in df.columns:
        keywords_df["search_volume"] = pd.to_numeric(df["Search Volume"], errors="coerce")
    if "CPC" in df.columns:
        keywords_df["cpc"] = pd.to_numeric(df["CPC"], errors="coerce")
    if "Keyword Difficulty" in df.columns:
        keywords_df["difficulty"] = pd.to_numeric(df["Keyword Difficulty"], errors="coerce")
    if diff_col in df.columns:
        keywords_df["wow_change"] = pd.to_numeric(df[diff_col], errors="coerce")

    return keywords_df, daily_df


def _parse_date_from_col_name(col: str) -> pd.Timestamp | None:
    """Parse a human-readable date column like '7th Aug Semrush' or 'Aug 16 GSC'.

    Returns a Timestamp or None if unparseable.
    """
    # Strip source suffix
    date_part = re.sub(r"\s*(Semrush|GSC)\s*$", "", col, flags=re.IGNORECASE).strip()
    # Remove ordinal suffixes
    date_part = re.sub(r"(\d+)(st|nd|rd|th)\b", r"\1", date_part)

    # Try various formats
    for fmt in ("%d %b", "%b %d", "%d %B", "%B %d"):
        try:
            dt = pd.to_datetime(date_part, format=fmt)
            # Infer year: if month > current month, it's probably last year
            # Columns go Aug -> Mar, so Aug-Dec = 2025, Jan-Mar = 2026
            if dt.month >= 8:
                dt = dt.replace(year=2025)
            else:
                dt = dt.replace(year=2026)
            return dt
        except (ValueError, TypeError):
            continue
    return None


def _parse_keyword_tracking_csv(uploaded_file) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse the manually maintained keyword tracking CSV.

    Columns: Primary Keywords, Primary/Secondary, Difficulty, Search Volume,
    then date-stamped rank columns like '7th Aug Semrush', 'Aug 16 GSC'.

    Returns:
        keywords_df: One row per keyword with static metadata.
        daily_df: Long-format dataframe with one row per keyword × date × source.
    """
    uploaded_file.seek(0)
    df = pd.read_csv(uploaded_file)
    df.columns = [c.strip() for c in df.columns]

    # Identify the keyword column
    kw_col = None
    for candidate in ["Primary Keywords", "Keywords", "Keyword"]:
        if candidate in df.columns:
            kw_col = candidate
            break
    if kw_col is None:
        st.error("Could not find a keyword column in the CSV.")
        return pd.DataFrame(), pd.DataFrame()

    # Identify date columns (contain 'Semrush' or 'GSC')
    date_cols = [c for c in df.columns if re.search(r"(Semrush|GSC)\s*$", c, re.IGNORECASE)]
    if not date_cols:
        st.error("Could not find date-stamped rank columns (expected 'Semrush' or 'GSC' suffix).")
        return pd.DataFrame(), pd.DataFrame()

    # Build long-format daily data
    daily_rows = []
    for _, row in df.iterrows():
        kw = str(row[kw_col]).strip()
        if not kw:
            continue
        for col in date_cols:
            dt = _parse_date_from_col_name(col)
            if dt is None:
                continue
            source = "semrush" if "semrush" in col.lower() else "gsc"
            rank_raw = row.get(col, "-")
            rank = pd.to_numeric(rank_raw, errors="coerce")

            daily_rows.append({
                "keyword": kw,
                "date": dt,
                "rank": rank,
                "result_type": source,
                "landing_page": "",
            })

    daily_df = pd.DataFrame(daily_rows)

    # Build keyword metadata
    keywords_df = df[[kw_col]].copy()
    keywords_df = keywords_df.rename(columns={kw_col: "keyword"})
    keywords_df["keyword"] = keywords_df["keyword"].str.strip()

    if "Primary/Secondary" in df.columns:
        keywords_df["tags"] = df["Primary/Secondary"].fillna("")
    else:
        keywords_df["tags"] = ""

    keywords_df["intents"] = ""

    if "Search Volume" in df.columns:
        keywords_df["search_volume"] = pd.to_numeric(df["Search Volume"], errors="coerce")
    if "Difficulty" in df.columns:
        keywords_df["difficulty"] = pd.to_numeric(df["Difficulty"], errors="coerce")

    keywords_df["cpc"] = np.nan

    return keywords_df, daily_df


def _detect_and_parse_csv(uploaded_file) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Auto-detect CSV format and parse accordingly."""
    uploaded_file.seek(0)
    header_bytes = uploaded_file.read(2048).decode("utf-8", errors="replace")
    uploaded_file.seek(0)

    # SEMrush Position Tracking: has a metadata header starting with "---"
    # and columns with _YYYYMMDD patterns
    if "Keyword," in header_bytes and re.search(r"_\d{8}", header_bytes):
        return _parse_position_tracking_csv(uploaded_file)

    # Manual keyword tracking: has 'Semrush' or 'GSC' in column names
    if re.search(r"(Semrush|GSC)", header_bytes):
        return _parse_keyword_tracking_csv(uploaded_file)

    st.error(
        "Unrecognized CSV format. Expected either:\n"
        "- SEMrush Position Tracking export (columns with `_YYYYMMDD` dates)\n"
        "- Keyword tracking sheet (columns ending in 'Semrush' or 'GSC')"
    )
    return pd.DataFrame(), pd.DataFrame()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _latest_rank(daily_df: pd.DataFrame) -> pd.DataFrame:
    """For each keyword return the most recent day's rank, type, and landing page."""
    latest_date = daily_df["date"].max()
    return daily_df[daily_df["date"] == latest_date].copy()


def _page_path_from_url(url: str) -> str:
    """Extract the path from a full URL for page categorization."""
    if not isinstance(url, str) or not url.startswith("http"):
        return ""
    try:
        return urlparse(url).path
    except Exception:
        return ""


def _expand_intents(intent_str: str) -> list[str]:
    """Split '|'-separated intent codes into labels."""
    if not isinstance(intent_str, str) or not intent_str.strip():
        return []
    return [INTENT_LABELS.get(code.strip(), code.strip()) for code in intent_str.split("|")]


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def _load_keyword_data() -> tuple[pd.DataFrame, pd.DataFrame] | None:
    """Load keyword data from database."""
    daily_df = query_df(
        "SELECT keyword, date, rank, result_type, landing_page FROM keyword_rankings ORDER BY date"
    )
    if daily_df.empty:
        return None
    daily_df["date"] = pd.to_datetime(daily_df["date"])
    daily_df["rank"] = pd.to_numeric(daily_df["rank"], errors="coerce")

    keywords_df = query_df("""
        SELECT DISTINCT ON (keyword)
            keyword, search_volume, cpc, difficulty, tags, intents
        FROM keyword_rankings
        ORDER BY keyword, date DESC
    """)

    # Compute wow_change from data
    dates = sorted(daily_df["date"].unique())
    if len(dates) >= 2:
        latest_date = dates[-1]
        prev_date = latest_date - pd.Timedelta(days=7)
        closest_prev = daily_df[daily_df["date"] <= prev_date]["date"].max()
        if pd.notna(closest_prev):
            latest_ranks = daily_df[daily_df["date"] == latest_date][["keyword", "rank"]].rename(columns={"rank": "rank_latest"})
            prev_ranks = daily_df[daily_df["date"] == closest_prev][["keyword", "rank"]].rename(columns={"rank": "rank_prev"})
            wow = latest_ranks.merge(prev_ranks, on="keyword", how="left")
            wow["wow_change"] = wow["rank_latest"] - wow["rank_prev"]
            keywords_df = keywords_df.merge(wow[["keyword", "wow_change"]], on="keyword", how="left")

    if "wow_change" not in keywords_df.columns:
        keywords_df["wow_change"] = np.nan

    return keywords_df, daily_df


def _insert_keyword_upload(uploaded_file):
    """Parse a SEMrush CSV and insert into database."""
    keywords_df, daily_df = _detect_and_parse_csv(uploaded_file)
    if daily_df.empty:
        return 0

    # Merge metadata into daily rows for upsert
    merged = daily_df.merge(
        keywords_df[["keyword", "search_volume", "cpc", "difficulty", "tags", "intents"]].drop_duplicates("keyword"),
        on="keyword",
        how="left",
    )
    merged["date"] = merged["date"].dt.strftime("%Y-%m-%d")
    merged = merged.fillna({"tags": "", "intents": "", "result_type": "", "landing_page": ""})

    # Convert to records and replace NaN with None (psycopg needs None, not nan)
    rows = merged.to_dict("records")
    for row in rows:
        for key in ("rank", "search_volume", "cpc", "difficulty"):
            v = row.get(key)
            if v is not None and (isinstance(v, float) and np.isnan(v)):
                row[key] = None

    upsert_keywords(rows)
    return len(rows)


def render():
    st.header("Keyword Performance")

    uploaded = st.file_uploader(
        "Upload SEMrush Position Tracking CSV",
        type=["csv"],
        key="keyword_upload",
        help="SEMrush → Position Tracking → Rankings Overview → Export CSV",
    )

    if uploaded is not None:
        count = _insert_keyword_upload(uploaded)
        if count:
            st.success(f"Inserted {count:,} keyword ranking rows.")

    result = _load_keyword_data()
    if result is None:
        if uploaded is None:
            st.info("Upload a SEMrush Position Tracking CSV to see analysis.")
        return

    keywords_df, daily_df = result

    if daily_df.empty:
        return

    # --- Date range filter ---
    all_dates = sorted(daily_df["date"].dt.date.unique())
    date_range = st.date_input(
        "Date range",
        value=(min(all_dates), max(all_dates)),
        min_value=min(all_dates),
        max_value=max(all_dates),
        key="kw_dates",
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        daily_df = daily_df[
            (daily_df["date"].dt.date >= date_range[0]) & (daily_df["date"].dt.date <= date_range[1])
        ]

    latest = _latest_rank(daily_df)
    dates = sorted(daily_df["date"].unique())
    n_days = len(dates)

    # ---------------------------------------------------------------
    # Overview metrics
    # ---------------------------------------------------------------
    st.subheader("Overview")

    total_keywords = keywords_df.shape[0]
    ranked_latest = latest[latest["rank"].notna()]
    n_ranked = len(ranked_latest)
    n_top10 = (ranked_latest["rank"] <= 10).sum()
    n_top3 = (ranked_latest["rank"] <= 3).sum()
    n_unranked = total_keywords - n_ranked

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Tracked", total_keywords)
    m2.metric("Ranked", n_ranked)
    m3.metric("Top 3", int(n_top3))
    m4.metric("Top 10", int(n_top10))
    m5.metric("Unranked", n_unranked)

    # ---------------------------------------------------------------
    # Latest rankings snapshot
    # ---------------------------------------------------------------
    st.subheader("Latest Rankings")

    snapshot = latest.merge(keywords_df, on="keyword", how="left")
    snapshot = snapshot[snapshot["rank"].notna()].copy()
    snapshot["landing_category"] = snapshot["landing_page"].apply(
        lambda u: categorize_page(_page_path_from_url(u))
    )

    display_cols = {
        "keyword": "Keyword",
        "rank": "Rank",
        "result_type": "SERP Type",
        "landing_category": "Page Category",
        "wow_change": "WoW Change",
        "search_volume": "Volume",
        "difficulty": "Difficulty",
    }
    snapshot_display = (
        snapshot[list(display_cols.keys())]
        .rename(columns=display_cols)
        .sort_values("Rank")
    )

    st.dataframe(
        snapshot_display,
        hide_index=True,
        width="stretch",
        column_config={
            "Rank": st.column_config.NumberColumn(format="%d"),
            "WoW Change": st.column_config.NumberColumn(
                help="Position change vs prior week (negative = improved)",
            ),
        },
    )

    # Keep type_counts for LLM summary
    type_counts = (
        latest[latest["rank"].notna()]
        .groupby("result_type")
        .size()
        .reset_index(name="keywords")
        .sort_values("keywords", ascending=False)
    )

    # ---------------------------------------------------------------
    # Daily rank trends
    # ---------------------------------------------------------------
    st.subheader("Daily Rank Trends")

    # Let user select keywords to chart
    ranked_keywords = sorted(
        ranked_latest["keyword"].tolist(),
        key=lambda k: ranked_latest.loc[ranked_latest["keyword"] == k, "rank"].iloc[0],
    )
    default_kws = ranked_keywords[:8]

    selected_kws = st.multiselect(
        "Select keywords to chart",
        ranked_keywords,
        default=default_kws,
        key="kw_trend_select",
    )

    if selected_kws:
        trend_df = daily_df[daily_df["keyword"].isin(selected_kws)].copy()
        trend_df = trend_df[trend_df["rank"].notna()]

        fig_trend = px.line(
            trend_df,
            x="date",
            y="rank",
            color="keyword",
            title="Position Over Time (lower is better)",
            markers=True,
        )
        fig_trend.update_yaxes(autorange="reversed")
        fig_trend.update_layout(xaxis_tickformat="%b %d")
        st.plotly_chart(fig_trend, width="stretch")

        kw_trend_text = "\n".join(
            f"  {kw}: rank {int(trend_df[trend_df['keyword']==kw]['rank'].iloc[-1])}"
            for kw in selected_kws
            if not trend_df[trend_df['keyword']==kw].empty
        )
        render_chart_insight("kw_trends", kw_trend_text, "Which keywords improved or declined and what does it suggest?")

    # ---------------------------------------------------------------
    # Rank stability
    # ---------------------------------------------------------------
    if n_days >= 3:
        st.subheader("Rank Stability")
        st.caption("Keywords with the most position volatility during the week.")

        stability = (
            daily_df[daily_df["rank"].notna()]
            .groupby("keyword")
            .agg(
                min_rank=("rank", "min"),
                max_rank=("rank", "max"),
                std=("rank", "std"),
                days_ranked=("rank", "count"),
            )
            .reset_index()
        )
        stability["swing"] = stability["max_rank"] - stability["min_rank"]
        stability = stability[stability["days_ranked"] >= 3]
        stability = stability.sort_values("swing", ascending=False)

        volatile = stability[stability["swing"] > 0].head(15)

        if not volatile.empty:
            fig_vol = px.bar(
                volatile,
                x="swing",
                y="keyword",
                orientation="h",
                title="Biggest Position Swings This Week",
                labels={"swing": "Position Swing (max − min)", "keyword": ""},
                text="swing",
            )
            fig_vol.update_layout(yaxis={"categoryorder": "total ascending"})
            fig_vol.update_traces(textposition="outside")
            st.plotly_chart(fig_vol, width="stretch")
        else:
            st.success("All positions were stable this week.")

    # ---------------------------------------------------------------
    # Keyword difficulty vs rank
    # ---------------------------------------------------------------
    st.subheader("Keyword Difficulty vs Rank")

    diff_rank = snapshot[["keyword", "rank", "difficulty", "search_volume"]].dropna(
        subset=["rank", "difficulty"]
    )

    if not diff_rank.empty:
        diff_rank = diff_rank.sort_values("rank")
        diff_rank["search_volume"] = diff_rank["search_volume"].apply(
            lambda v: f"{int(v):,}" if pd.notna(v) and v > 0 else "—"
        )
        diff_rank["difficulty"] = diff_rank["difficulty"].apply(
            lambda v: f"{int(v)}" if pd.notna(v) else "—"
        )
        diff_rank["rank"] = diff_rank["rank"].apply(lambda v: f"{v:.1f}" if pd.notna(v) else "—")

        # Add tag column from keywords_df if available
        if "tags" in keywords_df.columns:
            tag_map = keywords_df.set_index("keyword")["tags"].to_dict()
            diff_rank["tag"] = diff_rank["keyword"].map(tag_map).fillna("")

        display_cols = {"keyword": "Keyword", "rank": "Rank", "difficulty": "Difficulty", "search_volume": "Volume"}
        if "tag" in diff_rank.columns:
            display_cols["tag"] = "Tag"

        st.dataframe(
            diff_rank[list(display_cols.keys())].rename(columns=display_cols),
            hide_index=True,
            width="stretch",
        )

