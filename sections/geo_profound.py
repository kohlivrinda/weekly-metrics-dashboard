"""GEO Performance — Profound data analysis."""

import json
import re
from urllib.parse import urlparse

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from config import (
    COMPETITOR_DOMAINS,
    COMPETITOR_NAMES,
    OWNED_DOMAINS,
    TOPIC_COMPETITORS,
    categorize_page,
)
from db import query_df, upsert_profound
from llm import render_chart_insight

PLATFORMS = ["ChatGPT", "Google AI Overviews", "Perplexity"]


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------


@st.cache_data(ttl=300)
def _load_profound_data_from_db() -> pd.DataFrame | None:
    """Load all Profound data from database."""
    df = query_df("""
        SELECT date, topic, prompt, platform, position, mentioned, mentions,
               citations, run_id, platform_id, tags, region,
               persona, type, search_queries, normalized_mentions
        FROM profound ORDER BY date
    """)
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    return df


def _parse_profound_csv_for_db(uploaded_file) -> list[dict]:
    """Parse a Profound CSV and return rows ready for DB upsert."""
    df = pd.read_csv(uploaded_file)
    cit_cols = [c for c in df.columns if re.match(r"citation_\d+", c)]

    rows = []
    for _, row in df.iterrows():
        citations = []
        for c in cit_cols:
            v = row.get(c)
            if isinstance(v, str) and v.startswith("http"):
                citations.append(v)

        mentioned_raw = str(row.get("mentioned?", "")).lower()
        mentioned = mentioned_raw in ("yes", "true", "1")

        rows.append({
            "date": str(row["date"]),
            "topic": str(row.get("topic", "")),
            "prompt": str(row.get("prompt", "")),
            "platform": str(row.get("platform", "")),
            "position": str(row.get("position", "")),
            "mentioned": mentioned,
            "mentions": str(row.get("mentions", "") if pd.notna(row.get("mentions")) else ""),
            "normalized_mentions": str(row.get("normalized_mentions", "") if pd.notna(row.get("normalized_mentions")) else ""),
            "citations": json.dumps(citations),
            "response": str(row.get("response", "") if pd.notna(row.get("response")) else ""),
            "run_id": str(row.get("run_id", "") if pd.notna(row.get("run_id")) else ""),
            "platform_id": str(row.get("platformId", "") if pd.notna(row.get("platformId")) else ""),
            "tags": str(row.get("tags", "") if pd.notna(row.get("tags")) else ""),
            "region": str(row.get("region", "") if pd.notna(row.get("region")) else ""),
            "persona": str(row.get("persona", "") if pd.notna(row.get("persona")) else ""),
            "type": str(row.get("type", "") if pd.notna(row.get("type")) else ""),
            "search_queries": str(row.get("search_queries", "") if pd.notna(row.get("search_queries")) else ""),
        })
    return rows


def _is_mentioned(df: pd.DataFrame) -> pd.Series:
    """Boolean series: was our brand mentioned by name?"""
    return df["mentioned"].astype(bool)


def _has_domain(row, domain_set: set) -> bool:
    """Check if any citation URL belongs to one of the given domains."""
    citations = row.get("citations", [])
    if not isinstance(citations, list):
        return False
    for v in citations:
        if not isinstance(v, str) or not v.startswith("http"):
            continue
        try:
            h = urlparse(v).netloc.lower().removeprefix("www.")
            if any(h == d or h.endswith("." + d) for d in domain_set):
                return True
        except Exception:
            pass
    return False


def _extract_owned_urls(df: pd.DataFrame) -> pd.Series:
    """Extract all owned-domain URLs from the citations JSONB column."""
    urls = []
    for citations in df["citations"]:
        if not isinstance(citations, list):
            continue
        for v in citations:
            if not isinstance(v, str) or not v.startswith("http"):
                continue
            try:
                h = urlparse(v).netloc.lower().removeprefix("www.")
                if any(h == d or h.endswith("." + d) for d in OWNED_DOMAINS):
                    parsed = urlparse(v)
                    clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
                    urls.append(clean)
            except Exception:
                pass
    return pd.Series(urls) if urls else pd.Series(dtype=str)


def _comp_mentioned(mentions_str: str) -> bool:
    """Check if any competitor name appears in the mentions column."""
    m = str(mentions_str).lower()
    return any(name in m for name in COMPETITOR_NAMES)


def _extract_comp_names(mentions_series: pd.Series) -> pd.Series:
    """From a series of comma-separated mention strings, extract competitor names."""
    names = (
        mentions_series.dropna()
        .str.lower()
        .str.split(",")
        .explode()
        .str.strip()
    )
    return names[names.isin(COMPETITOR_NAMES)]


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------


def render():
    st.header("GEO Performance — Profound Data")

    uploaded = st.file_uploader(
        "Upload Profound CSV (raw data with citations)",
        type=["csv"],
        key="profound_upload",
    )

    if uploaded is not None:
        rows = _parse_profound_csv_for_db(uploaded)
        upsert_profound(rows)
        st.success(f"Inserted {len(rows):,} rows.")

    df = _load_profound_data_from_db()
    if df is None:
        if uploaded is None:
            st.info("Upload a Profound CSV export to see GEO analysis.")
        return

    # Pre-compute derived columns once
    df["is_mentioned"] = _is_mentioned(df)
    df["has_fp_citation"] = df.apply(
        lambda row: _has_domain(row, OWNED_DOMAINS), axis=1
    )
    df["comp_mentioned"] = df["mentions"].apply(_comp_mentioned)

    # --- Filters ---
    col1, col2, col3 = st.columns(3)
    with col1:
        topics = sorted(df["topic"].unique())
        selected_topic = st.selectbox("Topic", ["All"] + topics)
    with col2:
        dates = sorted(df["date"].dt.date.unique())
        date_range = st.date_input(
            "Date range",
            value=(min(dates), max(dates)),
            min_value=min(dates),
            max_value=max(dates),
        )
    with col3:
        selected_platform = st.selectbox("Platform", ["All"] + PLATFORMS)

    # Apply filters
    mask = pd.Series(True, index=df.index)
    if selected_topic != "All":
        mask &= df["topic"] == selected_topic
    if isinstance(date_range, tuple) and len(date_range) == 2:
        mask &= (df["date"].dt.date >= date_range[0]) & (
            df["date"].dt.date <= date_range[1]
        )
    if selected_platform != "All":
        mask &= df["platform"] == selected_platform
    filtered = df[mask]

    if filtered.empty:
        st.warning("No data matches the selected filters.")
        return

    # --- Overview metrics ---
    st.subheader("Overview")

    total_prompts = filtered["prompt"].nunique()
    mentioned = filtered[filtered["is_mentioned"]]
    prompts_appeared = mentioned["prompt"].nunique()

    # Count unique owned articles cited
    owned_urls = _extract_owned_urls(filtered)
    unique_articles_cited = owned_urls.nunique() if not owned_urls.empty else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Unique Prompts", total_prompts)
    m2.metric("Prompts We're Mentioned In", prompts_appeared)
    m3.metric("Mention Rate", f"{prompts_appeared / total_prompts * 100:.0f}%")
    m4.metric("Our Articles Cited", unique_articles_cited)

    # --- Per-platform appearance ---
    st.subheader("Mentions by Platform")

    platform_stats = []
    for platform in PLATFORMS:
        pdf = filtered[filtered["platform"] == platform]
        total = pdf["prompt"].nunique()
        appeared = pdf[pdf["is_mentioned"]]["prompt"].nunique()
        platform_stats.append(
            {
                "Platform": platform,
                "Total Prompts": total,
                "Mentioned": appeared,
                "Mention Rate": f"{appeared / total * 100:.0f}%" if total else "N/A",
            }
        )

    stats_df = pd.DataFrame(platform_stats)
    col_chart, col_table = st.columns([2, 1])

    with col_chart:
        fig = px.bar(
            stats_df,
            x="Platform",
            y="Mentioned",
            color="Platform",
            text="Mentioned",
            title="Prompts Mentioned by Platform",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, width="stretch")

    with col_table:
        st.dataframe(stats_df, hide_index=True, width="stretch")

    # --- Cross-platform overlap ---
    st.subheader("Cross-Platform Overlap")

    prompt_platforms = (
        mentioned.groupby("prompt")["platform"].apply(set).reset_index()
    )
    prompt_platforms["num_platforms"] = prompt_platforms["platform"].apply(len)

    all_three = (prompt_platforms["num_platforms"] == 3).sum()
    at_least_two = (prompt_platforms["num_platforms"] >= 2).sum()
    exactly_one = (prompt_platforms["num_platforms"] == 1).sum()

    o1, o2, o3 = st.columns(3)
    o1.metric("All 3 Platforms", all_three)
    o2.metric("At Least 2 Platforms", at_least_two)
    o3.metric("Exactly 1 Platform", exactly_one)

    overlap_data = []
    for p1 in PLATFORMS:
        row = {}
        prompts_p1 = set(mentioned[mentioned["platform"] == p1]["prompt"].unique())
        for p2 in PLATFORMS:
            prompts_p2 = set(mentioned[mentioned["platform"] == p2]["prompt"].unique())
            row[p2] = len(prompts_p1 & prompts_p2)
        overlap_data.append(row)

    overlap_df = pd.DataFrame(overlap_data, index=PLATFORMS)

    # --- Our Articles Cited ---
    st.subheader("Our Articles Cited")

    if owned_urls.empty:
        st.info("No owned-domain URLs found in citations.")
    else:
        url_counts = owned_urls.value_counts().reset_index()
        url_counts.columns = ["URL", "Times Cited"]

        # Extract path for readability
        url_counts["Page"] = url_counts["URL"].apply(
            lambda u: urlparse(u).path.rstrip("/") or "/"
        )
        url_counts["Page Category"] = url_counts["Page"].apply(
            lambda p: categorize_page(p)
        )

        col_chart, col_table = st.columns([2, 1])
        with col_chart:
            fig_cited = px.bar(
                url_counts.head(15),
                x="Times Cited",
                y="Page",
                orientation="h",
                title="Most Cited Owned Pages (Top 15)",
            )
            fig_cited.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig_cited, width="stretch")

        with col_table:
            st.dataframe(
                url_counts[["Page", "Page Category", "Times Cited"]],
                hide_index=True,
                width="stretch",
            )

        # Summary by page category
        cat_cited = url_counts.groupby("Page Category").agg(
            unique_pages=("Page", "nunique"),
            total_citations=("Times Cited", "sum"),
        ).reset_index().sort_values("total_citations", ascending=False)
        cat_cited.columns = ["Page Category", "Unique Pages", "Total Citations"]
        st.write("**Citations by page category:**")
        st.dataframe(cat_cited, hide_index=True, width="stretch")

    # ------------------------------------------------------------------
    # COMPETITOR MENTIONS — replaces old "Most Cited Pages" section
    # ------------------------------------------------------------------
    st.subheader("Competitor Mentions")

    comp_names = _extract_comp_names(filtered["mentions"])

    if comp_names.empty:
        st.info("No competitor mentions found in the filtered data.")
    else:
        comp_counts = comp_names.value_counts().reset_index()
        comp_counts.columns = ["Competitor", "Mentions"]

        fig_comp = px.bar(
            comp_counts.head(15),
            x="Mentions",
            y="Competitor",
            orientation="h",
            title="Most Mentioned Competitors",
        )
        fig_comp.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_comp, width="stretch")

        # Per-platform competitor breakdown
        comp_by_platform = []
        for platform in PLATFORMS:
            pdf = filtered[filtered["platform"] == platform]
            pcomp = _extract_comp_names(pdf["mentions"])
            if pcomp.empty:
                continue
            for name, count in pcomp.value_counts().head(5).items():
                total_rows = len(pdf)
                comp_by_platform.append(
                    {
                        "Platform": platform,
                        "Competitor": name,
                        "Mention Count": count,
                        "Rate": f"{count / total_rows * 100:.1f}%",
                    }
                )

        if comp_by_platform:
            st.write("**Top competitors by platform:**")
            st.dataframe(
                pd.DataFrame(comp_by_platform), hide_index=True, width="stretch"
            )

    # Keep gap_prompts for LLM summary
    prompt_stats = (
        filtered.groupby(["topic", "prompt"])
        .agg(
            total=("is_mentioned", "size"),
            mentioned_count=("is_mentioned", "sum"),
            cited_count=("has_fp_citation", "sum"),
        )
        .reset_index()
    )
    prompt_stats["mention_rate"] = prompt_stats["mentioned_count"] / prompt_stats["total"]
    prompt_stats["cite_rate"] = prompt_stats["cited_count"] / prompt_stats["total"]
    prompt_stats["gap"] = prompt_stats["cite_rate"] - prompt_stats["mention_rate"]
    gap_prompts = prompt_stats[
        (prompt_stats["cited_count"] > 0) & (prompt_stats["gap"] > 0)
    ].sort_values("gap", ascending=False)

    # ------------------------------------------------------------------
    # HEAD-TO-HEAD COMPETITOR COMPARISON
    # ------------------------------------------------------------------
    st.subheader("Head-to-Head vs Competitors")

    # Determine which topic we're looking at for competitor selection
    active_topic = selected_topic if selected_topic != "All" else None
    active_topics = [active_topic] if active_topic else list(TOPIC_COMPETITORS.keys())

    for topic in active_topics:
        rivals = TOPIC_COMPETITORS.get(topic)
        if not rivals:
            continue

        topic_data = filtered[filtered["topic"] == topic] if selected_topic == "All" else filtered
        if topic_data.empty:
            continue

        st.write(f"**{topic}** vs {', '.join(r.title() for r in rivals)}")

        # Compute mention flags for each rival
        rival_cols = {}
        for rival in rivals:
            col_name = f"{rival}_mentioned"
            topic_data = topic_data.copy()
            topic_data[col_name] = (
                topic_data["mentions"].fillna("").str.lower().str.contains(rival)
            )
            rival_cols[rival] = col_name

        h2h = (
            topic_data.groupby(["platform", "prompt"])
            .agg(
                total=("is_mentioned", "size"),
                our_mentions=("is_mentioned", "sum"),
                **{
                    f"{rival}_mentions": (col_name, "sum")
                    for rival, col_name in rival_cols.items()
                },
            )
            .reset_index()
        )
        h2h["our_rate"] = h2h["our_mentions"] / h2h["total"]
        for rival in rivals:
            h2h[f"{rival}_rate"] = h2h[f"{rival}_mentions"] / h2h["total"]

        # Show per-platform summary
        platform_h2h = (
            h2h.groupby("platform")
            .agg(
                total=("total", "sum"),
                our_mentions=("our_mentions", "sum"),
                **{
                    f"{rival}_mentions": (f"{rival}_mentions", "sum")
                    for rival in rivals
                },
            )
            .reset_index()
        )

        rows = []
        for _, row in platform_h2h.iterrows():
            entry = {"Platform": row["platform"]}
            entry[f"{topic} Mention Rate"] = f"{row['our_mentions'] / row['total']:.1%}"
            for rival in rivals:
                entry[f"{rival.title()} Rate"] = (
                    f"{row[f'{rival}_mentions'] / row['total']:.1%}"
                )
            rows.append(entry)

        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

        # Chart: contested prompts where any competitor mentions >= ours
        contested = h2h.copy()
        contested["max_rival"] = contested[[f"{r}_rate" for r in rivals]].max(axis=1)
        contested = contested[contested["max_rival"] >= contested["our_rate"]]
        contested = contested[contested["max_rival"] > 0]

        if not contested.empty:
            contested = contested.sort_values("max_rival", ascending=False).head(15)
            contested["label"] = contested["prompt"].str[:50]

            fig_h2h = go.Figure()
            fig_h2h.add_trace(
                go.Bar(
                    y=contested["label"],
                    x=contested["our_rate"],
                    name=topic,
                    orientation="h",
                    marker_color="#3b82f6",
                )
            )
            colors = ["#ef4444", "#f97316", "#eab308", "#84cc16"]
            for i, rival in enumerate(rivals):
                fig_h2h.add_trace(
                    go.Bar(
                        y=contested["label"],
                        x=contested[f"{rival}_rate"],
                        name=rival.title(),
                        orientation="h",
                        marker_color=colors[i % len(colors)],
                    )
                )

            fig_h2h.update_layout(
                title=f"Contested Prompts — {topic} vs Competitors",
                barmode="group",
                yaxis={
                    "categoryorder": "array",
                    "categoryarray": contested["label"][::-1].tolist(),
                },
                xaxis_title="Mention Rate",
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_h2h, width="stretch")
        else:
            st.success(f"No contested prompts — {topic} leads on all tracked prompts!")

    # ------------------------------------------------------------------
    # MISSED PROMPTS — competitors mentioned, we're not
    # ------------------------------------------------------------------
    st.subheader("Missed Prompts")
    st.caption(
        "Prompts where competitors are mentioned but we're absent — "
        "the biggest opportunities for visibility."
    )

    # Find prompts where we're NOT mentioned but at least one competitor IS
    missed = filtered[~filtered["is_mentioned"] & filtered["comp_mentioned"]].copy()

    if missed.empty:
        st.success("No missed prompts — we appear everywhere competitors do!")
    else:
        # Group by prompt: show which platforms we're missing on and which competitors appear
        missed_summary = (
            missed.groupby("prompt")
            .agg(
                platforms_missed=("platform", lambda x: ", ".join(sorted(x.unique()))),
                competitors=("mentions", lambda x: ", ".join(
                    sorted({
                        name for mentions_str in x.dropna()
                        for name in str(mentions_str).lower().split(",")
                        if name.strip() in COMPETITOR_NAMES
                    })
                )),
                times=("prompt", "size"),
            )
            .reset_index()
            .sort_values("times", ascending=False)
        )

        # Add topic column
        prompt_topics = filtered.groupby("prompt")["topic"].first()
        missed_summary["topic"] = missed_summary["prompt"].map(prompt_topics)

        st.dataframe(
            missed_summary[["topic", "prompt", "platforms_missed", "competitors", "times"]].rename(
                columns={
                    "topic": "Topic",
                    "prompt": "Prompt",
                    "platforms_missed": "Platforms We're Missing On",
                    "competitors": "Competitors Mentioned",
                    "times": "Occurrences",
                }
            ),
            hide_index=True,
            width="stretch",
        )

        # Metric: how many prompts we're missing vs total
        n_missed = missed_summary["prompt"].nunique()
        st.metric(
            "Prompts Where Competitors Appear Without Us",
            f"{n_missed} / {total_prompts}",
        )

    # Insight for competitive landscape
    comp_summary = ""
    if not comp_names.empty:
        top3 = comp_names.value_counts().head(3)
        comp_summary = "\n".join(f"  {n}: {c}x" for n, c in top3.items())
    missed_text = f"Missed prompts: {missed['prompt'].nunique() if not missed.empty else 0}" if not missed.empty else ""
    h2h_text = f"""Mention rate: {prompts_appeared}/{total_prompts} ({prompts_appeared/total_prompts*100:.0f}%)
Per platform: {', '.join(f"{s['Platform']}: {s['Mentioned']}/{s['Total Prompts']}" for s in platform_stats)}
Top competitors:\n{comp_summary}
{missed_text}"""
    render_chart_insight("h2h_competitors", h2h_text, "Where are we losing to competitors and what should we prioritize?")

    # --- Prompt-level detail table ---
    st.subheader("Prompt Detail")

    prompt_detail = (
        filtered.groupby(["prompt", "platform"])
        .agg(
            mentioned=("is_mentioned", lambda x: x.any()),
            best_position=("position", "first"),
        )
        .reset_index()
    )
    prompt_pivot = prompt_detail.pivot(
        index="prompt", columns="platform", values="mentioned"
    ).fillna(False)
    prompt_pivot = prompt_pivot.replace({True: "Yes", False: "No"})
    prompt_pivot["Mentioned On"] = prompt_pivot.apply(
        lambda row: sum(1 for v in row if v == "Yes"), axis=1
    )
    prompt_pivot = prompt_pivot.sort_values("Mentioned On", ascending=False)

    st.dataframe(prompt_pivot, width="stretch")
