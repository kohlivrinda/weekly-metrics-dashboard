"""Growth Diagnostics — cross-source analyses: funnel, content health, momentum,
AI conversion quality, first-touch attribution, keyword intent, GEO causality."""

from datetime import timedelta
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

from config import categorize_page, is_ga4_configured, is_gsc_configured
from db import query_df
from llm import (
    analyse_funnel_quadrants,
    analyse_content_half_life,
    analyse_momentum,
)
from sections.fetch_button import render_fetch_button as _render_fetch_button

# ---------------------------------------------------------------------------
# URL normalisation
# ---------------------------------------------------------------------------

def _to_path(url: str) -> str:
    """Strip protocol+domain from a GSC full URL, or pass GA4 paths through."""
    if url and url.startswith("http"):
        path = urlparse(url).path
    else:
        path = url or "/"
    return path.rstrip("/") or "/"


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def _load_gsc_pages(start: str, end: str) -> pd.DataFrame:
    df = query_df(
        """
        SELECT page,
               SUM(clicks)      AS clicks,
               SUM(impressions) AS impressions,
               AVG(ctr)         AS avg_ctr,
               AVG(position)    AS avg_position
        FROM gsc_page_daily
        WHERE date >= %s AND date <= %s
        GROUP BY page
        """,
        (start, end),
    )
    if df.empty:
        return df
    df["path"] = df["page"].apply(_to_path)
    df["page_category"] = df["path"].apply(categorize_page)
    for col in ["clicks", "impressions"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    for col in ["avg_ctr", "avg_position"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


@st.cache_data(ttl=300)
def _load_ga4_pages(start: str, end: str) -> pd.DataFrame:
    df = query_df(
        """
        SELECT page_path,
               SUM(sessions)               AS sessions,
               SUM(engaged_sessions)       AS engaged_sessions,
               SUM(new_users)              AS new_users,
               SUM(conversions)            AS conversions,
               SUM(exits)                  AS exits,
               SUM(engagement_duration_s)  AS engagement_duration_s
        FROM ga4
        WHERE date >= %s AND date <= %s
        GROUP BY page_path
        """,
        (start, end),
    )
    if df.empty:
        return df
    df["path"] = df["page_path"].apply(_to_path)
    for col in ["sessions", "engaged_sessions", "new_users", "conversions",
                "exits", "engagement_duration_s"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df


@st.cache_data(ttl=300)
def _load_gsc_daily_series(start: str, end: str) -> pd.DataFrame:
    """Daily page-level GSC impressions for half-life analysis."""
    df = query_df(
        """
        SELECT date, page, impressions, clicks
        FROM gsc_page_daily
        WHERE date >= %s AND date <= %s
        ORDER BY page, date
        """,
        (start, end),
    )
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df["path"] = df["page"].apply(_to_path)
    df["page_category"] = df["path"].apply(categorize_page)
    df["impressions"] = pd.to_numeric(df["impressions"], errors="coerce").fillna(0).astype(int)
    df["clicks"] = pd.to_numeric(df["clicks"], errors="coerce").fillna(0).astype(int)
    return df


@st.cache_data(ttl=300)
def _load_ga4_traffic_series(start: str, end: str) -> pd.DataFrame:
    df = query_df(
        """
        SELECT date, session_source, sessions, engaged_sessions, new_users
        FROM ga4_traffic
        WHERE date >= %s AND date <= %s
        ORDER BY date
        """,
        (start, end),
    )
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    for col in ["sessions", "engaged_sessions", "new_users"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df


@st.cache_data(ttl=300)
def _load_gsc_site_series(start: str, end: str) -> pd.DataFrame:
    df = query_df(
        "SELECT date, clicks, impressions, position FROM gsc_site_daily "
        "WHERE date >= %s AND date <= %s ORDER BY date",
        (start, end),
    )
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    for col in ["clicks", "impressions"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df["position"] = pd.to_numeric(df["position"], errors="coerce").fillna(0)
    return df


# ---------------------------------------------------------------------------
# Quadrant helpers
# ---------------------------------------------------------------------------

QUADRANT_META = {
    "Engine":       {"emoji": "✅", "color": "#22c55e", "desc": "High visibility, high engagement — your best content. Protect and scale."},
    "Mismatch":     {"emoji": "⚠️",  "color": "#f59e0b", "desc": "High visibility, low engagement — ranking for wrong queries or content doesn't deliver on the SERP promise."},
    "Hidden Gem":   {"emoji": "💎", "color": "#3b82f6", "desc": "Low visibility, high engagement — great content that needs an SEO push."},
    "Underperformer": {"emoji": "🔴", "color": "#ef4444", "desc": "Low visibility, low engagement — cut, consolidate, or fix."},
}


def _classify_quadrants(merged: pd.DataFrame) -> pd.DataFrame:
    """Add quadrant column using median split on impressions × engagement_rate."""
    m = merged.copy()
    imp_med = m["impressions"].median()
    eng_med = m["engagement_rate"].median()
    def _q(row):
        hi = row["impressions"] >= imp_med
        hq = row["engagement_rate"] >= eng_med
        if hi and hq:
            return "Engine"
        if hi and not hq:
            return "Mismatch"
        if not hi and hq:
            return "Hidden Gem"
        return "Underperformer"
    m["quadrant"] = m.apply(_q, axis=1)
    return m


# ---------------------------------------------------------------------------
# Content trajectory helpers
# ---------------------------------------------------------------------------

def _classify_trajectory(series: pd.Series, n_weeks: int) -> str:
    """Classify impression trajectory from a weekly series."""
    if len(series) < 3:
        return "Insufficient data"
    # Linear regression slope
    x = np.arange(len(series))
    slope, _ = np.polyfit(x, series.values, 1)
    pct_change = slope * len(series) / (series.mean() + 1)
    first_third = series.iloc[:max(1, len(series) // 3)].mean()
    last_third = series.iloc[-max(1, len(series) // 3):].mean()
    peak = series.max()
    if peak > 0 and last_third < peak * 0.4 and first_third < peak * 0.5:
        return "Spike & Decay"
    if pct_change > 0.3:
        return "Growing"
    if pct_change < -0.3:
        return "Declining"
    if series.iloc[0] == 0 and series.iloc[-1] > 0:
        return "Emerging"
    return "Stable"


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _pct(v) -> str:
    return f"{float(v):.1%}"


def _fmt_dur(s) -> str:
    s = max(0, int(s))
    return f"{s // 60}:{s % 60:02d}"


def _bar_h(df, x, y, title, x_fmt=None):
    fig = px.bar(df, x=x, y=y, orientation="h",
                 labels={x: x.replace("_", " ").title(), y: ""},
                 title=title)
    if x_fmt == "pct":
        fig.update_xaxes(tickformat=".0%")
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    return fig


# ---------------------------------------------------------------------------
# Main render
# ---------------------------------------------------------------------------

def render():
    st.header("Growth Diagnostics")

    if is_gsc_configured() or is_ga4_configured():
        _render_fetch_button()

    # Date range selector — default 4 weeks, max 90 days back
    max_end = pd.Timestamp.today().date() - timedelta(days=2)
    default_end = max_end
    default_start = default_end - timedelta(days=27)

    date_range = st.date_input(
        "Analysis window",
        value=(default_start, default_end),
        min_value=default_end - timedelta(days=89),
        max_value=max_end,
        key="diag_dates",
    )
    if not (isinstance(date_range, tuple) and len(date_range) == 2):
        st.info("Pick a start and end date.")
        return

    curr_start, curr_end = date_range
    start_s, end_s = str(curr_start), str(curr_end)
    date_label = f"{curr_start:%b %d} – {curr_end:%b %d, %Y}"

    tabs = st.tabs([
        "1 · Funnel Quadrants",
        "5 · Content Half-Life",
        "8 · Momentum Score",
    ])

    # =========================================================================
    # ANALYSIS 1: GSC → GA4 Funnel Quadrants
    # =========================================================================
    with tabs[0]:
        st.subheader("Search Visibility × Content Quality")
        st.caption(
            "Each page classified by two axes: **search visibility** (GSC impressions) and "
            "**content quality** (GA4 engagement rate). The four quadrants tell you where to act."
        )

        gsc_pages = _load_gsc_pages(start_s, end_s)
        ga4_pages = _load_ga4_pages(start_s, end_s)

        if gsc_pages.empty or ga4_pages.empty:
            st.info("Need both GSC and GA4 data to run this analysis.")
        else:
            # Merge on normalised path
            merged = gsc_pages.merge(ga4_pages, on="path", how="inner")
            # Filter noise: min 50 impressions AND 10 sessions
            merged = merged[(merged["impressions"] >= 50) & (merged["sessions"] >= 10)].copy()

            if merged.empty:
                st.info("Not enough overlapping pages with sufficient traffic.")
            else:
                merged["engagement_rate"] = (
                    merged["engaged_sessions"] / merged["sessions"].replace(0, pd.NA)
                ).fillna(0)
                merged["avg_engagement_s"] = (
                    merged["engagement_duration_s"] / merged["sessions"].replace(0, pd.NA)
                ).fillna(0)
                merged["conv_rate"] = (
                    merged["conversions"] / merged["sessions"].replace(0, pd.NA)
                ).fillna(0)
                merged = _classify_quadrants(merged)
                merged["page_category"] = merged["path"].apply(categorize_page)

                # --- Funnel overview bar ---
                st.markdown("#### Full-funnel overview")
                funnel_stages = []
                total_imp = int(merged["impressions"].sum())
                total_clicks = int(merged["clicks"].sum())
                total_sessions = int(merged["sessions"].sum())
                total_engaged = int(merged["engaged_sessions"].sum())
                total_conv = int(merged["conversions"].sum())

                funnel_df = pd.DataFrame([
                    {"Stage": "Impressions (GSC)",    "Count": total_imp,     "Drop": "—"},
                    {"Stage": "Clicks (GSC)",         "Count": total_clicks,  "Drop": f"{total_clicks/total_imp:.1%} of impressions" if total_imp else "—"},
                    {"Stage": "Sessions (GA4)",       "Count": total_sessions,"Drop": f"{total_sessions/total_clicks:.1%} of clicks" if total_clicks else "—"},
                    {"Stage": "Engaged Sessions",     "Count": total_engaged, "Drop": f"{total_engaged/total_sessions:.1%} of sessions" if total_sessions else "—"},
                    {"Stage": "Conversions",          "Count": total_conv,    "Drop": f"{total_conv/total_sessions:.1%} of sessions" if total_sessions and total_conv else "pending re-fetch"},
                ])
                fig_funnel = px.bar(
                    funnel_df, x="Count", y="Stage", orientation="h",
                    title="Aggregate Funnel: search impression → conversion",
                    text="Drop",
                )
                fig_funnel.update_traces(textposition="outside")
                fig_funnel.update_layout(yaxis={"categoryorder": "array", "categoryarray": funnel_df["Stage"].tolist()[::-1]})
                st.plotly_chart(fig_funnel, use_container_width=True)

                st.divider()

                # --- Quadrant summary ---
                st.markdown("#### Quadrant breakdown")
                for q, meta in QUADRANT_META.items():
                    col1, col2 = st.columns([1, 5])
                    col1.markdown(f"**{meta['emoji']} {q}**")
                    col2.caption(meta["desc"])

                qcounts = merged.groupby("quadrant").agg(
                    pages=("path", "count"),
                    impressions=("impressions", "sum"),
                    sessions=("sessions", "sum"),
                ).reset_index()
                qcounts["quadrant_label"] = qcounts["quadrant"].map(
                    lambda q: f"{QUADRANT_META[q]['emoji']} {q}"
                )

                col1, col2 = st.columns(2)
                with col1:
                    fig_q1 = px.bar(
                        qcounts.sort_values("pages"),
                        x="pages", y="quadrant_label", orientation="h",
                        title="Pages per Quadrant",
                        labels={"pages": "# Pages", "quadrant_label": ""},
                    )
                    st.plotly_chart(fig_q1, use_container_width=True)
                with col2:
                    fig_q2 = px.pie(
                        qcounts, names="quadrant_label", values="sessions",
                        title="Session Share by Quadrant",
                        color="quadrant_label",
                        color_discrete_map={
                            f"{QUADRANT_META[q]['emoji']} {q}": QUADRANT_META[q]["color"]
                            for q in QUADRANT_META
                        },
                    )
                    st.plotly_chart(fig_q2, use_container_width=True)

                st.divider()

                # --- Per-quadrant page tables ---
                st.markdown("#### Pages by quadrant")
                q_tabs = st.tabs([f"{QUADRANT_META[q]['emoji']} {q}" for q in QUADRANT_META])

                for i, (q, meta) in enumerate(QUADRANT_META.items()):
                    with q_tabs[i]:
                        st.caption(meta["desc"])
                        subset = merged[merged["quadrant"] == q].sort_values("impressions", ascending=False)

                        # Top 15 bar
                        top15 = subset.head(15).sort_values("impressions", ascending=True)
                        fig_top = px.bar(
                            top15, x="impressions", y="path", orientation="h",
                            title=f"Top 15 {q} pages by impressions",
                            labels={"impressions": "Impressions", "path": ""},
                        )
                        st.plotly_chart(fig_top, use_container_width=True)

                        # Full table
                        tbl = subset[[
                            "path", "page_category", "impressions", "clicks",
                            "avg_ctr", "avg_position", "sessions",
                            "engagement_rate", "avg_engagement_s",
                        ]].rename(columns={
                            "path": "Page", "page_category": "Category",
                            "impressions": "Impressions", "clicks": "Clicks",
                            "avg_ctr": "CTR", "avg_position": "Avg Position",
                            "sessions": "Sessions", "engagement_rate": "Eng. Rate",
                            "avg_engagement_s": "Avg Time (s)",
                        }).copy()
                        tbl["CTR"] = tbl["CTR"].map(_pct)
                        tbl["Eng. Rate"] = tbl["Eng. Rate"].map(_pct)
                        tbl["Avg Position"] = tbl["Avg Position"].map("{:.1f}".format)
                        tbl["Avg Time (s)"] = tbl["Avg Time (s)"].apply(_fmt_dur)
                        st.dataframe(tbl, use_container_width=True, hide_index=True)

                # --- Analyse button ---
                st.divider()
                anal_key = f"funnel_analysis_{start_s}_{end_s}"
                if st.button("Analyse Quadrants", key=f"btn_{anal_key}", type="primary"):
                    summary_lines = []
                    for q, meta in QUADRANT_META.items():
                        subset = merged[merged["quadrant"] == q]
                        top5 = subset.sort_values("impressions", ascending=False).head(5)
                        pages_str = "; ".join(
                            f"{r['path']} (imp={r['impressions']}, eng={r['engagement_rate']:.0%}, pos={r['avg_position']:.1f})"
                            for _, r in top5.iterrows()
                        )
                        summary_lines.append(
                            f"{meta['emoji']} {q}: {len(subset)} pages, "
                            f"{subset['impressions'].sum():,} total impressions, "
                            f"{subset['sessions'].sum():,} sessions\n"
                            f"  Top pages: {pages_str}"
                        )
                    with st.spinner("Analysing..."):
                        result = analyse_funnel_quadrants(
                            date_range=date_label,
                            quadrant_summaries="\n\n".join(summary_lines),
                        )
                        st.session_state[anal_key] = result

                if anal_key in st.session_state and st.session_state[anal_key]:
                    st.info(st.session_state[anal_key])

    # =========================================================================
    # ANALYSIS 5: Content Half-Life
    # =========================================================================
    with tabs[1]:
        st.subheader("Content Trajectory Analysis")
        st.caption(
            "Each content page classified by how its weekly impressions are trending over the analysis window. "
            "Growing = compounding asset. Declining = losing rank. Spike & Decay = trend content. "
            "Hidden Gems = high engagement but low impressions."
        )

        # Half-life needs more history — extend to 90 days regardless of date picker
        hl_start = str(curr_end - timedelta(days=89))
        gsc_series = _load_gsc_daily_series(hl_start, end_s)

        if gsc_series.empty:
            st.info("No GSC page-level data available.")
        else:
            # Bucket to weekly impressions per page
            gsc_series["week"] = gsc_series["date"].dt.to_period("W-SAT").apply(lambda r: r.start_time)
            weekly = (
                gsc_series.groupby(["path", "page_category", "week"])["impressions"]
                .sum()
                .reset_index()
            )

            # Only pages with at least 4 weeks of data and avg impressions >= 20
            page_stats = weekly.groupby("path").agg(
                total_imp=("impressions", "sum"),
                weeks=("week", "nunique"),
                avg_imp=("impressions", "mean"),
                category=("page_category", "first"),
            ).reset_index()
            eligible = page_stats[(page_stats["weeks"] >= 4) & (page_stats["avg_imp"] >= 20)]

            trajectories = []
            for path in eligible["path"]:
                page_weekly = (
                    weekly[weekly["path"] == path]
                    .sort_values("week")["impressions"]
                    .reset_index(drop=True)
                )
                traj = _classify_trajectory(page_weekly, len(page_weekly))
                meta = eligible[eligible["path"] == path].iloc[0]
                trajectories.append({
                    "path": path,
                    "category": meta["category"],
                    "trajectory": traj,
                    "total_impressions": int(meta["total_imp"]),
                    "avg_weekly_impressions": round(float(meta["avg_imp"]), 1),
                    "weeks_tracked": int(meta["weeks"]),
                })

            traj_df = pd.DataFrame(trajectories)

            if traj_df.empty:
                st.info("Not enough pages with 4+ weeks of data yet.")
            else:
                # Summary counts
                traj_counts = traj_df.groupby("trajectory").agg(
                    pages=("path", "count"),
                    total_impressions=("total_impressions", "sum"),
                ).reset_index().sort_values("pages", ascending=False)

                col1, col2 = st.columns(2)
                with col1:
                    fig_tc = px.bar(
                        traj_counts.sort_values("pages", ascending=True),
                        x="pages", y="trajectory", orientation="h",
                        title="Pages by Trajectory Type",
                        labels={"pages": "# Pages", "trajectory": ""},
                    )
                    st.plotly_chart(fig_tc, use_container_width=True)
                with col2:
                    fig_tp = px.pie(
                        traj_counts, names="trajectory", values="total_impressions",
                        title="Impression Share by Trajectory",
                    )
                    st.plotly_chart(fig_tp, use_container_width=True)

                # Trajectory filter + top pages
                selected_traj = st.selectbox(
                    "Explore trajectory",
                    traj_df["trajectory"].unique().tolist(),
                    key="traj_select",
                )
                subset_traj = traj_df[traj_df["trajectory"] == selected_traj].sort_values(
                    "total_impressions", ascending=False
                )

                # Trend lines for top 5 pages in selected trajectory
                top5_paths = subset_traj.head(5)["path"].tolist()
                trend_data = weekly[weekly["path"].isin(top5_paths)].copy()

                if not trend_data.empty:
                    fig_trend = px.line(
                        trend_data, x="week", y="impressions", color="path",
                        labels={"week": "Week", "impressions": "Impressions", "path": "Page"},
                        title=f"Weekly Impression Trends — Top 5 {selected_traj} pages",
                        markers=True,
                    )
                    st.plotly_chart(fig_trend, use_container_width=True)

                # Table
                tbl_traj = subset_traj[[
                    "path", "category", "avg_weekly_impressions", "total_impressions", "weeks_tracked"
                ]].rename(columns={
                    "path": "Page", "category": "Category",
                    "avg_weekly_impressions": "Avg Weekly Imp.",
                    "total_impressions": "Total Impressions",
                    "weeks_tracked": "Weeks Tracked",
                })
                st.dataframe(tbl_traj, use_container_width=True, hide_index=True)

                # Analyse button
                hl_key = f"halflife_analysis_{selected_traj}_{end_s}"
                if st.button(f"Analyse {selected_traj} Pages", key=f"btn_{hl_key}", type="primary"):
                    traj_text_lines = []
                    for traj_type in traj_df["trajectory"].unique():
                        group = traj_df[traj_df["trajectory"] == traj_type].sort_values(
                            "total_impressions", ascending=False
                        )
                        pages_str = "; ".join(
                            f"{r['path']} (avg={r['avg_weekly_impressions']:.0f} imp/wk, cat={r['category']})"
                            for _, r in group.head(8).iterrows()
                        )
                        traj_text_lines.append(
                            f"{traj_type} ({len(group)} pages): {pages_str}"
                        )
                    with st.spinner("Analysing..."):
                        result = analyse_content_half_life(
                            date_range=f"last 90 days ending {curr_end:%b %d, %Y}",
                            trajectories_text="\n\n".join(traj_text_lines),
                        )
                        st.session_state[hl_key] = result

                if hl_key in st.session_state and st.session_state[hl_key]:
                    st.info(st.session_state[hl_key])

    # =========================================================================
    # ANALYSIS 8: Momentum Score
    # =========================================================================
    with tabs[2]:
        st.subheader("Weekly Growth Momentum")
        st.caption(
            "A composite score across search visibility, organic traffic, and engagement. "
            "Each component is scored as week-over-week % change, then combined into a single "
            "−100 → +100 index. Green = accelerating, red = decelerating."
        )

        # Need 8 weeks of data to compare current vs prior week reliably
        mom_start = str(curr_end - timedelta(days=55))
        gsc_site = _load_gsc_site_series(mom_start, end_s)
        ga4_traffic = _load_ga4_traffic_series(mom_start, end_s)

        if gsc_site.empty and ga4_traffic.empty:
            st.info("No GSC or GA4 site-level data available.")
        else:
            # Weekly buckets
            def _bucket_weekly(df: pd.DataFrame, val_cols: list[str]) -> pd.DataFrame:
                df = df.copy()
                df["week"] = df["date"].dt.to_period("W-SAT").apply(lambda r: r.start_time)
                return df.groupby("week")[val_cols].sum().reset_index()

            metrics_by_week = {}

            if not gsc_site.empty:
                gsc_wk = _bucket_weekly(gsc_site, ["impressions", "clicks"])
                metrics_by_week["GSC Impressions"] = gsc_wk.set_index("week")["impressions"]
                metrics_by_week["GSC Clicks"] = gsc_wk.set_index("week")["clicks"]

            if not ga4_traffic.empty:
                ga4_wk = _bucket_weekly(ga4_traffic, ["sessions", "engaged_sessions", "new_users"])
                metrics_by_week["Sessions"] = ga4_wk.set_index("week")["sessions"]
                metrics_by_week["Engaged Sessions"] = ga4_wk.set_index("week")["engaged_sessions"]
                metrics_by_week["New Users"] = ga4_wk.set_index("week")["new_users"]

            if not metrics_by_week:
                st.info("Not enough data to compute momentum.")
            else:
                mom_df = pd.DataFrame(metrics_by_week).fillna(0)
                mom_df.index = pd.to_datetime(mom_df.index)
                mom_df = mom_df.sort_index()

                # Week-over-week % change per metric
                wow = mom_df.pct_change(fill_method=None).fillna(0) * 100

                # Composite momentum score: mean of all WoW changes, clipped to [-100, 100]
                wow["Momentum Score"] = wow.mean(axis=1).clip(-100, 100)

                # Display current week KPIs with WoW delta
                if len(mom_df) >= 2:
                    latest_week = mom_df.index[-1]
                    prev_week = mom_df.index[-2]
                    st.markdown(f"**Latest complete week:** {latest_week:%b %d} — comparing to {prev_week:%b %d}")
                    metric_cols = st.columns(len(metrics_by_week))
                    for i, metric in enumerate(metrics_by_week):
                        curr_val = int(mom_df.loc[latest_week, metric])
                        delta_pct = wow.loc[latest_week, metric]
                        metric_cols[i].metric(
                            metric,
                            f"{curr_val:,}",
                            delta=f"{delta_pct:+.1f}%",
                        )

                st.divider()

                # Momentum score trend line
                if len(wow) >= 2:
                    wow_plot = wow.reset_index().rename(columns={"index": "week"})
                    fig_mom = px.line(
                        wow_plot, x="week", y="Momentum Score",
                        title="Weekly Momentum Score (composite WoW change)",
                        markers=True,
                        labels={"week": "Week", "Momentum Score": "Score"},
                    )
                    fig_mom.add_hline(y=0, line_dash="dash", line_color="gray")
                    st.plotly_chart(fig_mom, use_container_width=True)

                # Individual metric trends
                wow_melted = wow.drop(columns=["Momentum Score"]).reset_index().rename(
                    columns={"index": "week"}
                ).melt(id_vars="week", var_name="Metric", value_name="WoW Change (%)")

                fig_comp = px.line(
                    wow_melted, x="week", y="WoW Change (%)", color="Metric",
                    title="Week-over-Week Change by Metric",
                    markers=True,
                )
                fig_comp.add_hline(y=0, line_dash="dash", line_color="gray")
                st.plotly_chart(fig_comp, use_container_width=True)

                # Raw numbers table
                st.markdown("**Raw weekly values**")
                raw_display = mom_df.copy()
                raw_display.index = raw_display.index.strftime("%b %d")
                raw_display.index.name = "Week"
                st.dataframe(raw_display.astype(int), use_container_width=True)

                # Analyse button
                mom_key = f"momentum_analysis_{end_s}"
                if st.button("Analyse Momentum", key=f"btn_{mom_key}", type="primary"):
                    if len(mom_df) >= 2:
                        lines = [f"Period: {mom_start} to {end_s}"]
                        for metric in metrics_by_week:
                            vals = mom_df[metric]
                            curr = int(vals.iloc[-1])
                            prev = int(vals.iloc[-2])
                            chg = wow[metric].iloc[-1]
                            lines.append(f"  {metric}: {curr:,} (prev: {prev:,}, WoW: {chg:+.1f}%)")
                        lines.append(f"  Composite Momentum Score: {wow['Momentum Score'].iloc[-1]:+.1f}")
                        with st.spinner("Analysing..."):
                            result = analyse_momentum(
                                date_range=date_label,
                                momentum_data="\n".join(lines),
                            )
                            st.session_state[mom_key] = result

                if mom_key in st.session_state and st.session_state[mom_key]:
                    st.info(st.session_state[mom_key])
