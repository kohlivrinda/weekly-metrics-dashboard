"""LLM integration for per-section insight summaries."""

import os

from openai import OpenAI
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

_client = None


def _get_client() -> OpenAI | None:
    global _client
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or api_key == "your-api-key-here":
        return None
    if _client is None:
        _client = OpenAI(api_key=api_key)
    return _client


def get_section_summary(section_name: str, data_summary: str) -> str | None:
    """Generate an LLM insight summary for a dashboard section."""
    client = _get_client()
    if client is None:
        return None

    response = client.chat.completions.create(
        model="gpt-4o",
        max_tokens=1024,
        messages=[
            {
                "role": "user",
                "content": f"""You are a marketing analytics expert reviewing weekly metrics for a B2B SaaS company (Maxim AI / Bifrost).

Analyze the following {section_name} data. Focus on:
- What changed and why it matters (don't just restate the numbers — interpret them)
- Any anomalies or surprising movements worth investigating
- One specific action to take this week

Be concise — 3-5 bullet points max. Write for a marketing team that already sees the tables.

Data:
{data_summary}""",
            }
        ],
    )
    return response.choices[0].message.content


def generate_call_summary(section_summaries: dict[str, str]) -> str | None:
    """Generate a full weekly call summary from all section data.

    Args:
        section_summaries: mapping of section name → data summary text.

    Returns:
        Markdown-formatted call notes, or None if not configured.
    """
    client = _get_client()
    if client is None:
        return None

    combined = "\n\n".join(
        f"## {name}\n{data}" for name, data in section_summaries.items()
    )

    response = client.chat.completions.create(
        model="gpt-4o",
        max_tokens=2048,
        messages=[
            {
                "role": "user",
                "content": f"""You are a marketing analytics expert preparing weekly call notes for a B2B SaaS company (Maxim AI / Bifrost).

Generate a structured weekly metrics call summary from the data below. Format it as markdown with these sections:
1. **Search Performance** — headline impressions number, week-over-week change, top page categories with changes
2. **Traffic Overview** — total sessions, key source changes (google, direct, referral), page category breakdown
3. **GEO Traffic** — AI source traffic (ChatGPT, Claude, Perplexity, Gemini) with changes
4. **Keyword Performance** — ranking highlights, movers (improved/declined), result type distribution
5. **GEO/Profound Performance** — prompt appearance rates per platform, cross-platform overlap, top cited articles
6. **Key Takeaways & Actions** — 3-5 bullet points

Use actual numbers. Be concise. Skip any section where data is not available.

Data:
{combined}""",
            }
        ],
    )
    return response.choices[0].message.content


def analyse_funnel_quadrants(date_range: str, quadrant_summaries: str) -> str | None:
    """Analyse funnel quadrant breakdown to prioritise where to act."""
    client = _get_client()
    if client is None:
        return None

    response = client.chat.completions.create(
        model="gpt-4o",
        max_tokens=1000,
        messages=[{
            "role": "user",
            "content": f"""You are a growth strategist for Maxim AI / Bifrost, a B2B SaaS company building AI/LLM developer tools.

For {date_range}, here is how content pages are distributed across four performance quadrants (search visibility × engagement quality):

{quadrant_summaries}

Quadrant definitions:
- ENGINE: high search visibility + high engagement → working well
- MISMATCH: high search visibility + low engagement → ranking for wrong queries, or content doesn't deliver on the SERP promise
- HIDDEN GEM: low search visibility + high engagement → great content that needs SEO investment
- UNDERPERFORMER: low visibility + low engagement → cut, consolidate, or fix

Analyse:
1. **Where is the biggest pipeline opportunity?** Which quadrant should get resource first and why?
2. **For MISMATCH pages** — what's likely causing low engagement on high-impression pages? What's the fix?
3. **For HIDDEN GEM pages** — what would it take to get these ranking? Internal linking, backlinks, keyword targeting?
4. **3 specific actions this week** ranked by expected impact on pipeline.

Be direct and opinionated. Skip the caveats.""",
        }],
    )
    return response.choices[0].message.content


def analyse_content_half_life(date_range: str, trajectories_text: str) -> str | None:
    """Analyse content trajectory patterns for content strategy decisions."""
    client = _get_client()
    if client is None:
        return None

    response = client.chat.completions.create(
        model="gpt-4o",
        max_tokens=1000,
        messages=[{
            "role": "user",
            "content": f"""You are a content strategist for Maxim AI / Bifrost, a B2B SaaS company building AI/LLM developer tools.

For {date_range}, here are content pages grouped by their impression trajectory over time:

{trajectories_text}

Trajectory types:
- GROWING: impressions consistently increasing — compounding SEO assets
- STABLE: impressions flat — plateaued content
- DECLINING: impressions consistently decreasing — losing relevance or rank
- SPIKE_DECAY: spike then rapid drop — trend content that lost traction
- EMERGING: new content still ramping up

Analyse:
1. **What do the growing pages have in common?** Topic areas, content type, URL structure?
2. **What's causing the declining pages to drop?** Keyword volatility, competitor content, algorithm changes?
3. **Which declining pages are worth saving vs letting go?** Consider traffic volume and strategic importance.
4. **Content production vs optimisation split** — based on this data, what % of content effort should go to new content vs updating existing?
5. **2–3 specific actions** for this week.

Be concrete. Name actual pages where possible.""",
        }],
    )
    return response.choices[0].message.content


def analyse_momentum(date_range: str, momentum_data: str) -> str | None:
    """Analyse weekly momentum score components."""
    client = _get_client()
    if client is None:
        return None

    response = client.chat.completions.create(
        model="gpt-4o",
        max_tokens=800,
        messages=[{
            "role": "user",
            "content": f"""You are a growth analyst for Maxim AI / Bifrost, a B2B SaaS company building AI/LLM developer tools.

Here is the weekly growth momentum data for {date_range}:

{momentum_data}

In 3–4 bullet points: what is the headline story this week? What's accelerating, what's decelerating, and what's the single most important thing to watch next week? Write like you're opening a weekly metrics call.""",
        }],
    )
    return response.choices[0].message.content


def analyse_content_drill_down(
    category: str,
    date_range: str,
    rank_by: str,
    top_pages_text: str,
    bottom_pages_text: str,
    converting_landing_pages_text: str,
) -> str | None:
    """Generate a deep content strategy analysis for a page category drill-down."""
    client = _get_client()
    if client is None:
        return None

    response = client.chat.completions.create(
        model="gpt-4o",
        max_tokens=1500,
        messages=[
            {
                "role": "user",
                "content": f"""You are a content marketing strategist for Maxim AI / Bifrost, a B2B SaaS company building AI/LLM developer tools (observability, testing, evaluation, prompt engineering, and an LLM gateway called Bifrost).

Analyze the **{category}** content performance for {date_range}, ranked by {rank_by}.

**TOP 20 PAGES:**
{top_pages_text}

**BOTTOM 20 PAGES:**
{bottom_pages_text}

**PAGES IN THIS CATEGORY THAT WERE FIRST-TOUCH ON CONVERTING SESSIONS** (entry pages where the visitor also submitted a demo or enterprise form):
{converting_landing_pages_text}

Conversions = demo form or enterprise inquiry form submissions.

Provide a sharp, opinionated analysis:

1. **What top performers have in common** — look at URL patterns, topic specificity, content depth signals. What themes or formats are winning?

2. **What's failing in bottom performers** — be specific. Too generic? Thin content? Wrong audience? Which are worth fixing vs. cutting?

3. **Content → conversion insight** — of the first-touch conversion pages listed, what do they have in common that the rest don't? What does this tell us about what content actually drives pipeline?

4. **3–5 specific actions for this week** — prioritized by expected impact. Be concrete (e.g. "update /articles/X to target Y keyword" not "improve SEO").

Write for a growth team that wants to act on this immediately. No hedging.""",
            }
        ],
    )
    return response.choices[0].message.content


def analyse_dropoff_pages(date_range: str, dropoff_pages_text: str, converting_pages_text: str) -> str | None:
    """Generate analysis of high-exit non-converting pages."""
    client = _get_client()
    if client is None:
        return None

    response = client.chat.completions.create(
        model="gpt-4o",
        max_tokens=1000,
        messages=[
            {
                "role": "user",
                "content": f"""You are a conversion optimization analyst for Maxim AI / Bifrost, a B2B SaaS company building AI/LLM developer tools.

For {date_range}, here are the pages where visitors most frequently exited without converting:

**TOP DROPOFF PAGES** (high exits, zero or near-zero conversions):
{dropoff_pages_text}

**TOP CONVERTING PAGES** (for reference — these are the pages that work):
{converting_pages_text}

Analyze:
1. **Why are users leaving from these pages?** — look at URL patterns to infer content type. Are these pages at the wrong stage of the funnel? Missing a CTA? Wrong audience?

2. **Which of these are fixable vs. structural?** — some pages will never convert (e.g. a very technical reference doc) and that's OK. Others represent real leakage.

3. **2–3 specific CRO actions** — what could be added or changed to reduce dropoff on the fixable pages? Be concrete.

Write for a team that will act on this.""",
            }
        ],
    )
    return response.choices[0].message.content


def render_chart_insight(chart_id: str, data_summary: str, question: str = ""):
    """Render a small insight button next to a chart.

    Args:
        chart_id: Unique key for this chart (e.g. "gsc_trend", "geo_trend").
        data_summary: Text representation of the data shown in the chart.
        question: Optional specific question to focus the analysis on.
    """
    if _get_client() is None:
        return

    cache_key = f"chart_insight_{chart_id}"

    if st.button("Explain this", key=f"btn_{chart_id}", type="tertiary"):
        with st.spinner("Analyzing..."):
            prompt = question or "What's the key takeaway from this data?"
            result = get_section_summary(prompt, data_summary)
            st.session_state[cache_key] = result

    if cache_key in st.session_state and st.session_state[cache_key]:
        st.info(st.session_state[cache_key])
