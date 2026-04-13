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
    """Generate an LLM insight summary for a dashboard section.

    Args:
        section_name: e.g. "Search Impressions", "GEO Performance"
        data_summary: A text representation of the key metrics/data for this section.

    Returns:
        The LLM-generated insight string, or None if the API key isn't configured.
    """
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

Analyze the following {section_name} data and provide:
1. Key takeaways (2-3 bullet points)
2. Notable trends or anomalies
3. One actionable recommendation

Be concise and specific. Reference actual numbers from the data.

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


def render_llm_insights(section_name: str, data_summary: str):
    """Render LLM insights in a Streamlit expander."""
    with st.expander("AI Insights", expanded=False):
        if _get_client() is None:
            st.info("Set `OPENAI_API_KEY` in `.env` to enable AI-powered insights.")
            return

        if st.button(f"Generate insights", key=f"llm_{section_name}"):
            with st.spinner("Analyzing..."):
                summary = get_section_summary(section_name, data_summary)
                if summary:
                    st.markdown(summary)
                else:
                    st.warning("Failed to generate insights.")
