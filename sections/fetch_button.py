"""Shared fetch button for GSC + GA4 data."""

import streamlit as st

from config import is_ga4_configured, is_gsc_configured


def render_fetch_button():
    """Render a single fetch button that pulls both GSC and GA4 data.

    Disables itself after being clicked.
    """
    already_fetched = st.session_state.get("fetch_done", False)

    col_btn, col_status = st.columns([1, 3])
    with col_btn:
        fetch_clicked = st.button(
            "Fetch GSC + GA4 Data",
            type="primary",
            disabled=already_fetched,
            key="fetch_gsc_ga4",
        )

    if fetch_clicked:
        st.session_state["fetch_done"] = True
        with col_status:
            if is_gsc_configured():
                with st.spinner("Fetching GSC data..."):
                    try:
                        from google_api import fetch_gsc_data
                        path, existed = fetch_gsc_data()
                        if existed:
                            st.info(f"GSC: already exists `{path}`")
                        else:
                            st.success(f"GSC: saved to `{path}`")
                    except Exception as e:
                        st.error(f"GSC: {e}")

            if is_ga4_configured():
                with st.spinner("Fetching GA4 data..."):
                    try:
                        from google_api import fetch_ga4_data
                        path, existed = fetch_ga4_data()
                        if existed:
                            st.info(f"GA4: already exists `{path}`")
                        else:
                            st.success(f"GA4: saved to `{path}`")
                    except Exception as e:
                        st.error(f"GA4: {e}")
            st.rerun()

    if already_fetched:
        with col_status:
            st.caption("Data fetched this session.")
