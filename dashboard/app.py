"""
dashboard/app.py — Gourmet AI Dashboard v2
New tabs: Response Manager, Competitor Bench, Trend Alerts, Staff Tracker

Run: streamlit run dashboard/app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import json
from datetime import datetime

st.set_page_config(
    page_title="Gourmet AI",
    page_icon="🍽️",
    layout="wide"
)

# ── Config ────────────────────────────────────────────────────────────────────
DEFAULT_RESTAURANT_ID = "kin_lom_chom_saphan"

def get_results_path(restaurant_id: str, filename: str) -> str:
    return f"data/results/{restaurant_id}/{filename}"

def load_csv_safe(path: str):
    if os.path.exists(path):
        return pd.read_csv(path)
    return None


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/color/96/restaurant.png", width=80)
    st.title("Gourmet AI")
    st.caption("Restaurant Intelligence Platform")
    st.divider()

    # Restaurant selector (multi-restaurant support)
    results_base = "data/results"
    available = [d for d in os.listdir(results_base) if os.path.isdir(f"{results_base}/{d}")] \
        if os.path.exists(results_base) else [DEFAULT_RESTAURANT_ID]
    restaurant_id = st.selectbox("Select Restaurant", available or [DEFAULT_RESTAURANT_ID])

    st.divider()
    st.caption(f"Last updated: {datetime.now().strftime('%d %b %Y %H:%M')}")

    # Manual trigger button
    if st.button("🔄 Run Pipeline Now", use_container_width=True):
        st.info("Pipeline triggered! Results will update shortly.")


# ── Load Data ─────────────────────────────────────────────────────────────────
df_analysis   = load_csv_safe(get_results_path(restaurant_id, "analysis.csv"))
df_keywords   = load_csv_safe(get_results_path(restaurant_id, "keywords.csv"))
df_sug        = load_csv_safe(get_results_path(restaurant_id, "suggestion.csv"))
df_competitors = load_csv_safe(get_results_path(restaurant_id, "competitors.csv"))

# Load trend report
trend_path = get_results_path(restaurant_id, "trend_report.json")
trend_data = json.load(open(trend_path)) if os.path.exists(trend_path) else None

# Load responses
resp_path = get_results_path(restaurant_id, "responses.json")
responses = json.load(open(resp_path)) if os.path.exists(resp_path) else []

if df_analysis is None:
    st.error("❌ No data found. Run the pipeline first: `python pipeline/main.py`")
    st.stop()

# ── KPI Strip ─────────────────────────────────────────────────────────────────
total = len(df_analysis)
bad_count = len(df_analysis[df_analysis['sentiment'] == 'bad'])
bad_pct = round(bad_count / total * 100, 1) if total else 0
good_pct = 100 - bad_pct
top_issue = df_sug.iloc[0]['category'].title() if df_sug is not None and not df_sug.empty else "None"
trend_status = "📈 Spike detected" if trend_data and trend_data.get("spikes_detected", 0) > 0 else "✅ Stable"

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Reviews", f"{total:,}")
k2.metric("Satisfaction", f"{good_pct}%", delta=f"{good_pct - 75:.1f}% vs benchmark")
k3.metric("Critical Issue", top_issue, delta="Needs Fix", delta_color="inverse")
k4.metric("Trend Status", trend_status)
k5.metric("Pending Replies", f"{len([r for r in responses if not r.get('approved')])} drafts")

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tabs = st.tabs([
    "🌍 Overview",
    "📊 Strategy",
    "💬 Reviews",
    "🔑 Keywords",
    "💡 Response Manager",
    "🏆 Competitor Bench",
    "📈 Trend Alerts",
    "👥 Staff Tracker"
])


# ── TAB 1: Overview ───────────────────────────────────────────────────────────
with tabs[0]:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Sentiment Distribution")
        fig = px.pie(df_analysis, names='sentiment', hole=0.5,
                     color='sentiment',
                     color_discrete_map={'good': '#00cc96', 'bad': '#EF553B'})
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.subheader("Issues by Category")
        if df_sug is not None:
            chart = df_sug.groupby("category", as_index=False)["issue_count"].sum()
            fig2 = px.bar(chart, x="issue_count", y="category", orientation='h',
                          color="issue_count", color_continuous_scale="reds")
            st.plotly_chart(fig2, use_container_width=True)

    if df_keywords is not None:
        st.subheader("Top Keywords This Period")
        top5 = df_keywords.head(5)['keyword'].tolist()
        st.info(f"Most talked about: **{', '.join(top5)}**")


# ── TAB 2: Strategy ───────────────────────────────────────────────────────────
with tabs[1]:
    st.header("🚀 Actionable Strategy Cards")
    if df_sug is None or df_sug.empty:
        st.success("No critical issues found!")
    else:
        if 'card_idx' not in st.session_state:
            st.session_state.card_idx = 0
        idx = st.session_state.card_idx
        row = df_sug.iloc[idx]
        st.progress((idx + 1) / len(df_sug), text=f"Card {idx + 1} / {len(df_sug)}")

        with st.container(border=True):
            cc1, cc2 = st.columns([3, 1])
            with cc1:
                st.subheader(f"📂 {row['category'].title()}")
            with cc2:
                color = "red" if row['priority_rank'] == 1 else "orange" if row['priority_rank'] == 2 else "green"
                st.markdown(f":{color}[**Priority #{row['priority_rank']}**]")
            st.divider()
            st.markdown(f"### 💡 {row['suggestion']}")
            m1, m2, m3 = st.columns(3)
            m1.metric("Severity", row['severity_of_issue'])
            m2.metric("Est. Cost", row['resource_cost'])
            m3.metric("Complaints", f"{row['issue_count']} reports")

        col_p, _, col_n = st.columns([1, 5, 1])
        with col_p:
            if st.button("⬅️", use_container_width=True):
                if st.session_state.card_idx > 0:
                    st.session_state.card_idx -= 1
                    st.rerun()
        with col_n:
            if st.button("➡️", use_container_width=True):
                if st.session_state.card_idx < len(df_sug) - 1:
                    st.session_state.card_idx += 1
                    st.rerun()


# ── TAB 3: Reviews ────────────────────────────────────────────────────────────
with tabs[2]:
    col_l, col_r = st.columns([1, 2])
    with col_l:
        cat_filter = st.multiselect("Category", df_analysis['category'].unique())
        sent_filter = st.radio("Sentiment", ["All", "good", "bad"], horizontal=True)
    with col_r:
        filtered = df_analysis.copy()
        if cat_filter:
            filtered = filtered[filtered['category'].isin(cat_filter)]
        if sent_filter != "All":
            filtered = filtered[filtered['sentiment'] == sent_filter]
        st.dataframe(filtered[['sentiment', 'category', 'keywords', 'review_text']], height=600, use_container_width=True)


# ── TAB 4: Keywords ───────────────────────────────────────────────────────────
with tabs[3]:
    if df_keywords is not None:
        c1, c2 = st.columns(2)
        with c1:
            n = st.slider("Top N keywords", 5, 50, 20)
            st.bar_chart(df_keywords.set_index('keyword')['frequency'].head(n))
        with c2:
            fig_tree = px.treemap(df_keywords.head(50), path=['category_type', 'keyword'],
                                  values='frequency', title="Keyword Hierarchy")
            st.plotly_chart(fig_tree, use_container_width=True)


# ── TAB 5: Response Manager ───────────────────────────────────────────────────
with tabs[4]:
    st.header("💡 AI Response Suggestions")
    st.caption("Review and approve AI-generated replies to negative reviews before posting.")

    if not responses:
        st.info("No response drafts yet. Run pipeline with `generate_responses: true` in config.")
    else:
        pending = [r for r in responses if not r.get('approved')]
        approved = [r for r in responses if r.get('approved')]

        st.metric("Pending Approval", len(pending))
        st.metric("Approved", len(approved))
        st.divider()

        for i, resp in enumerate(pending[:5]):
            with st.expander(f"📝 Review: \"{resp.get('original_review', '')[:60]}...\""):
                col_r, col_a = st.columns([3, 1])
                with col_r:
                    st.markdown("**Original Review:**")
                    st.write(resp.get('original_review', ''))
                    st.markdown("**Suggested Reply:**")
                    edited_reply = st.text_area(
                        "Edit before approving:",
                        value=resp.get('suggested_reply', ''),
                        key=f"reply_{i}",
                        height=120
                    )
                with col_a:
                    st.write("")
                    st.write("")
                    if st.button("✅ Approve", key=f"approve_{i}", use_container_width=True):
                        responses[i]['approved'] = True
                        responses[i]['approved_text'] = edited_reply
                        st.success("Approved!")
                    if st.button("🗑️ Discard", key=f"discard_{i}", use_container_width=True):
                        responses[i]['discarded'] = True
                        st.warning("Discarded")


# ── TAB 6: Competitor Benchmarking ───────────────────────────────────────────
with tabs[5]:
    st.header("🏆 Competitor Benchmarking")

    if df_competitors is None:
        st.info("No competitor data yet. Add `latitude` and `longitude` to config and re-run pipeline.")
    else:
        your_rating = round(df_analysis['sentiment'].map({'good': 5, 'bad': 2}).mean(), 2) if df_analysis is not None else 0
        comp_avg = df_competitors['rating'].mean()
        gap = round(your_rating - comp_avg, 2)

        c1, c2, c3 = st.columns(3)
        c1.metric("Your Est. Rating", f"{your_rating}/5")
        c2.metric("Competitor Average", f"{comp_avg:.2f}/5")
        c3.metric("Gap", f"{gap:+.2f}", delta_color="normal" if gap >= 0 else "inverse")

        st.subheader("Competitor Breakdown")
        fig_comp = px.bar(df_competitors, x='name', y='rating',
                          color='rating', color_continuous_scale='RdYlGn',
                          text='rating', title="Competitor Ratings")
        fig_comp.add_hline(y=your_rating, line_dash="dash",
                           annotation_text=f"You: {your_rating}", line_color="blue")
        st.plotly_chart(fig_comp, use_container_width=True)

        st.dataframe(df_competitors[['name', 'rating', 'total_ratings', 'address']],
                     use_container_width=True)


# ── TAB 7: Trend Alerts ───────────────────────────────────────────────────────
with tabs[6]:
    st.header("📈 Weekly Trend Alerts")

    if trend_data is None:
        st.info("No trend data yet. Trends become available after 2+ pipeline runs.")
    else:
        st.metric("Week", trend_data.get("week"))
        st.metric("Spikes Detected", trend_data.get("spikes_detected", 0))
        st.info(trend_data.get("summary", ""))

        spikes = trend_data.get("spikes", [])
        if spikes:
            st.subheader("🚨 Keyword Spikes")
            spike_df = pd.DataFrame(spikes)
            fig_spike = px.bar(spike_df, x='keyword', y='change_pct',
                               color='severity',
                               color_discrete_map={'HIGH': '#EF553B', 'MEDIUM': '#FFA500', 'LOW': '#00cc96'},
                               title="Keyword Change % vs Last Week")
            fig_spike.add_hline(y=30, line_dash="dash", annotation_text="Alert threshold (30%)")
            st.plotly_chart(fig_spike, use_container_width=True)

            for spike in spikes:
                level = "🔴" if spike['severity'] == "HIGH" else "🟡" if spike['severity'] == "MEDIUM" else "🟢"
                st.markdown(f"{level} {spike['alert_message']}")


# ── TAB 8: Staff Tracker ──────────────────────────────────────────────────────
with tabs[7]:
    st.header("👥 Staff Performance Tracker")
    st.caption("Correlates complaint keywords with likely operational causes.")

    if trend_data is None:
        st.info("Staff tracking requires trend data. Run pipeline twice (different weeks).")
    else:
        hints = trend_data.get("staff_hints", [])
        if not hints:
            st.success("✅ No staff-related complaint spikes this week.")
        else:
            st.warning(f"⚠️ {len(hints)} staff-related issue(s) detected this week")
            for hint in hints:
                with st.container(border=True):
                    c1, c2 = st.columns([1, 3])
                    with c1:
                        st.markdown(f"**Dept:** {hint['dept']}")
                        st.markdown(f"**Keyword:** `{hint['keyword']}`")
                    with c2:
                        st.markdown(f"**Likely Cause:** {hint['likely_cause']}")
                        st.markdown(f"**Recommended Action:** {hint['action']}")

        # Week-over-week service keyword chart
        if df_keywords is not None:
            service_kws = ["พนักงาน", "บริการ", "เสิร์ฟ", "ช้า", "รอ", "รอนาน"]
            service_data = df_keywords[df_keywords['keyword'].isin(service_kws)]
            if not service_data.empty:
                st.subheader("Service Keyword Frequency")
                fig_staff = px.bar(service_data, x='keyword', y='frequency',
                                   color='frequency', color_continuous_scale='reds',
                                   title="Service-Related Keywords This Period")
                st.plotly_chart(fig_staff, use_container_width=True)
