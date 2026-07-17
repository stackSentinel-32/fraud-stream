import os
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from dotenv import load_dotenv
from scipy import stats

load_dotenv()

_DSN = (
    f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
    f"port={os.getenv('POSTGRES_PORT', '5432')} "
    f"dbname={os.getenv('POSTGRES_DB', 'frauddb')} "
    f"user={os.getenv('POSTGRES_USER', 'fraud_user')} "
    f"password={os.getenv('POSTGRES_PASSWORD', '')}"
)
REFRESH_S = int(os.getenv("DASHBOARD_REFRESH_S", "5"))

# ── Page config ────────────────────────────────────────────────────
st.set_page_config(
    page_title="fraud-stream · Live Monitor",
    layout="wide",
    page_icon="🛡️",
    initial_sidebar_state="collapsed",
)

# ── Theme Configuration ──────────────────────────────────────────────
THEMES = {
    "Dark (Black)": {
        "bg": "linear-gradient(135deg, #0a0e1a 0%, #0d1526 50%, #0a0e1a 100%)",
        "card_bg": "rgba(17, 25, 40, 0.80)",
        "card_border": "rgba(99, 179, 237, 0.15)",
        "card_border_hover": "rgba(99, 179, 237, 0.45)",
        "text_main": "#e2e8f0",
        "text_dim": "#64748b",
        "text_header": "#f1f5f9",
        "text_subheader": "#cbd5e1",
        "grid_color": "rgba(99, 179, 237, 0.08)",
        "plotly_font": "#94a3b8"
    },
    "Light (White)": {
        "bg": "#f8fafc",
        "card_bg": "rgba(255, 255, 255, 0.80)",
        "card_border": "rgba(0, 0, 0, 0.1)",
        "card_border_hover": "rgba(0, 0, 0, 0.3)",
        "text_main": "#0f172a",
        "text_dim": "#475569",
        "text_header": "#020617",
        "text_subheader": "#334155",
        "grid_color": "rgba(0, 0, 0, 0.08)",
        "plotly_font": "#475569"
    },
    "Eye Protection": {
        "bg": "#fbf0d9",
        "card_bg": "rgba(244, 236, 216, 0.80)",
        "card_border": "rgba(163, 137, 108, 0.3)",
        "card_border_hover": "rgba(163, 137, 108, 0.6)",
        "text_main": "#433422",
        "text_dim": "#7c664d",
        "text_header": "#2b2116",
        "text_subheader": "#5c4b37",
        "grid_color": "rgba(163, 137, 108, 0.15)",
        "plotly_font": "#7c664d"
    }
}

saved_theme = st.query_params.get("theme", "Dark (Black)")
if saved_theme not in THEMES:
    saved_theme = "Dark (Black)"
t = THEMES[saved_theme]

# ── Premium CSS ────────────────────────────────────────────────────
st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    /* Hide Streamlit chrome */
    #MainMenu {{ visibility: hidden; }}
    footer {{ visibility: hidden; }}
    header {{ visibility: hidden; }}
    [data-testid="stDeployButton"] {{ display: none; }}

    /* Global font */
    html, body, [class*="css"] {{
        font-family: 'Inter', sans-serif;
    }}

    /* Background */
    .stApp {{
        background: {t['bg']};
    }}

    /* Compact container */
    .block-container {{
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        max-width: 1400px;
    }}

    /* Metric cards */
    [data-testid="stMetric"] {{
        background: {t['card_bg']};
        border: 1px solid {t['card_border']};
        border-radius: 14px;
        padding: 18px 22px;
        backdrop-filter: blur(12px);
        transition: border-color 0.3s ease;
    }}
    [data-testid="stMetric"]:hover {{
        border-color: {t['card_border_hover']};
    }}

    /* Metric label */
    [data-testid="stMetricLabel"] {{
        font-size: 0.68rem !important;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: {t['text_dim']} !important;
    }}

    /* Metric value */
    [data-testid="stMetricValue"] {{
        font-size: 1.75rem !important;
        font-weight: 700;
        color: {t['text_main']} !important;
        letter-spacing: -0.03em;
    }}

    /* Delta value */
    [data-testid="stMetricDelta"] {{
        font-size: 0.72rem !important;
        font-weight: 500;
    }}

    /* Title */
    h1 {{
        font-weight: 800;
        letter-spacing: -0.04em;
        color: {t['text_header']} !important;
    }}

    /* Subheaders */
    h2, h3 {{
        font-weight: 600;
        letter-spacing: -0.02em;
        color: {t['text_subheader']} !important;
    }}

    /* Dividers */
    hr {{
        margin: 1.5rem 0;
        border-color: {t['card_border']};
    }}

    /* Selectbox */
    [data-testid="stSelectbox"] label {{
        font-size: 0.75rem !important;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: {t['text_dim']} !important;
    }}

    /* Dataframe table */
    [data-testid="stDataFrame"] {{
        border-radius: 12px;
        overflow: hidden;
    }}

    /* Info / warning / success boxes */
    [data-testid="stAlert"] {{
        border-radius: 10px;
    }}

    /* Normal cursors everywhere */
    * {{ cursor: default !important; }}
    [data-baseweb="select"], [data-baseweb="select"] *,
    button, [role="option"], [role="button"],
    [data-testid="stSelectbox"], label {{ cursor: pointer !important; }}
    .js-plotly-plot .plotly, .js-plotly-plot .plotly * {{ cursor: crosshair !important; }}
</style>
""", unsafe_allow_html=True)

# ── DB helpers ─────────────────────────────────────────────────────

@st.cache_resource
def _get_conn() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(_DSN)
    conn.autocommit = True
    return conn


def query(sql: str) -> pd.DataFrame:
    conn = _get_conn()
    try:
        return pd.read_sql(sql, conn)
    except Exception:
        _get_conn.clear()
        return pd.read_sql(sql, _get_conn())


try:
    saved_rate = int(st.query_params.get("refresh", "5"))
except ValueError:
    saved_rate = 5

# ── Header ─────────────────────────────────────────────────────────────────
title_col, ts_col = st.columns([3, 1])
with title_col:
    st.title("🛡️ fraud-stream · Live Monitor")
with ts_col:
    refresh_label = "Paused" if saved_rate == 0 else f"{saved_rate}s"
    st.markdown(
        f"<div style='text-align:right; color:#475569; font-size:0.8rem; padding-top:1.5rem;'>"
        f"🔴 LIVE &nbsp;·&nbsp; Updated {datetime.now().strftime('%H:%M:%S')}<br>"
        f"Auto-refreshes every {refresh_label}</div>",
        unsafe_allow_html=True
    )

# ── Batched DB query (single round-trip for all KPIs) ──────────────
kpi = query("""
    SELECT
        COUNT(*)                                                          AS total_60s,
        EXTRACT(EPOCH FROM (MAX(created_at) - MIN(created_at)))          AS span_s,
        SUM(is_fraud::int)                                               AS fraud_count,
        SUM(isolation_forest_flag::int)                                  AS if_anomalies,
        ROUND(AVG(is_fraud::int) * 100, 2)                              AS fraud_pct,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms)        AS p50,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)        AS p95,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)        AS p99
    FROM predictions
    WHERE created_at >= NOW() - INTERVAL '10 minutes'
""").iloc[0]

tps_row = query("""
    SELECT COUNT(*) AS cnt,
           EXTRACT(EPOCH FROM (MAX(created_at) - MIN(created_at))) AS span_s
    FROM predictions
    WHERE created_at >= NOW() - INTERVAL '60 seconds'
""").iloc[0]

span = float(tps_row["span_s"] or 0)
tps  = float(tps_row["cnt"]) / span if span > 0.5 else 0.0

fraud_pct  = float(kpi["fraud_pct"]  or 0)
if_anom    = int(kpi["if_anomalies"] or 0)
p50        = float(kpi["p50"] or 0)
p95        = float(kpi["p95"] or 0)
p99        = float(kpi["p99"] or 0)

# ── KPI Metric Cards ───────────────────────────────────────────────
c1, c2, c3, c4, c5, c6 = st.columns(6)

c1.metric(
    "TPS (60 s)", f"{tps:.2f}",
    delta="streaming" if tps > 0 else "idle",
    delta_color="normal" if tps > 0 else "off",
)
c2.metric(
    "LightGBM Fraud %", f"{fraud_pct:.2f}%",
    delta="🔴 HIGH" if fraud_pct > 5 else "🟢 NORMAL",
    delta_color="inverse" if fraud_pct > 5 else "off",
)
c3.metric(
    "IF Anomalies (10 m)", f"{if_anom}",
    delta="🟡 CHECK" if if_anom > 50 else "🟢 OK",
    delta_color="inverse" if if_anom > 50 else "off",
)
c4.metric(
    "p50 Latency", f"{p50:.2f} ms",
    delta="fast" if p50 < 5 else "slow",
    delta_color="normal" if p50 < 5 else "inverse",
)
c5.metric(
    "p95 Latency", f"{p95:.2f} ms",
    delta="fast" if p95 < 10 else "slow",
    delta_color="normal" if p95 < 10 else "inverse",
)
c6.metric(
    "p99 Latency", f"{p99:.2f} ms",
    delta="fast" if p99 < 20 else "slow",
    delta_color="normal" if p99 < 20 else "inverse",
)

st.divider()

res_col, win_col, theme_col, refresh_col = st.columns([1, 1, 1, 1])
with res_col:
    res_options = ["5 sec", "10 sec", "30 sec", "1 min", "5 min", "10 min", "30 min"]
    saved_res = st.query_params.get("res", "1 min")
    saved_res = saved_res if saved_res in res_options else "1 min"
    resolution = st.selectbox(
        "Chart Resolution",
        options=res_options,
        index=res_options.index(saved_res),
    )
    if resolution != saved_res:
        st.query_params["res"] = resolution
        st.rerun()
with win_col:
    win_options = ["10 min", "30 min", "1 hour", "3 hours"]
    saved_win = st.query_params.get("win", "30 min")
    saved_win = saved_win if saved_win in win_options else "30 min"
    window = st.selectbox(
        "Time Window",
        options=win_options,
        index=win_options.index(saved_win),
    )
    if window != saved_win:
        st.query_params["win"] = window
        st.rerun()
with theme_col:
    theme_options = list(THEMES.keys())
    selected_theme = st.selectbox(
        "Color Theme",
        options=theme_options,
        index=theme_options.index(saved_theme),
    )
    if selected_theme != saved_theme:
        st.query_params["theme"] = selected_theme
        st.rerun()
with refresh_col:
    refresh_options = {
        "Off (Paused)": 0,
        "5 seconds": 5,
        "10 seconds": 10,
        "30 seconds": 30,
        "1 minute": 60,
    }
    refresh_by_seconds = {v: k for k, v in refresh_options.items()}
    saved_label = refresh_by_seconds.get(saved_rate, "5 seconds")
    saved_idx = list(refresh_options.keys()).index(saved_label)
    selected_refresh = st.selectbox(
        "Auto Refresh Rate",
        options=list(refresh_options.keys()),
        index=saved_idx,
    )
    current_refresh_rate = refresh_options[selected_refresh]
    if current_refresh_rate != saved_rate:
        st.query_params["refresh"] = str(current_refresh_rate)
        st.rerun()

ALLOWED_RES_SECONDS = {
    "5 sec": 5, "10 sec": 10, "30 sec": 30,
    "1 min": 60, "5 min": 300, "10 min": 600, "30 min": 1800
}
ALLOWED_WIN_INTERVALS = {
    "10 min": "10 minutes", "30 min": "30 minutes",
    "1 hour": "1 hour", "3 hours": "3 hours"
}

res_seconds  = ALLOWED_RES_SECONDS.get(resolution, 60)
win_interval = ALLOWED_WIN_INTERVALS.get(window, "30 minutes")

fraud_ts = query(f"""
    SELECT
        TO_TIMESTAMP(FLOOR((EXTRACT('epoch' FROM created_at) / {res_seconds})) * {res_seconds}) AS time_bucket,
        ROUND(AVG(is_fraud::int) * 100, 2)              AS fraud_pct,
        ROUND(AVG(isolation_forest_flag::int) * 100, 2) AS if_pct,
        COUNT(*)                                         AS volume
    FROM predictions
    WHERE created_at >= NOW() - INTERVAL '{win_interval}'
    GROUP BY 1
    ORDER BY 1
""")

# ── Plotly Charts ──────────────────────────────────────────────────
col_left, col_right = st.columns(2)

PLOTLY_LAYOUT = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter", color=t['plotly_font'], size=11),
    margin=dict(l=0, r=0, t=10, b=0),
    xaxis=dict(gridcolor=t['grid_color'], zeroline=False),
    yaxis=dict(gridcolor=t['grid_color'], zeroline=False),
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        font=dict(color=t['plotly_font'], size=11),
        orientation="h", x=0, y=-0.2
    ),
    hovermode="x unified",
)

with col_left:
    st.subheader("📈 Fraud Rate % — LightGBM vs Isolation Forest")
    if fraud_ts.empty:
        st.info("No data yet — waiting for transactions...")
    else:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=fraud_ts["time_bucket"], y=fraud_ts["fraud_pct"],
            mode="lines",
            name="LightGBM Fraud %",
            line=dict(color="#f43f5e", width=2.5),
            fill="tozeroy",
            fillcolor="rgba(244, 63, 94, 0.12)",
            hovertemplate="%{y:.2f}%<extra>LightGBM</extra>",
        ))
        fig.add_trace(go.Scatter(
            x=fraud_ts["time_bucket"], y=fraud_ts["if_pct"],
            mode="lines",
            name="Isolation Forest %",
            line=dict(color="#818cf8", width=2.5, dash="dot"),
            fill="tozeroy",
            fillcolor="rgba(129, 140, 248, 0.08)",
            hovertemplate="%{y:.2f}%<extra>Isolation Forest</extra>",
        ))
        fig.update_layout(**PLOTLY_LAYOUT, height=280)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

with col_right:
    st.subheader("📊 Transaction Volume")
    if fraud_ts.empty:
        st.info("No data yet — waiting for transactions...")
    else:
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=fraud_ts["time_bucket"], y=fraud_ts["volume"],
            name="Volume",
            marker=dict(
                color=fraud_ts["volume"],
                colorscale=[[0, "#1e40af"], [0.5, "#3b82f6"], [1, "#38bdf8"]],
                showscale=False,
            ),
            hovertemplate="%{y} txns<extra></extra>",
        ))
        fig2.update_layout(**PLOTLY_LAYOUT, height=280, bargap=0.2)
        st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})

st.divider()

# ── p99 Latency Gauge + KS Drift ──────────────────────────────────
gauge_col, drift_col = st.columns([1, 2])

with gauge_col:
    st.subheader("⚡ p99 API Latency")
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=p99,
        number=dict(suffix=" ms", font=dict(color="#e2e8f0", size=28)),
        delta=dict(reference=20, valueformat=".1f", suffix=" ms"),
        gauge=dict(
            axis=dict(range=[0, 100], tickcolor="#475569", tickfont=dict(color="#475569")),
            bar=dict(color="#38bdf8"),
            bgcolor="rgba(0,0,0,0)",
            borderwidth=0,
            steps=[
                dict(range=[0, 5],   color="rgba(16, 185, 129, 0.2)"),
                dict(range=[5, 20],  color="rgba(251, 191, 36, 0.2)"),
                dict(range=[20, 100], color="rgba(244, 63, 94, 0.2)"),
            ],
            threshold=dict(line=dict(color="#f43f5e", width=3), thickness=0.75, value=20),
        ),
        title=dict(text="<span style='font-size:0.8em;color:#64748b'>GREEN < 5ms | AMBER < 20ms | RED ≥ 20ms</span>"),
    ))
    fig_gauge.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color=t['plotly_font']),
        height=260,
        margin=dict(l=20, r=20, t=30, b=10),
    )
    st.plotly_chart(fig_gauge, use_container_width=True, config={"displayModeBar": False})

with drift_col:
    st.subheader("🧪 Feature Drift — KS Test (Amount Distribution)")
    drift_df = query("""
        SELECT
            CASE WHEN created_at >= NOW() - INTERVAL '1 hour'
                 THEN 'recent_1h' ELSE 'previous_1h' END AS window,
            (features->>'amount')::float                  AS amount
        FROM predictions
        WHERE created_at >= NOW() - INTERVAL '2 hours'
          AND features->>'amount' IS NOT NULL
    """)

    if drift_df.empty or drift_df["window"].nunique() < 2:
        st.info("Need data in both 1-hour windows to run drift check. Check back after ~1 hour of streaming.")
    else:
        recent   = drift_df[drift_df["window"] == "recent_1h"]["amount"].dropna()
        previous = drift_df[drift_df["window"] == "previous_1h"]["amount"].dropna()
        ks_stat, p_val = stats.ks_2samp(recent, previous)

        d1, d2, d3 = st.columns(3)
        d1.metric("KS Statistic", f"{ks_stat:.4f}", help="0 = identical, 1 = completely different")
        d2.metric("p-value",      f"{p_val:.4f}",   help="< 0.05 = statistically significant drift")
        d3.metric("Sample Sizes", f"{len(recent):,} / {len(previous):,}", help="recent / previous window")

        if p_val < 0.05:
            st.warning(f"⚠️ **Drift Detected** — p={p_val:.4f}. The transaction amount distribution has shifted. Consider retraining.")
        else:
            st.success(f"✅ **No Significant Drift** — p={p_val:.4f} ≥ 0.05. Distributions are stable.")

        summary_tbl = pd.DataFrame({
            "recent_1h":   recent.describe(),
            "previous_1h": previous.describe(),
        }).loc[["mean", "50%", "std", "max"]]
        summary_tbl.index = ["Mean", "Median", "Std Dev", "Max"]
        st.dataframe(summary_tbl.style.format("${:.2f}"), use_container_width=True)

st.divider()

# ── Recent Fraudulent Transactions Table ───────────────────────────
st.subheader("🚨 Recent Fraudulent Transactions")

fraud_table = query("""
    SELECT
        LEFT(transaction_id, 12)                        AS txn_id,
        ROUND(fraud_probability::numeric * 100, 1)      AS fraud_prob_pct,
        isolation_forest_flag                           AS if_flag,
        CASE WHEN isolation_forest_flag THEN 'Guaranteed Scam' ELSE 'Stealthy Fraud' END AS classification,
        ROUND(latency_ms::numeric, 2)                   AS latency_ms,
        TO_CHAR(created_at, 'HH24:MI:SS')               AS time
    FROM predictions
    WHERE is_fraud = TRUE
    ORDER BY created_at DESC
    LIMIT 50
""")

if fraud_table.empty:
    st.info("No fraudulent transactions detected yet.")
else:
    st.caption(f"Showing {len(fraud_table)} most recent fraud detections")

    def style_table(df: pd.DataFrame):
        def highlight_if(val):
            return "color: #fb923c; font-weight: 600" if val else "color: #475569"
        def highlight_prob(val):
            if val >= 90:
                return "color: #f43f5e; font-weight: 700"
            elif val >= 70:
                return "color: #fb923c; font-weight: 600"
            return "color: #fbbf24"
        def highlight_class(val):
            if 'Guaranteed Scam' in str(val):
                return "color: #f43f5e; font-weight: 700"
            return "color: #fb923c; font-weight: 600"
            
        return df.style\
            .map(highlight_if, subset=["if_flag"])\
            .map(highlight_prob, subset=["fraud_prob_pct"])\
            .map(highlight_class, subset=["classification"])\
            .format({"fraud_prob_pct": "{:.1f}%", "latency_ms": "{:.2f} ms"})

    st.dataframe(
        style_table(fraud_table),
        use_container_width=True,
        hide_index=True,
        column_config={
            "txn_id":        st.column_config.TextColumn("Transaction ID"),
            "fraud_prob_pct": st.column_config.NumberColumn("Fraud Probability", format="%.1f%%"),
            "if_flag":       st.column_config.CheckboxColumn("IF Anomaly"),
            "classification": st.column_config.TextColumn("Classification"),
            "latency_ms":    st.column_config.NumberColumn("Latency", format="%.2f ms"),
            "time":          st.column_config.TextColumn("Time"),
        }
    )

# ── Auto-refresh via st.fragment (no sleep, no UI glitches) ────────
if current_refresh_rate > 0:
    from datetime import timedelta

    @st.fragment(run_every=timedelta(seconds=current_refresh_rate))
    def _auto_refresh():
        st.rerun()

    _auto_refresh()
