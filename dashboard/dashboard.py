"""
dashboard/dashboard.py

Real-time fraud monitoring dashboard backed by the predictions table.

Run:
    streamlit run dashboard/dashboard.py
"""

import os
import time
from datetime import datetime

import pandas as pd
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


@st.cache_resource
def _get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(_DSN)


def query(sql: str) -> pd.DataFrame:
    conn = _get_conn()
    try:
        return pd.read_sql(sql, conn)
    except Exception:
        _get_conn.clear()
        return pd.read_sql(sql, _get_conn())


# ─────────────────────────────────────────────────────────────────
st.set_page_config(page_title="fraud-stream", layout="wide", page_icon="🛡️")
st.title("🛡️ fraud-stream · Live Monitor")
st.caption(f"Refreshes every {REFRESH_S}s · last updated {datetime.now().strftime('%H:%M:%S')}")

# ── TPS (last 60 s) ───────────────────────────────────────────────
tps_row = query("""
    SELECT COUNT(*) AS cnt,
           EXTRACT(EPOCH FROM (MAX(created_at) - MIN(created_at))) AS span_s
    FROM predictions
    WHERE created_at >= NOW() - INTERVAL '60 seconds'
""").iloc[0]

span = float(tps_row["span_s"] or 1)
tps  = float(tps_row["cnt"]) / span if span > 0 else 0.0

# ── Summary metrics (last 10 min) ────────────────────────────────
summary = query("""
    SELECT
        SUM(is_fraud::int)                                            AS fraud_count,
        SUM(isolation_forest_flag::int)                               AS if_anomalies,
        ROUND(AVG(is_fraud::int) * 100, 2)                           AS fraud_pct,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms)     AS p50,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)     AS p95,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)     AS p99
    FROM predictions
    WHERE created_at >= NOW() - INTERVAL '10 minutes'
""").iloc[0]

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("TPS (60 s)",           f"{tps:.2f}")
c2.metric("LightGBM fraud (10 m)", f"{summary['fraud_pct']:.2f}%")
c3.metric("IF anomalies (10 m)",  f"{int(summary['if_anomalies'] or 0)}")
c4.metric("p50 latency",          f"{summary['p50']:.1f} ms")
c5.metric("p95 latency",          f"{summary['p95']:.1f} ms")
c6.metric("p99 latency",          f"{summary['p99']:.1f} ms")

st.divider()

# ── Fraud rate over time ───────────────────────────────────────────
fraud_ts = query("""
    SELECT
        date_trunc('minute', created_at)          AS minute,
        ROUND(AVG(is_fraud::int) * 100, 2)        AS fraud_pct,
        ROUND(AVG(isolation_forest_flag::int) * 100, 2) AS if_pct,
        COUNT(*)                                  AS volume
    FROM predictions
    WHERE created_at >= NOW() - INTERVAL '30 minutes'
    GROUP BY 1
    ORDER BY 1
""")

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Fraud rate % — LightGBM vs Isolation Forest (30 min)")
    if fraud_ts.empty:
        st.info("No data yet.")
    else:
        st.line_chart(fraud_ts.set_index("minute")[["fraud_pct", "if_pct"]])

with col_right:
    st.subheader("Transaction volume (30 min)")
    if not fraud_ts.empty:
        st.bar_chart(fraud_ts.set_index("minute")["volume"])

st.divider()

# ── Feature drift — KS test on amount ────────────────────────────
# KS test: no normality assumption, compares full CDF shape.
# p < 0.05 → distributions differ significantly → flag drift.
st.subheader("Feature drift — amount distribution (KS test, 1-hour windows)")

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
    st.info("Need data in both hourly windows to run drift check.")
else:
    recent   = drift_df[drift_df["window"] == "recent_1h"]["amount"].dropna()
    previous = drift_df[drift_df["window"] == "previous_1h"]["amount"].dropna()

    ks_stat, p_val = stats.ks_2samp(recent, previous)

    d1, d2, d3 = st.columns(3)
    d1.metric("KS statistic", f"{ks_stat:.4f}", help="0 = identical distributions, 1 = completely different")
    d2.metric("p-value",      f"{p_val:.4f}",   help="< 0.05 signals a statistically significant shift")
    d3.metric("Samples",      f"{len(recent)} / {len(previous)}", help="recent / previous window")

    if p_val < 0.05:
        st.warning(f"⚠️ Drift detected (p={p_val:.4f}). Amount distribution shifted between windows.")
    else:
        st.success(f"✅ No significant drift (p={p_val:.4f} ≥ 0.05).")

    summary_tbl = pd.DataFrame({
        "recent_1h":   recent.describe(),
        "previous_1h": previous.describe(),
    }).loc[["mean", "50%", "std", "max"]]
    summary_tbl.index = ["mean", "median", "std", "max"]
    st.dataframe(summary_tbl.style.format("{:.2f}"), use_container_width=True)

# ── Auto-refresh ───────────────────────────────────────────────────
time.sleep(REFRESH_S)
st.rerun()
