-- db/init.sql
-- Runs automatically on first Postgres container start.
-- Idempotent — safe to re-run.

CREATE TABLE IF NOT EXISTS predictions (
    id               SERIAL PRIMARY KEY,
    transaction_id   VARCHAR(64)   NOT NULL,
    features         JSONB         NOT NULL,
    fraud_probability FLOAT        NOT NULL CHECK (fraud_probability BETWEEN 0 AND 1),
    is_fraud         BOOLEAN       NOT NULL,
    latency_ms       FLOAT         NOT NULL,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Dashboard queries are almost always "last N minutes"
CREATE INDEX IF NOT EXISTS idx_predictions_created_at
    ON predictions (created_at DESC);

-- Partial index — only indexes the rare fraud=TRUE rows
CREATE INDEX IF NOT EXISTS idx_predictions_is_fraud
    ON predictions (is_fraud)
    WHERE is_fraud = TRUE;

-- Convenience view used by the Streamlit dashboard
CREATE OR REPLACE VIEW fraud_summary AS
SELECT
    COUNT(*)                                             AS total_scored,
    SUM(is_fraud::INT)                                   AS total_fraud,
    ROUND(AVG(fraud_probability::NUMERIC) * 100, 2)     AS avg_fraud_prob_pct,
    ROUND(AVG(latency_ms::NUMERIC), 2)                  AS avg_latency_ms,
    MAX(created_at)                                      AS last_scored_at
FROM predictions;
