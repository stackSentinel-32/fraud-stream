-- ─────────────────────────────────────────────────────────────────
-- fraud-stream · Database Initialization
--
-- This file runs automatically on first Postgres container startup
-- (mounted at /docker-entrypoint-initdb.d/).
-- It is idempotent: safe to run multiple times due to IF NOT EXISTS.
-- ─────────────────────────────────────────────────────────────────

-- ── Transactions table ────────────────────────────────────────────
-- Stores every transaction that flows through the scoring pipeline.
-- JSONB for raw_features allows flexible schema as features evolve
-- without requiring a migration — useful during active development.
CREATE TABLE IF NOT EXISTS transactions (
    id               SERIAL PRIMARY KEY,
    transaction_id   VARCHAR(64)   NOT NULL,
    amount           FLOAT         NOT NULL,
    fraud_probability FLOAT        NOT NULL CHECK (fraud_probability BETWEEN 0 AND 1),
    is_fraud         BOOLEAN       NOT NULL,
    scored_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    raw_features     JSONB                              -- full feature vector for audit/retraining
);

-- ── Indexes ───────────────────────────────────────────────────────
-- Time-series index: dashboard queries are almost always "last N minutes"
CREATE INDEX IF NOT EXISTS idx_transactions_scored_at
    ON transactions (scored_at DESC);

-- Fraud filter index: "show me only fraud cases" is a common query
CREATE INDEX IF NOT EXISTS idx_transactions_is_fraud
    ON transactions (is_fraud)
    WHERE is_fraud = TRUE;   -- partial index — only indexes TRUE rows, smaller and faster

-- ── Seed check view (optional but useful for debugging) ───────────
-- Run: SELECT * FROM fraud_summary;
CREATE OR REPLACE VIEW fraud_summary AS
SELECT
    COUNT(*)                                          AS total_transactions,
    SUM(is_fraud::INT)                                AS total_fraud,
    ROUND(AVG(fraud_probability::NUMERIC) * 100, 2)  AS avg_fraud_prob_pct,
    MAX(scored_at)                                    AS last_scored_at
FROM transactions;
