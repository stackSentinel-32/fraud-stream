# fraud-stream

A production-quality, end-to-end real-time fraud detection system built as a portfolio project. Transactions stream from the PaySim dataset through Apache Kafka, scored in real time by a LightGBM classifier and an Isolation Forest anomaly detector served via FastAPI, with results persisted to PostgreSQL and visualised on a live Streamlit dashboard.

---

## Architecture

Transactions are generated row-by-row from the PaySim dataset by a Python producer script and published to a Kafka topic called `transactions`. A Kafka consumer subscribes, sends each transaction's engineered feature vector to the FastAPI scoring endpoint, receives back a LightGBM fraud probability and an Isolation Forest anomaly flag, and writes both to a PostgreSQL `predictions` table. The Streamlit dashboard polls that table every few seconds to show live TPS, fraud rate (both models), latency percentiles (p50/p95/p99), and a KS-test-based amount-distribution drift indicator.

```
[PaySim CSV]
     │
     ▼
producer.py ──► Kafka (transactions topic) ──► consumer.py ──► FastAPI /predict
                                                                      │
                                                              LightGBM  +  Isolation Forest
                                                                      │
                                                             PostgreSQL predictions
                                                                      │
                                                           Streamlit dashboard
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka + Zookeeper (Confluent CP 7.6) |
| ML model | LightGBM 4.5 (supervised) + Isolation Forest (unsupervised) |
| Inference API | FastAPI 0.115 + Uvicorn (4 workers) |
| Database | PostgreSQL 16 |
| Dashboard | Streamlit 1.41 |
| Load testing | Locust 2.32 |
| Orchestration | Docker Compose |

---

## Setup — Fresh Clone to Working Demo

### Prerequisites
- Docker Desktop (running)
- Python 3.12+
- Kaggle account (to download datasets)

### Step 1 — Clone and configure

```bash
git clone https://github.com/stackSentinel-32/fraud-stream.git
cd fraud-stream
cp .env.example .env
# Open .env and set POSTGRES_PASSWORD to any value you like
```

### Step 2 — Download datasets

```bash
# Install Kaggle CLI
pip install kaggle

# Download Credit Card Fraud dataset (training data)
kaggle datasets download -d mlg-ulb/creditcardfraud -p data/ --unzip

# Download PaySim dataset (streaming simulation)
kaggle datasets download -d ealaxi/paysim1 -p data/ --unzip
mv data/PS_20174392719_1491204439457_log.csv data/paysim.csv
```

### Step 3 — Install Python dependencies and train the models

```bash
pip install -r requirements.txt

# Trains LightGBM + Isolation Forest on PaySim features
# Saves: model/model.pkl, model/isolation_forest.pkl, model/feature_columns.json
python model/train.py
```

Expected output:
```
LightGBM  AUC-PR : ~0.72
LightGBM  AUC-ROC: ~0.99
Isolation Forest fitted (contamination=0.013)
Saved → model/model.pkl
Saved → model/isolation_forest.pkl
```

### Step 4 — Start the full stack

```bash
docker compose up --build
```

Wait for all services to report healthy (~60–90 seconds on first build):

```bash
docker compose ps
# All six services should show "healthy" or "running"
```

### Step 5 — Start the producer (on demand)

```bash
# Open a new terminal — this simulates live transaction traffic
python producer/producer.py
```

### Step 6 — Open the interfaces

| Interface | URL |
|---|---|
| Streamlit dashboard | http://localhost:8501 |
| FastAPI docs (Swagger) | http://localhost:8000/docs |
| FastAPI health check | http://localhost:8000/health |

### Step 7 — Run the load test (optional)

```bash
# Start Locust UI
locust -f loadtest/locustfile.py --host http://localhost:8000

# Open http://localhost:8089 → set Users=500, Spawn rate=10 → Start
```

---

## Load Test Results

> Run with Locust ramping from 10 → 500 concurrent users over 5 minutes, single host, 4 uvicorn workers.

| Metric | Value |
|---|---|
| Peak requests/sec | _TBD_ |
| p50 latency | _TBD_ ms |
| p95 latency | _TBD_ ms |
| p99 latency | _TBD_ ms |
| Failure rate | _TBD_ % |
| Breaking point (users) | _TBD_ concurrent users |
| Primary bottleneck | _TBD_ |

---

## Model Performance

### LightGBM (supervised)

| Metric | Value |
|---|---|
| Dataset | PaySim (6.3M transactions, Kaggle) |
| Fraud rate | ~1.3% |
| Features | 12 (amount, balance deltas, transaction type one-hot) |
| Imbalance handling | `scale_pos_weight` ≈ 76 |
| AUC-PR | ~0.72 |
| AUC-ROC | ~0.99 |
| Fraud recall | ~90% at threshold 0.5 |
| Training time | ~60 seconds (6.3M rows, 500 trees) |

### Isolation Forest (unsupervised — no labels used)

| Metric | Value |
|---|---|
| Algorithm | Isolation Forest (sklearn) |
| Contamination | 0.013 (matches PaySim fraud rate) |
| n_estimators | 100 |
| Use case | Catches distributional anomalies the supervised model misses |
| Compared with LightGBM | Higher false-positive rate but zero dependency on fraud labels |

---

## What I Would Improve With More Time

**1. Exactly-once delivery with Kafka transactions**
The consumer uses at-least-once semantics — if it crashes mid-write, a message may be processed twice. Kafka transactions plus idempotent Postgres `INSERT ... ON CONFLICT DO NOTHING` on `transaction_id` would give exactly-once guarantees.

**2. Model versioning with MLflow**
`model.pkl` is a single file with no versioning. MLflow tracking (log AUC-PR, feature schema, hyperparameters per run) would enable A/B testing two models on live traffic and rolling back in seconds.

**3. Prometheus + Grafana instead of Streamlit for ops metrics**
Streamlit polls Postgres every 5 seconds — fine for a demo but not for production alerting. `prometheus-fastapi-instrumentator` + Grafana would give sub-second metrics and alert rules.

**4. Dead letter queue for malformed messages**
Malformed Kafka messages are currently logged and skipped. A dead letter topic (`transactions.dlq`) would capture every bad message for inspection and replay without blocking the main consumer.

**5. Async consumer to eliminate HTTP blocking**
The consumer's `predict_via_api` call is synchronous. Rewriting with `asyncio` + `httpx.AsyncClient` would allow concurrent in-flight requests, significantly increasing throughput on I/O-bound workloads.

**6. Online model retraining**
As transaction patterns drift (detected by the KS test), the model should be retrained automatically on a rolling window. An Airflow DAG triggering `train.py` weekly would keep the model current without manual intervention.

---

## Project Structure

```
fraud-stream/
├── docker-compose.yml      # full stack orchestration
├── Dockerfile              # shared base image for api, consumer, dashboard
├── requirements.txt        # pinned Python dependencies
├── .env.example            # configuration template (copy to .env)
├── data/                   # datasets — gitignored
├── model/
│   ├── train.py            # training pipeline (LightGBM + Isolation Forest)
│   ├── model.pkl           # LightGBM artifact — gitignored
│   ├── isolation_forest.pkl # IF artifact — gitignored
│   └── feature_columns.json
├── producer/
│   ├── producer.py         # Kafka producer (run manually)
│   └── peek.py             # debug consumer for pipe verification
├── consumer/
│   └── consumer.py         # Kafka consumer → API → Postgres
├── api/
│   └── main.py             # FastAPI scoring endpoint
├── dashboard/
│   └── dashboard.py        # Streamlit live dashboard
├── loadtest/
│   └── locustfile.py       # Locust load test
└── db/
    └── init.sql            # Postgres schema (auto-runs on first start)
```
