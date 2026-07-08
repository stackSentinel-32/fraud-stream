# fraud-stream

A production-quality, end-to-end real-time fraud detection system built as a final-year Data Science / AI-ML portfolio project. The system simulates a continuous stream of financial transactions, scores each one for fraud probability using a trained LightGBM model served via a REST API, persists results to PostgreSQL, and visualises system health on a live Streamlit dashboard — all orchestrated with Apache Kafka and Docker Compose.

---

## Architecture

Transactions are generated row-by-row from the PaySim dataset by a Python producer script and published to a Kafka topic called `transactions`. A Kafka consumer service subscribes to that topic, sends each transaction's feature vector to the FastAPI scoring endpoint via HTTP, receives back a fraud probability and latency measurement, and writes the result to a PostgreSQL `predictions` table. The Streamlit dashboard polls that table every few seconds to display live throughput (TPS), fraud rate over time, prediction latency percentiles (p50/p95/p99), and a KS-test-based feature drift indicator comparing the transaction amount distribution across one-hour windows. Load testing with Locust independently hammers the FastAPI endpoint to characterise throughput limits and latency under concurrency.

```
[PaySim CSV]
     │
     ▼
producer.py ──► Kafka (transactions topic) ──► consumer.py ──► FastAPI /predict
                                                                      │
                                                               LightGBM model
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
| ML model | LightGBM 4.5 (binary classifier) |
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

### Step 3 — Install Python dependencies and train the model

```bash
pip install -r requirements.txt

# Trains LightGBM on creditcard.csv → saves model/model.pkl and model/feature_columns.json
python model/train.py
```

Expected output:
```
AUC-PR  (primary)  : 0.0785
AUC-ROC (secondary): 0.9348
Model saved → model/model.pkl
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

| Metric | Value |
|---|---|
| Dataset | Credit Card Fraud Detection (Kaggle, 284,807 rows) |
| Fraud rate | 0.17% |
| Algorithm | LightGBM (binary classifier) |
| Imbalance handling | `scale_pos_weight` = 577 |
| AUC-PR | 0.0785 |
| AUC-ROC | 0.9348 |
| Fraud recall | 89% |
| Training time | ~4 seconds |

---

## What I Would Improve With More Time

**1. Fix the PaySim schema mismatch**
The current producer zeroes out V1–V28 because the model was trained on PCA-anonymised creditcard.csv features with no PaySim equivalent. The correct fix is to train a second LightGBM model directly on PaySim's raw features (`amount`, `oldbalanceOrg`, `newbalanceOrig`, etc.), which would make the streaming scores meaningful rather than demo artifacts.

**2. Exactly-once delivery with Kafka transactions**
The consumer currently uses at-least-once semantics — if it crashes mid-write, a message may be processed twice. Kafka transactions plus idempotent Postgres `INSERT ... ON CONFLICT DO NOTHING` on `transaction_id` would give exactly-once guarantees without major complexity.

**3. Model versioning with MLflow**
`model.pkl` is a single file with no versioning. Adding MLflow tracking (log AUC-PR, feature schema, and hyperparameters per run; serve the registered model) would let you A/B two models on live traffic and roll back in seconds — a realistic production capability.

**4. Prometheus + Grafana instead of Streamlit for ops metrics**
Streamlit polls Postgres every 5 seconds, which is acceptable for a demo but not for production alerting. Instrumenting the FastAPI app with `prometheus-fastapi-instrumentator` and attaching a Grafana dashboard would give sub-second metrics, alert rules, and a more defensible monitoring story.

**5. Dead letter queue for malformed messages**
Malformed Kafka messages are currently logged and skipped. In production, skipping silently loses data. A dead letter topic (`transactions.dlq`) would capture every bad message for inspection and replay without blocking the main consumer.

**6. Async consumer to eliminate HTTP blocking**
The consumer's `predict_via_api` call is synchronous — it blocks the process while waiting for the API response. Rewriting the consumer as an async `asyncio` loop with `httpx.AsyncClient` would allow concurrent in-flight requests without threads, increasing throughput significantly on I/O-bound workloads.

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
│   ├── train.py            # training pipeline
│   ├── model.pkl           # generated artifact — gitignored
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
