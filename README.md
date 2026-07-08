# fraud-stream — Real-Time Fraud Detection System

A production-quality, end-to-end streaming fraud detection pipeline built as a final-year Data Science / AI-ML portfolio project.

## Architecture

```
creditcard.csv ──► train.py ──► model.pkl
                                    │
paysim.csv ──► producer.py ──► Kafka ──► consumer.py ──► PostgreSQL
                                                │               │
                                           api/main.py    dashboard.py
                                                │
                                          locustfile.py (load test)
```

## Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka + Zookeeper (Docker) |
| ML model | LightGBM |
| Inference API | FastAPI + Uvicorn |
| Database | PostgreSQL 16 |
| Dashboard | Streamlit |
| Load testing | Locust |
| Orchestration | Docker Compose |

## Quick Start

```bash
# 1. Copy environment file and fill in values
cp .env.example .env

# 2. Start infrastructure
docker compose up -d

# 3. Verify services are healthy
docker compose ps

# 4. Install Python deps
pip install -r requirements.txt

# 5. Train the model (add creditcard.csv to data/ first)
python model/train.py

# 6. Start the API
uvicorn api.main:app --reload

# 7. Start the consumer
python consumer/consumer.py

# 8. Start the producer (simulates live transactions)
python producer/producer.py

# 9. Launch the dashboard
streamlit run dashboard/dashboard.py
```

## Dataset

- **Training**: [Credit Card Fraud Detection](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud) — place as `data/creditcard.csv`
- **Simulation**: [PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1) — place as `data/paysim.csv`
