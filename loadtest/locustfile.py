"""
loadtest/locustfile.py

Locust load test targeting the FastAPI /predict endpoint.

Run (UI mode):
    locust -f loadtest/locustfile.py --host http://localhost:8000
    Then open http://localhost:8089

Run (headless, auto-ramp):
    locust -f loadtest/locustfile.py --host http://localhost:8000 \
        --users 500 --spawn-rate 10 --run-time 5m \
        --headless --html locust_report.html
"""

import csv
import json
import os
import random
from pathlib import Path

from dotenv import load_dotenv
from locust import HttpUser, between, task

load_dotenv()

_FEATURE_COLUMNS_PATH = Path(os.getenv("FEATURE_COLUMNS_PATH", "model/feature_columns.json"))
_PAYSIM_PATH = Path(os.getenv("PAYSIM_PATH", "data/paysim.csv"))
_SAMPLE_SIZE = int(os.getenv("LOCUST_SAMPLE_SIZE", "100"))


def _build_payload_templates() -> list[dict]:
    feature_columns: list[str] = json.loads(_FEATURE_COLUMNS_PATH.read_text())
    templates: list[dict] = []

    if _PAYSIM_PATH.exists():
        with _PAYSIM_PATH.open("r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for i, row in enumerate(reader):
                if i >= _SAMPLE_SIZE:
                    break
                amount   = float(row.get("amount", 0.0))
                old_org  = float(row.get("oldbalanceOrg", 0.0))
                new_org  = float(row.get("newbalanceOrig", 0.0))
                old_dest = float(row.get("oldbalanceDest", 0.0))
                new_dest = float(row.get("newbalanceDest", 0.0))
                txn_type = row.get("type", "")
                raw = {
                    "amount": amount,
                    "oldbalanceOrg": old_org, "newbalanceOrig": new_org,
                    "balance_delta_org": new_org - old_org,
                    "oldbalanceDest": old_dest, "newbalanceDest": new_dest,
                    "balance_delta_dest": new_dest - old_dest,
                    "type_CASH_IN":  1.0 if txn_type == "CASH_IN"  else 0.0,
                    "type_CASH_OUT": 1.0 if txn_type == "CASH_OUT" else 0.0,
                    "type_DEBIT":    1.0 if txn_type == "DEBIT"    else 0.0,
                    "type_PAYMENT":  1.0 if txn_type == "PAYMENT"  else 0.0,
                    "type_TRANSFER": 1.0 if txn_type == "TRANSFER" else 0.0,
                }
                templates.append({col: raw.get(col, 0.0) for col in feature_columns})

    if not templates:
        templates = [{col: 0.0 for col in feature_columns}]

    return templates


_TEMPLATES: list[dict] = _build_payload_templates()


class FraudAPIUser(HttpUser):
    """Simulates a client sending transaction scoring requests to the API."""

    wait_time = between(0.05, 0.5)  # realistic inter-request delay per user

    @task(10)
    def predict(self) -> None:
        payload = random.choice(_TEMPLATES)
        with self.client.post(
            "/predict",
            json=payload,
            name="POST /predict",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"Unexpected status: {resp.status_code}")

    @task(1)
    def health(self) -> None:
        self.client.get("/health", name="GET /health")
