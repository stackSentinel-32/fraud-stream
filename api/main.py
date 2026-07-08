"""
api/main.py

FastAPI model serving layer — loads model once, scores per request.

Run:
    uvicorn api.main:app --reload --port 8000
"""

import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

import joblib
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, create_model

load_dotenv()

logging.basicConfig(
    level=logging.getLevelName(os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

MODEL_PATH = Path(os.getenv("MODEL_PATH", "model/model.pkl"))
FEATURE_COLUMNS_PATH = Path(os.getenv("FEATURE_COLUMNS_PATH", "model/feature_columns.json"))

# Build the Pydantic input model dynamically from feature_columns.json.
# This means any mismatch between request payload and trained schema
# is caught automatically by FastAPI and returns a 422 before reaching
# our prediction code — no manual validation needed.
_feature_columns: list[str] = json.loads(FEATURE_COLUMNS_PATH.read_text())
TransactionInput = create_model(
    "TransactionInput",
    **{col: (float, ...) for col in _feature_columns},
)


class PredictionResponse(BaseModel):
    fraud_probability: float
    is_fraud: bool
    latency_ms: float


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load heavy artifacts once at startup, not per request."""
    app.state.model = joblib.load(MODEL_PATH)
    app.state.feature_columns = _feature_columns
    logger.info("Model loaded | features: %d", len(_feature_columns))
    yield
    logger.info("API shutting down.")


app = FastAPI(title="fraud-stream", version="1.0.0", lifespan=lifespan)


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Return 422 with a plain-English summary instead of raw Pydantic internals."""
    missing = [e["loc"][-1] for e in exc.errors() if e["type"] == "missing"]
    detail = (
        f"Missing required feature fields: {missing}. "
        f"Expected {len(_feature_columns)} fields matching model/feature_columns.json."
        if missing
        else str(exc.errors())
    )
    logger.warning("422 validation error: %s", detail)
    return JSONResponse(status_code=422, content={"detail": detail})


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/predict", response_model=PredictionResponse)
async def predict(txn: TransactionInput) -> PredictionResponse:
    """Score a transaction feature vector and return fraud probability."""
    t0 = time.monotonic()

    row = [[getattr(txn, col) for col in app.state.feature_columns]]
    prob = float(app.state.model.predict_proba(row)[0][1])
    is_fraud = prob >= 0.5
    latency_ms = (time.monotonic() - t0) * 1000

    logger.info("prob=%.4f | fraud=%-5s | %.2fms", prob, is_fraud, latency_ms)
    return PredictionResponse(
        fraud_probability=prob,
        is_fraud=is_fraud,
        latency_ms=latency_ms,
    )
