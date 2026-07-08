"""
model/train.py

Trains a LightGBM binary classifier on the Credit Card Fraud dataset
and persists the model artifact + feature schema for the API layer.

Run:
    python model/train.py
"""

import json
import logging
import os
import sys
from pathlib import Path

import joblib
import lightgbm as lgb
import pandas as pd
from dotenv import load_dotenv
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

load_dotenv()

logging.basicConfig(
    level=logging.getLevelName(os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Paths — read from .env so nothing is hardcoded ────────────────
DATA_PATH = Path(os.getenv("DATA_PATH", "data/creditcard.csv"))
MODEL_PATH = Path(os.getenv("MODEL_PATH", "model/model.pkl"))
FEATURE_COLUMNS_PATH = Path(os.getenv("FEATURE_COLUMNS_PATH", "model/feature_columns.json"))

LABEL_COLUMN = "Class"
TEST_SIZE = 0.2
RANDOM_STATE = 42


def load_data(path: Path) -> pd.DataFrame:
    """
    Load the raw creditcard CSV and do a basic schema sanity-check.

    Raises FileNotFoundError with a helpful download hint rather than a
    cryptic pandas error, so the README quick-start is self-sufficient.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"Dataset not found at '{path}'.\n"
            "Download from kaggle.com/datasets/mlg-ulb/creditcardfraud "
            "and place it at data/creditcard.csv"
        )
    df = pd.read_csv(path)
    if LABEL_COLUMN not in df.columns:
        raise ValueError(f"Expected label column '{LABEL_COLUMN}' not in dataset columns: {list(df.columns)}")
    fraud_rate = df[LABEL_COLUMN].mean() * 100
    logger.info("Loaded %d rows | fraud rate: %.4f%%", len(df), fraud_rate)
    return df


def split_features_labels(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """
    Return (X, y), dropping Time alongside Class.

    Time is wall-clock offset from the first transaction in the dataset —
    it encodes dataset-collection order, not real-world transaction timing.
    Keeping it lets the model learn position-in-file patterns that will
    not generalise to a live stream where timestamps are continuous.
    """
    drop_cols = [col for col in [LABEL_COLUMN, "Time"] if col in df.columns]
    return df.drop(columns=drop_cols), df[LABEL_COLUMN]


def compute_scale_pos_weight(y: pd.Series) -> float:
    """
    Return count(negatives) / count(positives) for LightGBM's scale_pos_weight.

    Why scale_pos_weight instead of SMOTE?
    SMOTE generates synthetic minority samples. On 285K rows it is slow,
    and — critically — it must run *inside* each CV fold to avoid synthetic
    samples from the training fold leaking into the validation fold (a subtle
    but common data-leakage bug). scale_pos_weight achieves the same effect
    by up-weighting minority misclassifications during tree building, with
    zero augmentation overhead and zero leakage risk.
    """
    n_neg = int((y == 0).sum())
    n_pos = int((y == 1).sum())
    weight = n_neg / n_pos
    logger.info("scale_pos_weight = %.2f  (%d neg / %d pos)", weight, n_neg, n_pos)
    return weight


def train_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    scale_pos_weight: float,
) -> lgb.LGBMClassifier:
    """
    Fit a LightGBM classifier with imbalance-aware loss weighting.

    Hyperparameters are deliberately at sensible defaults — this is a
    reproducible baseline, not a tuned system. n_estimators=500 with
    learning_rate=0.05 is a well-established starting point for tabular
    fraud detection before any grid/random search.
    """
    model = lgb.LGBMClassifier(
        n_estimators=500,
        learning_rate=0.05,
        num_leaves=31,             # default; controls tree complexity
        scale_pos_weight=scale_pos_weight,
        random_state=RANDOM_STATE,
        n_jobs=-1,                 # saturate all CPU cores during training
        verbose=-1,                # silence LightGBM's own stdout output
    )
    logger.info("Training LightGBM... (this takes ~30s on a laptop)")
    model.fit(X_train, y_train)
    logger.info("Training complete.")
    return model


def evaluate(
    model: lgb.LGBMClassifier,
    X_test: pd.DataFrame,
    y_test: pd.Series,
) -> None:
    """
    Report AUC-PR as primary metric, AUC-ROC as secondary.

    Why not accuracy?
    Predicting "not fraud" for every row gives ~99.83% accuracy on this
    dataset while catching zero fraud cases. AUC-PR evaluates the
    precision/recall trade-off *on the minority class only*, making it
    the correct metric for imbalanced binary classification. AUC-ROC is
    shown because interviewers often ask for it — note it can look
    optimistic on skewed data (0.97+ is common even for weak models).
    """
    y_proba = model.predict_proba(X_test)[:, 1]
    y_pred = model.predict(X_test)

    logger.info("── Evaluation ───────────────────────────────")
    logger.info("AUC-PR  (primary)  : %.4f", average_precision_score(y_test, y_proba))
    logger.info("AUC-ROC (secondary): %.4f", roc_auc_score(y_test, y_proba))
    report = classification_report(y_test, y_pred, target_names=["Legit", "Fraud"])
    logger.info("Classification report:\n%s", report)


def save_artifacts(
    model: lgb.LGBMClassifier,
    feature_columns: list[str],
) -> None:
    """
    Persist model and feature schema to disk.

    feature_columns.json is consumed by the API at startup to validate
    that incoming request payloads contain exactly the columns the model
    was trained on — catches schema drift early rather than inside predict().
    """
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)

    joblib.dump(model, MODEL_PATH)
    logger.info("Model saved → %s", MODEL_PATH)

    FEATURE_COLUMNS_PATH.write_text(json.dumps(feature_columns, indent=2))
    logger.info("Feature schema saved → %s", FEATURE_COLUMNS_PATH)

    # Reminder: both files are in .gitignore — don't force-add them.
    logger.info("NOTE: model.pkl and feature_columns.json are gitignored by design.")


def main() -> None:
    """End-to-end training pipeline: load → split → train → evaluate → save."""
    df = load_data(DATA_PATH)
    X, y = split_features_labels(df)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
        stratify=y,   # preserve the 0.17% fraud rate in both splits
    )
    logger.info("Split → train: %d rows | test: %d rows", len(X_train), len(X_test))

    spw = compute_scale_pos_weight(y_train)
    model = train_model(X_train, y_train, spw)
    evaluate(model, X_test, y_test)
    save_artifacts(model, list(X.columns))

    logger.info("Done. Next: python api/main.py")


if __name__ == "__main__":
    main()
