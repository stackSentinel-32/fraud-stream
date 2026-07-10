"""
model/train.py

Trains two complementary fraud detectors on the PaySim dataset:
  1. LightGBM       — supervised, high-precision, uses fraud labels
  2. Isolation Forest — unsupervised, catches anomalies without labels

Run:
    python model/train.py
"""

import json
import logging
from pathlib import Path

import joblib
import lightgbm as lgb
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import average_precision_score, classification_report, roc_auc_score
from sklearn.model_selection import train_test_split

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

DATA_PATH     = Path("data/paysim.csv")
MODEL_PATH    = Path("model/model.pkl")
IF_PATH       = Path("model/isolation_forest.pkl")
FEATURES_PATH = Path("model/feature_columns.json")

FEATURE_COLS = [
    "amount",
    "oldbalanceOrg", "newbalanceOrig", "balance_delta_org",
    "oldbalanceDest", "newbalanceDest", "balance_delta_dest",
    "type_CASH_IN", "type_CASH_OUT", "type_DEBIT", "type_PAYMENT", "type_TRANSFER",
]


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["balance_delta_org"]  = df["newbalanceOrig"] - df["oldbalanceOrg"]
    df["balance_delta_dest"] = df["newbalanceDest"] - df["oldbalanceDest"]
    type_dummies = pd.get_dummies(df["type"], prefix="type")
    for col in [c for c in FEATURE_COLS if c.startswith("type_")]:
        if col not in type_dummies.columns:
            type_dummies[col] = 0
    return pd.concat([df, type_dummies], axis=1)


def train_lgbm(
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train: pd.Series,
    y_test: pd.Series,
) -> lgb.LGBMClassifier:
    scale_pos_weight = (y_train == 0).sum() / (y_train == 1).sum()
    model = lgb.LGBMClassifier(
        n_estimators=500,
        learning_rate=0.05,
        num_leaves=63,
        scale_pos_weight=scale_pos_weight,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )
    model.fit(X_train, y_train)

    probs = model.predict_proba(X_test)[:, 1]
    log.info("LightGBM  AUC-PR : %.4f", average_precision_score(y_test, probs))
    log.info("LightGBM  AUC-ROC: %.4f", roc_auc_score(y_test, probs))
    log.info("\n%s", classification_report(y_test, (probs >= 0.5).astype(int), target_names=["legit", "fraud"]))
    return model


def train_isolation_forest(X_train: pd.DataFrame) -> IsolationForest:
    # contamination=0.013 matches PaySim's 1.3% fraud rate
    iforest = IsolationForest(n_estimators=100, contamination=0.013, random_state=42, n_jobs=-1)
    iforest.fit(X_train)
    log.info("Isolation Forest fitted (contamination=0.013)")
    return iforest


def main() -> None:
    log.info("Loading %s", DATA_PATH)
    df = engineer_features(pd.read_csv(DATA_PATH))

    X = df[FEATURE_COLS].astype(float)
    y = df["isFraud"].astype(int)
    log.info("Rows: %d | Fraud: %d (%.3f%%)", len(df), y.sum(), y.mean() * 100)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )

    log.info("Training LightGBM...")
    lgbm_model = train_lgbm(X_train, X_test, y_train, y_test)

    log.info("Training Isolation Forest...")
    iforest_model = train_isolation_forest(X_train)

    joblib.dump(lgbm_model, MODEL_PATH)
    log.info("Saved → %s", MODEL_PATH)

    joblib.dump(iforest_model, IF_PATH)
    log.info("Saved → %s", IF_PATH)

    FEATURES_PATH.write_text(json.dumps(FEATURE_COLS))
    log.info("Feature columns saved → %s", FEATURES_PATH)


if __name__ == "__main__":
    main()
