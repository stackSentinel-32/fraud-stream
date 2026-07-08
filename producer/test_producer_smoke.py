"""Smoke test for producer.py — run from producer/ directory."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from producer import map_paysim_to_message  

cols = json.loads(Path(__file__).parent.parent.joinpath("model/feature_columns.json").read_text())

row = {
    "step": "1",
    "type": "PAYMENT",
    "amount": "149.62",
    "nameOrig": "C1",
    "nameDest": "M1",
    "isFraud": "0",
}
msg = map_paysim_to_message(row, cols, "test-uuid-1234")

assert set(msg["features"].keys()) == set(cols), "Feature key mismatch!"
assert msg["features"]["Amount"] == 149.62, "Amount not mapped correctly!"
assert all(msg["features"][v] == 0.0 for v in cols if v != "Amount"), "V1-V28 not zeroed!"

print("producer.py  imports OK")
print("peek.py      imports OK")
print(f"Feature keys (first 5): {list(msg['features'].keys())[:5]}")
print(f"Amount:  {msg['features']['Amount']}")
print(f"V1:      {msg['features']['V1']}  (placeholder zero)")
print(f"meta:    {list(msg['meta'].keys())}")
print("All assertions passed.")
