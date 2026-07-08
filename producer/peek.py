"""
producer/peek.py

Throwaway debug consumer — prints arriving Kafka messages to stdout.

Run this in a SEPARATE terminal to visually confirm the producer pipe
works before building the real consumer. It reads from the LATEST
offset so it only shows messages that arrive after it starts.

Run:
    python producer/peek.py

Stop with Ctrl+C.
"""

import json
import logging
import os
import sys

from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaConnectionError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "transactions")


def main() -> None:
    """Connect to Kafka and print each arriving message until Ctrl+C."""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="latest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            group_id=None,
        )
    except NoBrokersAvailable:
        logger.error(
            "Cannot connect to Kafka at %s. "
            "Start the stack with: docker compose up -d",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        sys.exit(1)

    logger.info("👂 Listening on topic '%s' @ %s", KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
    logger.info("─" * 60)

    try:
        for msg in consumer:
            val: dict = msg.value
            meta: dict = val.get("meta", {})
            txn_id: str = val.get("transaction_id", "")[:8]
            amount: float = meta.get("paysim_amount", 0.0)
            txn_type: str = meta.get("paysim_type", "?")
            is_fraud: int = meta.get("ground_truth_fraud", 0)
            fraud_label: str = "🚨 FRAUD" if is_fraud else "✅ legit"

            logger.info(
                "offset=%-6d | id=%s | type=%-12s | amount=%10.2f | %s",
                msg.offset,
                txn_id,
                txn_type,
                amount,
                fraud_label,
            )
    except KeyboardInterrupt:
        logger.info("\nPeek stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
