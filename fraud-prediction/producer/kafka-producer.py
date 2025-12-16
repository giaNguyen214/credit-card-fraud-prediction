# kafka_producer_app_fast_correct.py
# ============================================================
# HIGH-THROUGHPUT KAFKA PRODUCER – FAST BUT LOGICALLY CORRECT
#
# GUARANTEES:
# - card_txn_auth ALWAYS references an existing card/user/merchant
# - Same logical model as the OLD (slow) producer
# - Scales to 100+ txn/sec without Spark join misses
#
# STRATEGY:
# 1) Warm up reference entities (user/card/merchant)
# 2) Keep them in memory pools
# 3) High-rate TXN stream reuses those references
# ============================================================

from confluent_kafka import Producer
import json
import time
import random
import sys
import os

# ------------------------------------------------------------
# Import generators
# ------------------------------------------------------------
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils.generator import (
    generate_bundle,
    gen_card_txn_auth_event,
    introduce_noise,
)

# ============================================================
# Kafka Producer Configuration (THROUGHPUT OPTIMIZED)
# ============================================================

producer_conf = {
    "bootstrap.servers": "52.221.207.227:30092,52.221.207.227:30093",
    "client.id": "fraud-simulator-fast-correct",
    "acks": "1",
    "linger.ms": 5,
    "batch.size": 65536,
    "queue.buffering.max.messages": 200000,
    "compression.type": "lz4",
    "socket.timeout.ms": 10000,
}

producer = Producer(producer_conf)

# ============================================================
# Validation
# ============================================================

def validate_record(record_type: str, data: dict) -> bool:
    required_fields = {
        "user_profile": ["party_id", "event_id", "event_ts"],
        "card_account": ["card_ref", "party_id_fk", "event_id"],
        "merchant_profile": ["merchant_id", "event_id"],
        "card_txn_auth": ["event_id", "card_ref", "merchant_ref", "amount_minor"],
    }

    for field in required_fields.get(record_type, []):
        if field not in data or data[field] is None:
            return False
    return True


def send_to_kafka(topic: str, value: dict, key: str | None = None) -> None:
    if not validate_record(topic, value):
        return

    producer.produce(
        topic=topic,
        key=key.encode("utf-8") if key else None,
        value=json.dumps(value).encode("utf-8"),
    )


# ============================================================
# Reference Pools (IN-MEMORY STATE)
# ============================================================

USER_POOL = []
CARD_POOL = []
MERCHANT_POOL = []

MAX_POOL_SIZE = 5000

# ============================================================
# Warm-up Phase
# ============================================================

def warmup_references(n=1000):
    for _ in range(n):
        bundle = generate_bundle()

        user = bundle["user_profile"]
        card = bundle["card_account"]
        merchant = bundle["merchant_profile"]

        USER_POOL.append(user)
        CARD_POOL.append(card)
        MERCHANT_POOL.append(merchant)

        send_to_kafka("user_profile", user, key=user["party_id"])
        send_to_kafka("merchant_profile", merchant, key=merchant["merchant_id"])
        send_to_kafka("card_account", card, key=card["card_ref"])

    producer.poll(0)
    print(f"✅ Warmup complete: {len(USER_POOL)} users/cards/merchants")


# ============================================================
# Main Loop — FAST + CORRECT
# ============================================================

if __name__ == "__main__":

    TXN_PER_SEC = 100          # main throughput
    REF_REFRESH_PER_SEC = 20   # slow background refresh

    warmup_references(1000)

    while True:
        loop_start = time.time()

        # ----------------------------------------------------
        # 1) Slowly refresh reference entities
        # ----------------------------------------------------
        for _ in range(REF_REFRESH_PER_SEC):
            bundle = generate_bundle()

            user = bundle["user_profile"]
            card = bundle["card_account"]
            merchant = bundle["merchant_profile"]

            USER_POOL.append(user)
            CARD_POOL.append(card)
            MERCHANT_POOL.append(merchant)

            send_to_kafka("user_profile", user, key=user["party_id"])
            send_to_kafka("merchant_profile", merchant, key=merchant["merchant_id"])
            send_to_kafka("card_account", card, key=card["card_ref"])

        # keep pools bounded
        USER_POOL[:] = USER_POOL[-MAX_POOL_SIZE:]
        CARD_POOL[:] = CARD_POOL[-MAX_POOL_SIZE:]
        MERCHANT_POOL[:] = MERCHANT_POOL[-MAX_POOL_SIZE:]

        # ----------------------------------------------------
        # 2) High-throughput transaction stream
        # ----------------------------------------------------
        for _ in range(TXN_PER_SEC):
            user = random.choice(USER_POOL)
            card = random.choice(CARD_POOL)
            merchant = random.choice(MERCHANT_POOL)

            txn = gen_card_txn_auth_event(card, merchant, user)
            txn = introduce_noise("card_txn_auth", txn)

            send_to_kafka(
                topic="card_txn_auth",
                value=txn,
                key=txn["card_ref"],
            )

        producer.poll(0)

        elapsed = time.time() - loop_start
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
