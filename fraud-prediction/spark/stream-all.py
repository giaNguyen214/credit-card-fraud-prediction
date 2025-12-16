import redis
import pandas as pd
import random
import time
import socket
import os
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import ( # type: ignore
    col,
    from_json,
    trim,
    upper,
    lower,
    when,
    lit,
    coalesce,
    regexp_replace,
    abs as ps_abs,
    greatest,
    least,
    from_unixtime
)

from pyspark.sql.types import ( # type: ignore
    StructType, StructField, StringType, IntegerType, DoubleType
)
import pickle
from io import BytesIO
import joblib
import numpy as np

# ============================================================
# CONFIG
# ============================================================
KAFKA_SERVERS = "kafka-0.kafka-service.kafka.svc.cluster.local:9092"
MINIO_ENDPOINT = "http://minio-service.minio.svc.cluster.local:9000"
REDIS_HOST = "redis.infra.svc.cluster.local"
TS_RETENTION_MS = "600000"

def get_host_ip():
    try:
        return os.environ.get("POD_IP", socket.gethostbyname(socket.gethostname()))
    except:
        return "0.0.0.0"

driver_ip = get_host_ip()
print(f"🔥 Driver IP for Executors to connect: {driver_ip}")

MODEL_BASE = "s3a://model/artifacts"

_cat_model = None
_lgb_model = None
_xgb_model = None
_processor = None
_feature_names = None


def load_pickle_from_s3(path: str):
    obj = spark.sparkContext.binaryFiles(path, 1).collect()[0][1]

    # Spark có thể trả bytes hoặc PortableDataStream
    if hasattr(obj, "open"):
        data = obj.open().read()
    else:
        data = obj

    return joblib.load(BytesIO(data))

def load_models():
    global _cat_model, _lgb_model, _xgb_model, _processor, _feature_names

    if _cat_model is not None:
        return  # đã load rồi

    print("🔥 Loading ML models from MinIO...")

    _cat_model = load_pickle_from_s3(f"{MODEL_BASE}/cat_model.pkl")
    _lgb_model = load_pickle_from_s3(f"{MODEL_BASE}/lgb_model.pkl")
    _xgb_model = load_pickle_from_s3(f"{MODEL_BASE}/xgb_model.pkl")
    _processor = load_pickle_from_s3(f"{MODEL_BASE}/processor.pkl")
    _feature_names = load_pickle_from_s3(f"{MODEL_BASE}/feature_names.pkl")

    print("✅ Models loaded successfully")



# ============================================================
# 🔥 SPARK SESSION (dùng lại config cũ, NOT TOUCH)
# ============================================================
spark = (
    SparkSession.builder
        .appName("All_Streams")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")

        .config("spark.driver.host", driver_ip)
        .config("spark.driver.bindAddress", "0.0.0.0")

        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
load_models()

# ============================================================
# METRICS UTILITIES (Grafana)
# ============================================================
_ts_ready = False
LATENCY_SERIES = {
    "user": "ts:latency:user_ms",
    "merchant": "ts:latency:merchant_ms",
    "card": "ts:latency:card_ms",
    "txn": "ts:latency:txn_ms",
}

# Redis key
# Live Stream Logs – Table các giao dịch Fraud mới nhất
FRAUD_STREAM_KEY = "stream:fraud:events"
# Model Confidence Distribution – Histogram probability score - Redis keys (10 buckets)
SCORE_BUCKET_PREFIX = "ts:score:bucket_"  # 0_1 ... 9_10

# Fraud by Category (Pie chart)
FRAUD_BY_CATEGORY_HASH_PREFIX = "fraud:by_category:"
FRAUD_BY_CHANNEL_HASH_PREFIX = "fraud:by_channel:"

def ensure_timeseries(r: redis.Redis):
    """Create Redis time series keys used by Grafana if they do not exist."""
    series = [
        "ts:txn:count",
        "ts:txn:fraud",
        "ts:txn:pred",
        "ts:txn:amount_minor",
        *LATENCY_SERIES.values()
    ]
    for key in series:
        try:
            r.execute_command("TS.CREATE", key, "RETENTION", TS_RETENTION_MS)
        except Exception:
            # Ignore if the series already exists
            pass
    for i in range(10):
        key = f"{SCORE_BUCKET_PREFIX}{i}_{i+1}"
        try:
            r.execute_command("TS.CREATE", key, "RETENTION", TS_RETENTION_MS)
        except:
            pass


# ============================================================
# FRAUD PREDICTION
# ============================================================

# def predict_fraud(txn_row, user_info, card_info, merchant_info):
#     simulated_fraud = 1 if random.random() < 0.8 else 0
#     # return 1
#     return simulated_fraud


def feature_engineering(df):
    df = df.copy()

    if 'trans_date_trans_time' in df.columns:
        df['trans_date_trans_time'] = pd.to_datetime(
            df['trans_date_trans_time'], errors='coerce'
        )
        df['hour'] = df['trans_date_trans_time'].dt.hour
        df['day_of_week'] = df['trans_date_trans_time'].dt.dayofweek
        df['month'] = df['trans_date_trans_time'].dt.month

        if 'dob' in df.columns:
            df['dob'] = pd.to_datetime(df['dob'], errors='coerce')
            df['age'] = (df['trans_date_trans_time'] - df['dob']).dt.days // 365

    drop_cols = [
        'trans_num', 'unix_time', 'first', 'last', 'street', 'city',
        'state', 'zip', 'dob', 'trans_date_trans_time',
        'cc_num', 'Unnamed: 0'
    ]

    cols_to_drop = [c for c in drop_cols if c in df.columns]
    return df.drop(columns=cols_to_drop)


def predict_fraud(txn_row, user_info, card_info, merchant_info):

    # 1. Merge raw
    features = {}
    features.update(user_info)
    features.update(card_info)
    features.update(merchant_info)
    features.update(txn_row.to_dict())

    X = pd.DataFrame([features])

    # 2. Datetime TRƯỚC (để age không bị NaN)
    if "trans_date_trans_time" in X.columns:
        X["trans_date_trans_time"] = pd.to_datetime(
            X["trans_date_trans_time"], errors="coerce"
        )

    if "dob" in X.columns:
        X["dob"] = pd.to_datetime(X["dob"], errors="coerce")

    # 3. amt = amount_major (theo feature_names.pkl)
    X["amt"] = X.get("amount_major", 0.0)

    # 4. Numeric SAU
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce")

    # 5. Feature engineering
    X = feature_engineering(X)

    # 6. Chốt schema train
    X = X.reindex(columns=_feature_names)

    # 7. Transform + ensemble
    Xp = _processor.transform(X)

    p = (
        _cat_model.predict_proba(Xp)[:, 1]
        + _lgb_model.predict_proba(Xp)[:, 1]
        + _xgb_model.predict_proba(Xp)[:, 1]
    ) / 3.0

    return int(p[0] >= 0.5)





# ============================================================
# USER STREAM
# ============================================================

CITY_MASTER = {
    "VN-HCM-01": {"city": "Ho Chi Minh City", "lat": 10.7769, "long": 106.7009, "pop": 9000000},
    "VN-HN-01":  {"city": "Ha Noi", "lat": 21.0278, "long": 105.8342, "pop": 8000000},
    "VN-DN-01":  {"city": "Da Nang", "lat": 16.0544, "long": 108.2022, "pop": 1200000},
}

user_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("party_id", StringType()),
    StructField("given_name", StringType()),
    StructField("family_name", StringType()),
    StructField("full_name", StringType()),
    StructField("gender_code", StringType()),
    StructField("birth_date", StringType()),
    StructField("birth_year", IntegerType()),
    StructField("occupation_title", StringType()),
    StructField("address_line", StringType()),
    StructField("region_code", StringType()),
    StructField("home_postal_hint", StringType()),
    StructField("location_id_home", StringType()),
    StructField("age", IntegerType()),
    StructField("age_group", StringType()),
    StructField("income_level", StringType()),
    StructField("home_latitude", DoubleType()),
    StructField("home_longitude", DoubleType()),
])

user_df_raw = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "user_profile")
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), user_schema).alias("data"))
        .select("data.*")
)

user_df = (
    user_df_raw
        .withColumn("region_code", upper(trim(coalesce(col("region_code"), lit("UNK")))))
        .withColumn("address_line", trim(coalesce(col("address_line"), lit("") )))
        .withColumn("income_level", upper(trim(coalesce(col("income_level"), lit("MEDIUM")))))
        .withColumn("age", when(col("age") < 0, 0).when(col("age") > 110, 110).otherwise(coalesce(col("age"), lit(0))))
        .withColumn("age_group", upper(trim(coalesce(col("age_group"), lit("UNKNOWN")))))
        .withColumn("home_latitude", coalesce(col("home_latitude"), lit(0.0)))
        .withColumn("home_longitude", coalesce(col("home_longitude"), lit(0.0)))
)

def write_user(batch_df, batch_id):
    start = time.time()
    pdf = batch_df.toPandas()
    r = redis.Redis(host=REDIS_HOST, decode_responses=True)
    pipe = r.pipeline()

    for _, row in pdf.iterrows():
        pid = row["party_id"]
        if not pid:
            continue

        city = CITY_MASTER.get(row["location_id_home"], {})

        pipe.hset(f"u:{pid}", mapping={
            "first": row["given_name"],
            "last": row["family_name"],
            "gender": row["gender_code"],
            "street": row["address_line"],
            "city": city.get("city", "Unknown"),
            "state": row["region_code"],
            "zip": row["home_postal_hint"],
            "lat": city.get("lat", 0.0),
            "long": city.get("long", 0.0),
            "city_pop": city.get("pop", 0),
            "job": row["occupation_title"],
            "dob": row["birth_date"],
            # Engineered features for ML
            "age": row.get("age", 0),
            "age_group": row.get("age_group", "UNKNOWN"),
            "income_level": row.get("income_level", "MEDIUM"),
            "home_latitude": row.get("home_latitude", city.get("lat", 0.0)),
            "home_longitude": row.get("home_longitude", city.get("long", 0.0)),
        })

    pipe.execute()

    latency_ms = int((time.time() - start) * 1000)
    r.execute_command("TS.ADD", LATENCY_SERIES["user"], int(time.time()*1000), latency_ms)
    # print(f"[USER] Batch {batch_id} → {len(pdf)} rows | {latency_ms} ms")

user_q = (
    user_df.writeStream
        .foreachBatch(write_user)
        .outputMode("append")
        .option("checkpointLocation", "s3a://fraud/checkpoints/user_redis/")
        .start()
)

# ============================================================
# MERCHANT STREAM
# ============================================================

merch_schema = StructType([
    StructField("event_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_display_name", StringType()),
    StructField("mcc_group", StringType()),
    StructField("geo_lat", DoubleType()),
    StructField("geo_lon", DoubleType()),
    StructField("risk_score_merchant", DoubleType()),
    StructField("avg_txn_amount_minor", IntegerType()),
    StructField("merchant_type", StringType()),
    StructField("is_high_risk", IntegerType()),
])

merch_df_raw = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "merchant_profile")
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), merch_schema).alias("data"))
        .select("data.*")
)

merch_df = (
    merch_df_raw
        .withColumn("mcc_group", upper(trim(coalesce(col("mcc_group"), lit("UNK")))))
        .withColumn("merchant_display_name", trim(coalesce(col("merchant_display_name"), lit("Merchant"))))
        .withColumn("geo_lat", least(greatest(coalesce(col("geo_lat"), lit(0.0)), lit(-90.0)), lit(90.0)))
        .withColumn("geo_lon", least(greatest(coalesce(col("geo_lon"), lit(0.0)), lit(-180.0)), lit(180.0)))
        .withColumn("risk_score_merchant", least(greatest(coalesce(col("risk_score_merchant"), lit(0.0)), lit(0.0)), lit(1.0)))
        .withColumn("merchant_type", upper(trim(coalesce(col("merchant_type"), lit("UNKNOWN")))))
        .withColumn("is_high_risk", when(col("is_high_risk") == 1, lit(1)).otherwise(lit(0)))
        .withColumn("avg_txn_amount_minor", coalesce(col("avg_txn_amount_minor"), lit(0)))
)

def write_merchant(batch_df, batch_id):
    start = time.time()

    pdf = batch_df.toPandas()
    r = redis.Redis(host=REDIS_HOST, decode_responses=True)
    pipe = r.pipeline()

    for _, row in pdf.iterrows():
        mid = row["merchant_id"]
        if not mid:
            continue

        pipe.hset(f"m:{mid}", mapping={
            "merchant": row["merchant_display_name"],
            "category": row["mcc_group"],
            "merch_lat": row["geo_lat"],
            "merch_long": row["geo_lon"],
            # Engineered features for ML
            "risk_score_merchant": row.get("risk_score_merchant", 0.0),
            "avg_txn_amount_minor": row.get("avg_txn_amount_minor", 0),
            "merchant_type": row.get("merchant_type", "UNKNOWN"),
            "is_high_risk": row.get("is_high_risk", 0),
        })

    pipe.execute()

    latency_ms = int((time.time() - start) * 1000)
    r.execute_command("TS.ADD", LATENCY_SERIES["merchant"], int(time.time()*1000), latency_ms)

    # print(f"[MERCHANT] Batch {batch_id} → {len(pdf)} rows | {latency_ms} ms")

merch_q = (
    merch_df.writeStream
        .foreachBatch(write_merchant)
        .outputMode("append")
        .option("checkpointLocation", "s3a://fraud/checkpoints/merchant_redis/")
        .start()
)

# ============================================================
# CARD STREAM
# ============================================================

card_schema = StructType([
    StructField("event_id", StringType()),
    StructField("party_id_fk", StringType()),
    StructField("card_ref", StringType()),
    StructField("card_pan_last4", StringType()),
    StructField("product_type", StringType()),
    StructField("brand", StringType()),
    StructField("days_since_issuance", IntegerType()),
    StructField("card_age_category", StringType()),
    StructField("is_primary_card", IntegerType()),
    StructField("daily_limit_minor", IntegerType()),
])

card_df_raw = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "card_account")
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), card_schema).alias("data"))
        .select("data.*")
)

card_df = (
    card_df_raw
        .withColumn("product_type", upper(trim(coalesce(col("product_type"), lit("UNKNOWN")))))
        .withColumn("brand", upper(trim(coalesce(col("brand"), lit("UNKNOWN")))))
        .withColumn("days_since_issuance", when(col("days_since_issuance") < 0, 0).otherwise(coalesce(col("days_since_issuance"), lit(0))))
        .withColumn("card_age_category", upper(trim(coalesce(col("card_age_category"), lit("UNKNOWN")))))
        .withColumn("is_primary_card", when(col("is_primary_card") == 1, lit(1)).otherwise(lit(0)))
        .withColumn("daily_limit_minor", when(col("daily_limit_minor") < 0, 0).otherwise(least(coalesce(col("daily_limit_minor"), lit(0)), lit(100000000))))
)

def write_card(batch_df, batch_id):
    start = time.time()

    pdf = batch_df.toPandas()
    r = redis.Redis(host=REDIS_HOST, decode_responses=True)
    pipe = r.pipeline()

    for _, row in pdf.iterrows():
        cid = row["card_ref"]
        if not cid:
            continue

        pipe.hset(f"c:{cid}", mapping={
            "party_id": row["party_id_fk"],
            "cc_last4": row["card_pan_last4"],
            # Engineered features for ML
            "product_type": row.get("product_type", "UNKNOWN"),
            "brand": row.get("brand", "UNKNOWN"),
            "days_since_issuance": row.get("days_since_issuance", 0),
            "card_age_category": row.get("card_age_category", "UNKNOWN"),
            "is_primary_card": row.get("is_primary_card", 0),
            "daily_limit_minor": row.get("daily_limit_minor", 5000000),
        })

    pipe.execute()

    latency_ms = int((time.time() - start) * 1000)
    r.execute_command("TS.ADD", LATENCY_SERIES["card"], int(time.time()*1000), latency_ms)

    # print(f"[CARD] Batch {batch_id} → {len(pdf)} rows | {latency_ms} ms")

card_q = (
    card_df.writeStream
        .foreachBatch(write_card)
        .outputMode("append")
        .option("checkpointLocation", "s3a://fraud/checkpoints/card_redis/")
        .start()
)

# ============================================================
# TRANSACTION STREAM
# ============================================================

txn_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("event_epoch_sec", IntegerType()),
    StructField("card_ref", StringType()),
    StructField("merchant_ref", StringType()),
    StructField("amount_minor", IntegerType()),
    StructField("auth_code", StringType()),
    StructField("amount_major", DoubleType()),
    StructField("amount_sign", IntegerType()),
    StructField("currency", StringType()),
    StructField("channel_code", StringType()),
    StructField("entry_mode", StringType()),
    StructField("risk_score_online", DoubleType()),
    StructField("hour_of_day", IntegerType()),
    StructField("is_unusual_hour", IntegerType()),
    StructField("distance_from_home_km", DoubleType()),
    StructField("amount_category", StringType()),
    StructField("exceeds_daily_limit", IntegerType()),
    StructField("amount_deviation_ratio", DoubleType()),
    StructField("days_since_card_issued", IntegerType()),
])

txn_df_raw = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "card_txn_auth")
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), txn_schema).alias("data"))
        .select("data.*")
)

txn_df = (
    txn_df_raw
        .withColumn("amount_minor", greatest(coalesce(col("amount_minor"), lit(0)), lit(0)))
        .withColumn("amount_major", coalesce(col("amount_major"), col("amount_minor") / lit(100.0)))
        .withColumn("amount_sign", when(col("amount_sign") == -1, lit(-1)).otherwise(lit(1)))
        .withColumn("currency", upper(trim(coalesce(col("currency"), lit("VND")))))
        .withColumn("channel_code", upper(trim(coalesce(col("channel_code"), lit("POS")))))
        .withColumn("entry_mode", upper(trim(coalesce(col("entry_mode"), lit("CHIP")))))
        .withColumn("risk_score_online", least(greatest(coalesce(col("risk_score_online"), lit(0.0)), lit(0.0)), lit(1.0)))
        .withColumn("hour_of_day", when((col("hour_of_day") >= 0) & (col("hour_of_day") <= 23), col("hour_of_day")).otherwise(lit(0)))
        .withColumn("is_unusual_hour", when(col("hour_of_day").isin(0, 1, 2, 3, 4, 5), lit(1)).otherwise(lit(0)))
        .withColumn("distance_from_home_km", ps_abs(coalesce(col("distance_from_home_km"), lit(0.0))))
        .withColumn(
            "amount_category",
            when(col("amount_major") < 50, lit("SMALL"))
            .when(col("amount_major") < 200, lit("MEDIUM"))
            .when(col("amount_major") < 1000, lit("LARGE"))
            .otherwise(lit("XLARGE"))
        )
        .withColumn("exceeds_daily_limit", when(col("exceeds_daily_limit") == 1, lit(1)).otherwise(lit(0)))
        .withColumn("amount_deviation_ratio", ps_abs(coalesce(col("amount_deviation_ratio"), lit(0.0))))
        .withColumn("days_since_card_issued", when(col("days_since_card_issued") < 0, 0).otherwise(coalesce(col("days_since_card_issued"), lit(0))))
        .withColumn(
            "trans_date_trans_time",
            from_unixtime(col("event_epoch_sec")).cast("timestamp")
        )
)

def process_and_join(batch_df, batch_id):
    start = time.time()

    if batch_df.isEmpty():
        return

    pdf = batch_df.toPandas()
    r = redis.Redis(host=REDIS_HOST, decode_responses=True)

    global _ts_ready
    if not _ts_ready:
        ensure_timeseries(r)
        _ts_ready = True

    ts_now = int(time.time() * 1000)
    batch_txn_count = 0
    batch_pred_fraud = 0
    batch_amount_minor = 0

    batch_score_buckets = [0] * 10

    for _, row in pdf.iterrows():
        batch_txn_count += 1
        batch_amount_minor += int(row["amount_minor"])

        card_info = r.hgetall(f"c:{row['card_ref']}")

        predicted_fraud = 0
        if card_info:
            user = r.hgetall(f"u:{card_info['party_id']}")
            merch = r.hgetall(f"m:{row['merchant_ref']}")
            predicted_fraud = predict_fraud(row, user, card_info, merch)
            batch_pred_fraud += predicted_fraud

            if predicted_fraud == 1:
                r.execute_command(
                    "XADD", FRAUD_STREAM_KEY, "*",
                    "txn_id", row["event_id"],
                    "amount_minor", int(row["amount_minor"]),
                    "merchant", merch.get("category", "UNK"),
                    "channel", row.get("channel_code", "UNK"),
                    "ts", row["event_ts"]
                )
                r.execute_command("XTRIM", FRAUD_STREAM_KEY, "MAXLEN", "~", 1000)

                today = time.strftime("%Y%m%d", time.gmtime())
                category_key = f"{FRAUD_BY_CATEGORY_HASH_PREFIX}{today}"
                channel_key = f"{FRAUD_BY_CHANNEL_HASH_PREFIX}{today}"
                r.hincrby(category_key, merch.get("category", "UNK"), 1)
                r.hincrby(channel_key, row.get("channel_code", "UNK"), 1)

        score = random.random()
        idx = int(min(9, max(0, score * 10)))
        batch_score_buckets[idx] += 1

        r.set("latest_transaction_amount", float(row["amount_minor"]) / 100.0)

    try:
        r.execute_command("TS.ADD", "ts:txn:count", ts_now, batch_txn_count)
        r.execute_command("TS.ADD", "ts:txn:pred", ts_now, batch_pred_fraud)
        r.execute_command("TS.ADD", "ts:txn:fraud", ts_now, batch_pred_fraud)
        print(f"ts:txn:fraud: {batch_pred_fraud}")
        r.execute_command("TS.ADD", "ts:txn:amount_minor", ts_now, batch_amount_minor)

        for i in range(10):
            r.execute_command(
                "TS.ADD",
                f"{SCORE_BUCKET_PREFIX}{i}_{i+1}",
                ts_now,
                batch_score_buckets[i]
            )
    except Exception as e:
        print(f"[TXN] Failed to write Grafana metrics: {e}")

    latency_ms = int((time.time() - start) * 1000)
    r.execute_command("TS.ADD", LATENCY_SERIES["txn"], int(time.time()*1000), latency_ms)

    # print(f"[TXN] Batch {batch_id} → {len(pdf)} rows | {latency_ms} ms")

txn_q = (
    txn_df.writeStream
        .foreachBatch(process_and_join)
        .outputMode("append")
        .option("checkpointLocation", "s3a://fraud/checkpoints/txn_process/")
        .start()
)

# ============================================================
# WAIT FOR ALL STREAMS
# ============================================================
txn_q.awaitTermination()