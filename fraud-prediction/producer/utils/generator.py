# fraud_data_gen.py
# =========================
# Bộ hàm sinh data cho 4 topic: user_profile, card_account, merchant_profile, card_txn_auth
# MỤC TIÊU:
# - KHÔNG có field nào trùng với flat table ban đầu
# - Các field phải "dơ", đổi tên, chia nhỏ, để downstream Spark phải JOIN + FEATURE ENGINEERING
# - Code giữ nguyên toàn bộ comment giải thích để dễ maintain
# - FEATURE ENGINEERING: thêm các field hỗ trợ fraud detection & ML pipeline
# =========================

import uuid
import random
import math
from datetime import datetime, timezone

# ============================================================
# Noise injection để tạo dữ liệu "bẩn" cho bước preprocessing
# ============================================================

def introduce_noise(record_type: str, record: dict) -> dict:
    """Inject deliberately messy values while keeping required keys present."""
    noisy = dict(record)

    def _pick(mutations, max_apply=2):
        count = random.randint(1, min(max_apply, len(mutations)))
        for fn in random.sample(mutations, count):
            fn(noisy)

    if record_type == "user_profile":
        _pick([
            lambda r: r.update({"income_level": random.choice(["", None, "LOW", "MEDIUM", "HIGH", "UNKNOWN"])}),
            lambda r: r.update({"address_line": f"  {r.get('address_line','').title()}   "}),
            lambda r: r.update({"birth_year": r.get("birth_year", 1980) + random.randint(-8, 12)}),
            lambda r: r.update({"home_latitude": None if random.random() < 0.4 else r.get("home_latitude")}),
            lambda r: r.update({"region_code": random.choice([r.get("region_code", "").lower(), "??", None])}),
        ])

    elif record_type == "card_account":
        _pick([
            lambda r: r.update({"days_since_issuance": r.get("days_since_issuance", 0) + random.randint(-120, 365)}),
            lambda r: r.update({"daily_limit_minor": random.choice([r.get("daily_limit_minor", 0), -100000, 0, 999999999])}),
            lambda r: r.update({"product_type": random.choice(["", "CREDIT ", " debit", None])}),
            lambda r: r.update({"brand": random.choice(["VISA", "MC", "???", None])}),
        ])

    elif record_type == "merchant_profile":
        _pick([
            lambda r: r.update({"geo_lat": r.get("geo_lat", 0) + random.uniform(-1.5, 1.5)}),
            lambda r: r.update({"geo_lon": r.get("geo_lon", 0) + random.uniform(-1.5, 1.5)}),
            lambda r: r.update({"mcc_group": random.choice(["", "UNK", r.get("mcc_group", "FOOD")])}),
            lambda r: r.update({"merchant_display_name": r.get("merchant_display_name", "Merchant").lower()}),
            lambda r: r.update({"risk_score_merchant": round(random.uniform(-0.2, 1.3), 3)}),
        ])

    elif record_type == "card_txn_auth":
        _pick([
            lambda r: r.update({"amount_minor": int(random.choice([r.get("amount_minor", 0), -abs(r.get("amount_minor", 0)), r.get("amount_minor", 0) * 20 or 1]))}),
            lambda r: r.update({"amount_major": str(r.get("amount_major", 0.0)) + "  "}),
            lambda r: r.update({"currency": random.choice(["vnd", "usd", " "])}),
            lambda r: r.update({"hour_of_day": random.choice([r.get("hour_of_day", 0), "", -1, 27])}),
            lambda r: r.update({"channel_code": random.choice(["POS", "ECOM", "", None])}),
            lambda r: r.update({"fraud_flag": random.choice([0, 1, 1, 2])}),
        ], max_apply=3)

    return noisy

# =========================
# Static "dimension" data
# (giống như location_dim / city_dim)
# =========================

CITY_MASTER = [
    {
        "location_id": "VN-HCM-01",
        "city_name": "Ho Chi Minh City",
        "country_code": "VN",
        "region_code": "HCM",
        "postal_code": "700000",
        "base_latitude": 10.7769,
        "base_longitude": 106.7009,
        "population_size": 9000000,
    },
    {
        "location_id": "VN-HN-01",
        "city_name": "Ha Noi",
        "country_code": "VN",
        "region_code": "HN",
        "postal_code": "100000",
        "base_latitude": 21.0278,
        "base_longitude": 105.8342,
        "population_size": 8000000,
    },
    {
        "location_id": "VN-DN-01",
        "city_name": "Da Nang",
        "country_code": "VN",
        "region_code": "DN",
        "postal_code": "550000",
        "base_latitude": 16.0544,
        "base_longitude": 108.2022,
        "population_size": 1200000,
    },
]

MERCHANT_CATEGORIES = ["ELEC", "FOOD", "TRVL", "FASH", "GROC", "SERV"]
JOBS = ["software_engineer","data_scientist","accountant","teacher","doctor","sales_rep"]
GENDERS = ["M", "F"]
CURRENCIES = ["VND"]

# =========================
# Helper functions
# =========================

def random_city():
    """Chọn một city_dim record bất kỳ (giả lập dimension table)."""
    return random.choice(CITY_MASTER)

def random_birthdate():
    """Sinh ngày sinh đơn giản trong khoảng 1970-2000."""
    year = random.randint(1970, 2000)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return datetime(year, month, day, tzinfo=timezone.utc)

def random_vietnamese_name():
    """
    Sinh tên kiểu Việt rất đơn giản.
    Thực tế có thể dùng list lớn hơn hoặc Faker.
    """
    first_names = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Vo", "Dang"]
    middle_names = ["Van", "Thi", "Thanh", "Duc", "Minh", "Ngoc"]
    last_names = ["Anh", "Binh", "Hoa", "Tuan", "Linh", "Hung", "Trang"]

    family_name = random.choice(first_names)
    given_name = f"{random.choice(middle_names)} {random.choice(last_names)}"
    full_name = f"{family_name} {given_name}"
    return family_name, given_name, full_name

def random_amount_minor():
    """
    Sinh số tiền theo distribution giống code JS:
    - nhiều giao dịch nhỏ, ít giao dịch lớn
    Trả về đơn vị 'minor' (VND * 100) -> integer.
    """
    r = random.random()
    if r < 0.7:
        amt = random.random() * 50
    elif r < 0.95:
        amt = 50 + random.random() * 450
    else:
        amt = 500 + random.random() * 4500
    return int(round(amt * 100))

# =========================
# Feature Engineering Helpers
# =========================

def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Tính khoảng cách Haversine giữa 2 điểm (đơn vị: km)
    Dùng để detect fraud: giao dịch ở nơi xa so với địa chỉ nhà
    """
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return 0.0
    
    R = 6371  # bán kính Trái Đất (km)
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return R * c

def get_hour_of_day(timestamp_str: str) -> int:
    """Extract giờ từ ISO timestamp"""
    try:
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return dt.hour
    except:
        return 0

def is_unusual_time(hour: int) -> int:
    """Fraud indicator: giao dịch vào giờ lạ (0-5 sáng)"""
    return 1 if hour in [0, 1, 2, 3, 4, 5] else 0

def get_amount_category(amount_minor: int) -> str:
    """Phân loại mức tiền"""
    amount_usd = amount_minor / 100.0
    if amount_usd < 50:
        return "SMALL"
    elif amount_usd < 200:
        return "MEDIUM"
    elif amount_usd < 1000:
        return "LARGE"
    else:
        return "XLARGE"

def validate_and_clean_field(value, expected_type, default=None):
    """Xử lý null/invalid values"""
    if value is None or value == "":
        return default
    try:
        if expected_type == int:
            return int(value)
        elif expected_type == float:
            return float(value)
        elif expected_type == str:
            return str(value).strip()
    except:
        return default
    return value

# =========================
# Topic 1: user_profile
# =========================

def gen_user_profile_event():
    """
    Giả lập record cho topic: user_profile
    Các field tương ứng từ flat table cũ được CHIA NHỎ / ĐỔI TÊN:
    - không có first, last → dùng given_name, family_name, full_name
    - dob → birth_date + birth_year
    - city, state, zip, lat, long, city_pop → JOIN qua CITY_MASTER bằng location_id_home
    - cc_num hoàn toàn không tồn tại → thay bằng party_id
    
    FEATURE ENGINEERING:
    - age: tuổi tính từ birth_date
    - age_group: phân loại nhóm tuổi
    """
    now = datetime.now(timezone.utc)
    city_dim = random_city()

    family, given, full = random_vietnamese_name()
    birth_dt = random_birthdate()

    party_id = f"party_{100000 + random.randint(0, 899999)}"
    
    # Calculate age
    age = now.year - birth_dt.year
    if age < 0:
        age = 0
    elif age > 100:
        age = 100
    
    # Age group for fraud patterns
    if age < 25:
        age_group = "18-24"
    elif age < 35:
        age_group = "25-34"
    elif age < 45:
        age_group = "35-44"
    elif age < 55:
        age_group = "45-54"
    elif age < 65:
        age_group = "55-64"
    else:
        age_group = "65+"

    event = {
        "event_id": str(uuid.uuid4()),
        "event_ts": now.isoformat(),

        # business key thay cho cc_num
        "party_id": party_id,

        # tên tách nhỏ 
        "given_name": given,
        "family_name": family,
        "full_name": full,

        "gender_code": random.choice(GENDERS),

        # dob chia nhỏ
        "birth_date": birth_dt.date().isoformat(),
        "birth_year": birth_dt.year,
        
        # FEATURE ENGINEERING
        "age": age,
        "age_group": age_group,
        "income_level": random.choice(["LOW", "MEDIUM", "HIGH"]),

        "occupation_title": random.choice(JOBS),

        # địa chỉ mapping ngược từ city dim
        "address_line": "Some random street",
        "region_code": city_dim["region_code"],
        "home_postal_hint": city_dim["postal_code"],
        "location_id_home": city_dim["location_id"],
        
        # GEOGRAPHIC FEATURES
        "home_latitude": validate_and_clean_field(city_dim["base_latitude"], float, 0.0),
        "home_longitude": validate_and_clean_field(city_dim["base_longitude"], float, 0.0),
    }

    return event


# =========================
# Topic 2: card_account
# =========================

def gen_card_account_event(party_id, location_id_home):
    """
    Giả lập topic card_account
    - Không chứa cc_num
    - Dùng card_ref + card_pan_hash làm key JOIN sang transaction
    
    FEATURE ENGINEERING:
    - days_since_issuance: số ngày kể từ cấp thẻ (fraud indicator)
    - card_age_category: mới, cũ
    """
    now = datetime.now(timezone.utc)

    # số PAN giả → hash
    pan_num = str(4000000000000000 + random.randint(0, 999999999999999))
    card_ref = f"card_{uuid.uuid4().hex[:12]}"
    
    # Card issuance date (1-3 năm trước)
    days_old = random.randint(30, 1095)
    
    # Card age category for fraud patterns
    if days_old < 30:
        card_age_cat = "NEW"
    elif days_old < 180:
        card_age_cat = "RECENT"
    elif days_old < 365:
        card_age_cat = "MODERATE"
    else:
        card_age_cat = "ESTABLISHED"

    event = {
        "event_id": str(uuid.uuid4()),
        "event_ts": now.isoformat(),

        "party_id_fk": party_id,            # join sang user_profile
        "card_ref": card_ref,               # join sang txn
        "card_pan_hash": f"h_{hash(pan_num) & 0xffffffff:x}",
        "card_pan_last4": pan_num[-4:],     # giống cc_num cuối 4 số

        "billing_location_id": location_id_home,

        "product_type": random.choice(["CREDIT","DEBIT"]),
        "brand": random.choice(["VISA","MASTERCARD","JCB"]),
        
        # FEATURE ENGINEERING
        "days_since_issuance": validate_and_clean_field(days_old, int, 0),
        "card_age_category": card_age_cat,
        "is_primary_card": random.choice([0, 0, 1]),  # 2/3 xác suất là thẻ chính
        "daily_limit_minor": int(random.choice([500000, 1000000, 2000000, 5000000])),  # VND * 100
    }
    return event


# =========================
# Topic 3: merchant_profile
# =========================

def gen_merchant_profile_event():
    """
    Merchant profile:
    - không dùng merchant, merch_lat, merch_long
    - đổi thành merchant_id, merchant_display_name, geo_lat/geo_lon
    
    FEATURE ENGINEERING:
    - risk_score_merchant: fraud risk của merchant (0-1)
    - avg_txn_amount: trung bình tiền giao dịch
    - merchant_type: loại hình kinh doanh
    """
    now = datetime.now(timezone.utc)
    city_dim = random_city()

    merchant_id = f"m_{1000 + random.randint(0,9999)}"

    # location lệch nhẹ quanh city center
    geo_lat = validate_and_clean_field(city_dim["base_latitude"] + (random.random() - 0.5) * 0.02, float, 0.0)
    geo_lon = validate_and_clean_field(city_dim["base_longitude"] + (random.random() - 0.5) * 0.02, float, 0.0)
    
    mcc_group = random.choice(MERCHANT_CATEGORIES)
    
    # Merchant fraud risk score (dựa trên category)
    fraud_risk_map = {
        "ELEC": 0.15,
        "FOOD": 0.05,
        "TRVL": 0.20,
        "FASH": 0.12,
        "GROC": 0.03,
        "SERV": 0.18,
    }
    base_risk = fraud_risk_map.get(mcc_group, 0.10)
    merchant_risk = base_risk + random.uniform(-0.05, 0.05)
    merchant_risk = max(0.0, min(1.0, merchant_risk))

    event = {
        "event_id": str(uuid.uuid4()),
        "event_ts": now.isoformat(),

        "merchant_id": merchant_id,
        "merchant_display_name": f"Merchant #{random.randint(1,9999)}",

        "mcc_group": mcc_group,

        # lat/long mới
        "geo_lat": geo_lat,
        "geo_lon": geo_lon,

        "location_id_operate": city_dim["location_id"],
        
        # FEATURE ENGINEERING
        "risk_score_merchant": round(merchant_risk, 3),
        "avg_txn_amount_minor": int(random.choice([100000, 300000, 500000, 1000000])),  # VND * 100
        "merchant_type": random.choice(["RETAIL", "ONLINE", "PHYSICAL", "BOTH"]),
        "is_high_risk": 1 if merchant_risk > 0.15 else 0,
    }
    return event


# =========================
# Topic 4: card_txn_auth
# =========================

def gen_card_txn_auth_event(card_account, merchant_profile, user_profile=None):
    """
    Main transaction stream:
    - Không có amt float → amount_minor + amount_sign
    - Không có city/lat/long → JOIN merchant_profile + user_profile
    - Không có trans_num → auth_code
    
    FEATURE ENGINEERING:
    - hour_of_day: giờ giao dịch (0-23)
    - is_unusual_time: fraud indicator cho giao dịch vào giờ lạ
    - distance_from_home: khoảng cách từ nhà
    - amount_category: SMALL/MEDIUM/LARGE/XLARGE
    - exceeds_daily_limit: vượt quá daily limit
    - amount_deviation: so với average merchant amount
    - amount_in_major: chuyển sang đơn vị major (VND)
    """
    now = datetime.now(timezone.utc)

    amount_minor = random_amount_minor()
    amount_sign = random.choice([1, 1, 1, 1, -1])  # đa số dương
    amount_major = amount_minor / 100.0  # Chuyển sang VND
    
    hour_of_day = now.hour
    is_unusual = is_unusual_time(hour_of_day)

    fraud_flag = 1 if random.random() < 0.005 else 0
    auth_code = f"AUTH-{random.randint(100000, 99999999)}"
    
    # Calculate distance from home if user_profile available
    distance_km = 0.0
    if user_profile:
        distance_km = calculate_distance(
            user_profile.get("home_latitude", 0),
            user_profile.get("home_longitude", 0),
            merchant_profile.get("geo_lat", 0),
            merchant_profile.get("geo_lon", 0)
        )
    
    # Amount features
    amount_cat = get_amount_category(amount_minor)
    exceeds_limit = 1 if amount_minor > card_account.get("daily_limit_minor", 5000000) else 0
    
    # Amount deviation from merchant average
    merch_avg = card_account.get("daily_limit_minor", 1000000)
    amount_dev = abs(amount_minor - merch_avg) / max(merch_avg, 1)
    
    # Fraud probability based on features
    fraud_prob = 0.005
    if is_unusual == 1:
        fraud_prob += 0.05
    if exceeds_limit == 1:
        fraud_prob += 0.08
    if distance_km > 50:
        fraud_prob += 0.05
    if amount_cat in ["LARGE", "XLARGE"]:
        fraud_prob += 0.03
    fraud_prob = min(0.99, fraud_prob)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_ts": now.isoformat(),
        "event_epoch_sec": int(now.timestamp()),

        # JOIN keys:
        "card_ref": card_account["card_ref"],
        "card_pan_hash": card_account["card_pan_hash"],
        "merchant_ref": merchant_profile["merchant_id"],

        # amount kiểu mới
        "amount_minor": validate_and_clean_field(amount_minor, int, 0),
        "amount_major": round(amount_major, 2),
        "amount_sign": amount_sign,
        "currency": "VND",

        "channel_code": random.choice(["POS","ECOM","MOBILE"]),
        "entry_mode": random.choice(["CHIP","MAGSTRIPE","CONTACTLESS","MANUAL"]),

        "auth_code": auth_code,

        "fraud_flag": fraud_flag,
        "risk_score_online": round(random.uniform(0, 1), 3),
        
        # FEATURE ENGINEERING
        "hour_of_day": validate_and_clean_field(hour_of_day, int, 0),
        "is_unusual_hour": is_unusual,
        "distance_from_home_km": round(distance_km, 2),
        "amount_category": amount_cat,
        "exceeds_daily_limit": exceeds_limit,
        "amount_deviation_ratio": round(amount_dev, 3),
        "txn_fraud_probability": round(fraud_prob, 3),
        "days_since_card_issued": card_account.get("days_since_issuance", 0),
    }

    return event


# =========================
# Orchestration: tạo 1 bundle đầy đủ 4 topic
# =========================

def generate_bundle():
    """
    Sinh 1 bộ event:
    1 user_profile
    1 card_account (liên kết user)
    1 merchant_profile
    1 card_txn_auth (liên kết card + merchant)

    Mục đích:
    - Producer sẽ gọi từng event và gửi vào đúng Kafka topic
    - FEATURE ENGINEERING đã bao gồm: age, card_age, distance, fraud indicators
    """
    user_ev = gen_user_profile_event()
    card_ev = gen_card_account_event(
        party_id=user_ev["party_id"],
        location_id_home=user_ev["location_id_home"]
    )
    merchant_ev = gen_merchant_profile_event()
    txn_ev = gen_card_txn_auth_event(card_ev, merchant_ev, user_ev)

    # Inject noise để Spark preprocessing phải xử lý nhiều hơn
    user_ev = introduce_noise("user_profile", user_ev)
    card_ev = introduce_noise("card_account", card_ev)
    merchant_ev = introduce_noise("merchant_profile", merchant_ev)
    txn_ev = introduce_noise("card_txn_auth", txn_ev)

    return {
        "user_profile": user_ev,
        "card_account": card_ev,
        "merchant_profile": merchant_ev,
        "card_txn_auth": txn_ev,
    }
