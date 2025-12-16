
# SYSTEM ARCHITECTURE
<img width="1007" height="566" alt="image" src="https://github.com/user-attachments/assets/93773c37-8007-48e8-86b1-ca2c93141c54" />

# FOLDER STRUCTURE
```
ASSIGNMENT/
│
├── grafana/                        # Grafana deployment & dashboard
│   ├── grafana.yaml                # K8s config deploy Grafana
│   └── fraud-dashboard.json        # Dashboard import vào Grafana
│
├── kafka/                          # Kafka deployment on Kubernetes
│   ├── kafka.yaml                  # K8s config deploy Kafka
│   └── kafka-nodeport.yaml         # Expose Kafka qua NodePort
│
├── spark/                          # Spark deployment on Kubernetes
│   └── spark.yaml                  # K8s config deploy Spark
│
├── redis/                          # Redis deployment on Kubernetes
│   ├── redis.yaml                  # K8s config deploy Redis
│   └── redis-nodeport.yaml         # Expose Redis qua NodePort
│
├── minio/                          # MinIO (object storage) deployment
│   └── minio.yaml                  # K8s config deploy MinIO
│
├── fraud-prediction/               # Real-time fraud detection pipeline
│   │
│   ├── spark/                      # Spark Structured Streaming
│   │   ├── stream-all.py           # Long-running Spark job:
│   │   │                             # - Consume data từ Kafka
│   │   │                             # - Lưu raw data vào MinIO
│   │   │                             # - Xử lý & feature engineering
│   │   │                             # - Push intermediate + prediction vào Redis
│   │   └── requirements.txt        # Dependencies cho Spark job
│   │
│   ├── producer/                   # Kafka data producer
│   │   ├── kafka-producer.py       # Gọi generator và đẩy data vào Kafka
│   │   ├── requirements.txt        # Dependencies cho producer
│   │   └── utils/
│   │       └── generator.py        # Logic sinh synthetic data
│
├── model/                          # Offline training artifacts & results
│   │
│   ├── artifacts/                  # Trained models & preprocessors
│   │   ├── processor.pkl           # Preprocessing pipeline
│   │   ├── xgb_model.pkl           # XGBoost model
│   │   ├── lgb_model.pkl           # LightGBM model
│   │   ├── cat_model.pkl           # CatBoost model
│   │   └── feature_names.pkl       # Feature list
│   │
│   └── plots/                      # Training & evaluation outputs
│       ├── external_test_result.png
│       ├── XGBoost_evaluation.png
│       ├── LightGBM_evaluation.png
│       └── CatBoost_evaluation.png
│
└── document.txt                    # Hướng dẫn hệ thống
```

# HƯỚNG DẪN CHẠY HỆ THỐNG FRAUD DETECTION (END-TO-END)

## 1. Chuẩn bị môi trường Kubernetes
- Cài đặt Kubernetes (kubeadm).
- Kiểm tra cluster:
  Lấy thông tin các node trong cluster:  `kubectl get nodes -o wide`

## 2. Deploy framework lên Kubernetes
- Apply toàn bộ các file YAML để dựng hạ tầng xử lý streaming
- Kiểm tra các service/pod:  

  Lấy thông tin các pod đang chạy:       `kubectl get pods -A -o wide`
  
  Lấy thông tin các port đang mở:        `kubectl get svc -A`

## 3. Sinh dữ liệu và đẩy vào Kafka
Kafka producer sẽ sinh synthetic transaction data và gửi vào Kafka topic

`cd fraud-prediction/producer`

`pip install -r requirements.txt`

`python kafka-producer.py`

## 4. Chạy Spark Structured Streaming Job
- Spark job là long-running, luôn lắng nghe dữ liệu từ Kafka.
- Truy cập vào Spark Master Pod. Ví dụ:
  `kubectl exec -it -n kafka spark-master-5f5d65ff69-7hqfq -- bash`
- Submit Spark Streaming Job
  ```
  /opt/spark/bin/spark-submit
  --master spark://spark-master:7077
  --deploy-mode client
  --conf spark.jars.ivy=/tmp/.ivy2
  --packages
  org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,
  org.apache.hadoop:hadoop-aws:3.3.4,
  com.amazonaws:aws-java-sdk-bundle:1.12.262
  /opt/spark/stream-all.py
  ```
