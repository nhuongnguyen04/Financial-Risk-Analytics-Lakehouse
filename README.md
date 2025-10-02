# 📊 Financial Risk Analytics Lakehouse  
_Architecture & Standards Documentation_  

---

## 1. 🎯 Mục tiêu dự án
- Xây dựng **Data Lakehouse** cho phân tích rủi ro tài chính và phát hiện gian lận.  
- Kết hợp dữ liệu batch + streaming, phục vụ cả **BI dashboard** và **Machine Learning**.  
- Đảm bảo dữ liệu được quản lý theo **Bronze – Silver – Gold** layer, có tính **scalability, reliability và reproducibility**.  

---

## 2. 🏗 Kiến trúc tổng quan

### 2.1. Thành phần chính
- **Ingestion**
  - Batch: Airflow jobs (datasets từ Kaggle, AlphaVantage, yfinance).  
  - Streaming: Kafka (transactions, logs, auth events).  
- **Storage**
  - Data Lake: MinIO (S3 API), chia bucket theo `bronze/silver/gold`.  
  - Metadata Store: PostgreSQL (catalog, schema, lineage).  
- **Processing**
  - Spark (batch + structured streaming).  
  - PySpark jobs cho ETL và feature engineering.  
- **Orchestration**
  - Airflow để quản lý workflows.  
- **Machine Learning**
  - MLflow cho model training, tracking, registry.  
- **Serving & Monitoring**
  - Streaming scoring (Spark Structured Streaming + Kafka).  
  - Alerts → Kafka/Elasticsearch + Kibana.  
  - BI Dashboard: Apache Superset.  
  - Monitoring: Prometheus + Grafana.  

---

### 2.2. Data Flow

1. **Ingest**
   - Kafka Producers → transaction stream, log stream.  
   - Batch loader → market data, customer info, historical datasets.  
2. **Bronze Layer**
   - Lưu raw data nguyên bản (schema-on-read, partition theo ngày).  
3. **Silver Layer**
   - Chuẩn hóa schema, deduplicate, enrich (timezone, geo, join customer info).  
   - Hash/anonymize PII.  
4. **Gold Layer**
   - Fact & Dim tables, feature tables cho ML.  
   - Aggregations cho BI.  
5. **Machine Learning**
   - Offline training (batch).  
   - Online scoring (streaming).  
6. **Serving**
   - Alerts & risk scoring → Kafka topic + Dashboard.  

---

## 3. 📂 Data Layers & Schema

### 3.1. Bronze
- **Mục tiêu**: Raw data, immutable.  
- **Nguồn**: PaySim, Bank Customers, Alpha Vantage, Loghub, synthetic streams.  
- **Quy chuẩn**:
  - Lưu dưới định dạng Parquet/JSON.  
  - Partition theo `dt=YYYY-MM-DD`.  
  - Không đổi schema, chỉ thêm metadata.  

### 3.2. Silver
- **Mục tiêu**: Data đã làm sạch, chuẩn hóa.  
- **Xử lý**:
  - Deduplication.  
  - Standardize timestamp → UTC.  
  - Normalize schema (field naming convention).  
  - Hash/anonymize PII (`customer_id`, `device_id`).  
- **Quy chuẩn**:
  - Schema cố định.  
  - Lưu ở Parquet + Delta format (support ACID).  

### 3.3. Gold
- **Mục tiêu**: Data phục vụ BI/ML.  
- **Tables**:
  - `fact_transactions`  
  - `dim_customers`  
  - `fact_market_quotes`  
  - `features_transaction_risk`  
- **Quy chuẩn**:
  - Star schema cho BI.  
  - Feature store document hóa rõ ràng.  

---

## 4. 📈 Use Cases

1. **Fraud Detection**  
   - ML model dự đoán gian lận theo transaction stream.  
2. **Credit Risk Scoring**  
   - Dựa trên lịch sử vay + market indicators.  
3. **Operational Risk Analytics**  
   - Phân tích logs (auth/system) để phát hiện bất thường.  
4. **Business Intelligence**  
   - Báo cáo giao dịch, khách hàng, thị trường.  

---

## 5. 🧩 Quy chuẩn đặt tên & quản lý dữ liệu

### 5.1. Naming conventions
- **Buckets**: `lake/{layer}/{domain}/{table}/{dt}`  
- **Tables**: `layer_domain_table`  
- **Fields**: lowercase, snake_case.  

### 5.2. Data governance
- **Schema Registry** (PostgreSQL + Glue-like table).  
- **Versioning**: Delta Lake.  
- **PII Handling**: Hashing (SHA256), không lưu plaintext.  

---

## 6. 🤖 Machine Learning Workflow

1. **Data Prep**: Silver/Gold tables → feature engineering.  
2. **Model Training**: XGBoost/LightGBM.  
3. **Model Tracking**: MLflow (params, metrics, artifacts).  
4. **Model Registry**: promote từ `Staging` → `Production`.  
5. **Serving**:  
   - Batch scoring (Airflow).  
   - Real-time scoring (Spark Structured Streaming + Kafka).  

---

## 7. 🛠 Operational Requirements

- **Orchestration**: Airflow DAG cho ingest, transform, training, monitoring.  
- **Monitoring**:  
  - Prometheus (infra, Kafka, Spark metrics).  
  - Grafana dashboards.  
- **Data Quality**: Great Expectations/Deequ validation.  
- **Logging**: Centralized logs (ELK stack).  
- **Resilience**:  
  - Retry cơ chế trong Airflow & Spark.  
  - Kafka replication factor ≥ 2.  

---

## 8. 📦 Deliverables

- Docker Compose environment (Kafka, MinIO, Spark, PostgreSQL, Airflow, MLflow).  
- Data Lakehouse (Bronze/Silver/Gold).  
- Feature store & schema dictionary.  
- ML models (logged in MLflow).  
- Streaming scoring + alerts.  
- BI dashboards (Superset/Kibana).  
- Monitoring setup (Prometheus + Grafana).  
- Documentation:  
  - Architecture design.  
  - Data schemas.  
  - Runbook vận hành.  

---

## 9. ✅ Quy tắc kiểm tra & review

- **Code**: Theo PEP8 (Python), linting với `black` & `flake8`.  
- **ETL Jobs**: Unit test bằng `pytest`.  
- **Data Pipeline**: Validation trước khi đẩy Silver/Gold.  
- **Model**: Yêu cầu log ROC-AUC ≥ 0.85 trước khi promote Production.  
- **Documentation**: Mọi schema & pipeline đều phải có markdown/docs kèm theo.  

---
