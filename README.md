# ⚡ Financial Risk Analytics Lakehouse  
_Kappa Architecture with Apache Iceberg (Unified Streaming Lakehouse)_

---

## 1. 🎯 Mục tiêu dự án

- Xây dựng **Real-time Data Lakehouse** phục vụ phân tích rủi ro tài chính & phát hiện gian lận theo **Kappa Architecture**.  
- Thay thế kiến trúc Lambda cũ (batch + stream song song) bằng **pipeline streaming duy nhất**, dễ vận hành và tiết kiệm tài nguyên.  
- Sử dụng **Apache Iceberg** làm open table format thống nhất cho batch & streaming workloads (ACID, Time Travel, Schema Evolution).  
- Hỗ trợ phân tích BI & Machine Learning trên dữ liệu real-time và lịch sử.

---

## 2. 🏗 Kiến trúc tổng quan

### 2.1. Thành phần chính

| Layer | Công cụ | Vai trò |
|-------|----------|---------|
| **Ingestion** | Apache Kafka | Log trung tâm: transactions, logs, events |
| **Processing** | Spark Structured Streaming | Xử lý streaming, transform, enrich và ghi trực tiếp vào Iceberg |
| **Storage** | Apache Iceberg on MinIO (S3 API) | Lưu dữ liệu hợp nhất (batch + stream) với snapshot, ACID, schema evolution |
| **Metadata & Catalog** | PostgreSQL | Metadata catalog cho Iceberg |
| **Orchestration** | Apache Airflow | Quản lý job streaming, replay, validation |
| **Machine Learning** | MLflow | Tracking, model registry, serving |
| **Monitoring** | Prometheus + Grafana | Theo dõi hiệu năng Spark/Kafka |
| **Serving / BI** | Superset, Elasticsearch, Kafka Consumers | Dashboard & alert real-time |

---

### 2.2. Data Flow (Kappa Architecture)

```mermaid
flowchart LR
    subgraph Source["📥 Data Sources"]
        A1["Finance APIs (AlphaVantage, yfinance)"]
        A2["Transaction Systems"]
        A3["Log Streams"]
    end

    subgraph Kafka["🌀 Apache Kafka (Event Log)"]
        K1["Transactions Topic"]
        K2["Logs Topic"]
    end

    subgraph Spark["⚡ Spark Structured Streaming"]
        S1["Stream Processor"]
        S2["Data Cleaning & Enrichment"]
        S3["Feature Extraction"]
    end

    subgraph Iceberg["🧊 Apache Iceberg on MinIO"]
        B["Bronze Layer<br/>(Raw Stream)"]
        S["Silver Layer<br/>(Cleansed)"]
        G["Gold Layer<br/>(Analytics/ML)"]
    end

    subgraph Catalog["🗂 PostgreSQL Catalog"]
        C["Iceberg Metadata<br/>Schema & Snapshots"]
    end

    subgraph ML["🤖 ML & Analytics"]
        M1["MLflow Tracking"]
        M2["Real-time Scoring"]
        M3["Superset / DuckDB / BI"]
    end

    subgraph Monitoring["📈 Monitoring & Orchestration"]
        F["Airflow DAGs"]
        P["Prometheus & Grafana"]
    end

    %% Connections
    Source --> Kafka
    Kafka --> Spark
    Spark --> Iceberg
    Iceberg --> ML
    Iceberg -->|Snapshots & Time Travel| Catalog
    ML --> Kafka
    Iceberg --> Monitoring
    Monitoring --> Spark
 

---

## 3. 📂 Data Layers & Schema

### 3.1. Bronze (Raw Stream)
- **Nguồn**: Kafka topics (transactions, auth logs, market data).  
- **Định dạng**: Iceberg table (append-only).  
- **Partition**: theo `event_date=YYYY-MM-DD`.  
- **Schema**: schema-on-read, immutable.  

### 3.2. Silver (Cleansed)
- Chuẩn hóa timestamp, loại bỏ trùng, enrich từ master data.  
- Hash/anonymize PII (`customer_id`, `device_id`).  
- Schema fixed và được quản lý trong Iceberg catalog (PostgreSQL).  

### 3.3. Gold (Analytics & ML)
- Fact/Dim tables và feature tables.  
- Star schema cho BI và feature store cho ML.  
- Hỗ trợ **time travel** để phân tích quá khứ hoặc audit model drift.

---

## 4. 📈 Use Cases

1. **Fraud Detection**  
   - Real-time model scoring trên stream transactions.  
2. **Credit Risk Scoring**  
   - ML model dự đoán rủi ro tín dụng theo hồ sơ lịch sử & dữ liệu thị trường.  
3. **Operational Risk Analytics**  
   - Phân tích logs hệ thống & hành vi bất thường.  
4. **Real-time BI**  
   - Dashboard cập nhật theo stream data từ Iceberg.

---


## 5. 🤖 Machine Learning Workflow

1. **Feature Engineering**: Lấy data từ Silver/Gold Iceberg tables.  
2. **Model Training**: XGBoost / LightGBM trên Spark hoặc sklearn.  
3. **Model Tracking**: MLflow (params, metrics, artifacts).  
4. **Model Registry**: promote từ `Staging` → `Production`.  
5. **Real-time Scoring**: Spark Structured Streaming + Kafka output.  

---

## 6. 🛠 Operational Requirements

- **Orchestration**: Airflow DAG khởi chạy, giám sát và replay streaming jobs.  
- **Monitoring**:  
  - Prometheus cho Kafka, Spark metrics.  
  - Grafana dashboards.  
- **Data Quality**: Great Expectations / Deequ validation.  
- **Resilience**:  
  - Checkpointing + Exactly-once với Spark Structured Streaming.  
  - Kafka replication factor ≥ 2.  
- **Compaction & Maintenance**:  
  - Iceberg `rewrite_data_files` + `expire_snapshots` định kỳ.

---

## 7. 📦 Deliverables

- ✅ Docker Compose environment: Kafka, Spark, PostgreSQL, MinIO, Airflow, MLflow.  
- ✅ Unified Kappa pipeline: Spark Structured Streaming → Iceberg.  
- ✅ Iceberg Catalog trên PostgreSQL + MinIO.  
- ✅ Streaming ETL templates (`stream_to_iceberg.py`, `query_iceberg.py`).  
- ✅ Replay job cho historical data.  
- ✅ ML models logged trong MLflow.  
- ✅ Real-time alerts qua Kafka/Elasticsearch.  
- ✅ BI dashboards (Superset).  
- ✅ Monitoring setup (Prometheus + Grafana).  
- ✅ Documentation & runbook.

---

## 8. ✅ Quy tắc kiểm tra & review

- **Code**: Theo PEP8, linting bằng `black`, `flake8`.  
- **Streaming Jobs**: Test logic với `pytest` và mini Kafka.  
- **Data Validation**: Check schema, null, duplicate trước khi ghi Iceberg.  
- **Model**: ROC-AUC ≥ 0.85 trước khi promote Production.  
- **Documentation**: Mọi schema & pipeline có markdown mô tả rõ ràng.  

---

## 9. 🌊 Hướng phát triển tiếp theo

- **Flink Integration**: thử nghiệm Flink Agents (FLIP-531) chạy song song Spark.  
- **Iceberg SQL Catalog**: mở rộng sang Trino/DuckDB.  
- **Streaming Database Layer**: tích hợp RisingWave hoặc Materialize để query stream SQL.  
- **Shift Left Data Quality**: áp dụng kiểm soát schema & quality ngay từ ingestion layer.  

---

