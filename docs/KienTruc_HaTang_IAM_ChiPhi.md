# Hạ tầng, Orchestration, IAM & Chi phí GCP — Cường (Platform/Orchestration Engineer)

Đồ án 8 — Chuỗi cung ứng & Logistics · Cloud bắt buộc: **GCP** · Hiện đang build trên
**phương án dự phòng local (Docker Compose)** theo Mục 4b tài liệu tổng hợp, do hạn
chế thẻ tín dụng/tài khoản GCP (xem `File_HuongDan_PhamVanQuynh.md`). Kiến trúc, star
schema và code giữ nguyên khi "bê" lên GCP thật — chỉ đổi endpoint kết nối.

## 1. Sơ đồ kiến trúc

### 1a. Kiến trúc target trên GCP (bắt buộc theo đề bài)

```
Nguồn dữ liệu                Ingestion              Xử lý & Lưu trữ
──────────────               ─────────              ────────────────
DataCo/Olist CSV  ──batch──▶ Dataflow/Datastream ─▶ GCS (raw)
                                                        │
Kafka producer     ──stream─▶ Managed Service      ─▶ Dataproc PySpark
(mô phỏng tracking)           for Apache Kafka          (batch: lead time,
                                    │                     delay, ghép chặng)
                                    ▼                        │
                              Dataproc Structured             ▼
                              Streaming (windowing)      GCS (cleansed→curated,
                                    │                     Parquet, partition
                                    └────────────┬───────  ship_date+warehouse)
                                                 ▼
                                         BigQuery staging
                                                 │
                                                 ▼
                    ┌───────────────── Cloud Composer (Airflow) ─────────────────┐
                    │ DAG: ingest → Dataproc → load BigQuery → dbt → dashboard   │
                    └────────────────────────────┬────────────────────────────────┘
                                                 ▼
                                    dbt-bigquery (staging → marts)
                                                 │
                                                 ▼
                              BigQuery star schema (Fact_Shipment + 4 Dim)
                                                 │
                              ┌──────────────────┼──────────────────┐
                              ▼                  ▼                  ▼
                      Looker Studio        BigQuery ML          Vertex AI
                      (dashboard SLA)   (dự đoán trễ giao hàng) (gợi ý tuyến)
```

### 1b. Kiến trúc đang chạy — Docker Compose local (phương án dự phòng)

```
scripts/upload_to_minio.py ─▶ MinIO (bucket "raw", thay GCS)
kafka_producer.py (continuous) ─▶ Kafka + Zookeeper (Docker, thay Managed Kafka)
                                        │
                              [PySpark batch job — Khang, chưa hoàn thiện]
                                        │
                                        ▼
                          DuckDB (logistics.duckdb) / Postgres — thay BigQuery
                                        │
                              dbt-duckdb (staging → marts)
                                        │
                        ┌───────────────┼───────────────┐
                        ▼                                ▼
                 Metabase/Superset               scikit-learn/XGBoost
                 (thay Looker Studio)             (thay BigQuery ML)

     Điều phối toàn bộ luồng trên: Airflow tự host (Docker) — thay Cloud Composer
     DAG: logistics_dwh_pipeline (airflow/dags/logistics_pipeline_dag.py)
```

> Ánh xạ dịch vụ đầy đủ: xem Mục 4b, `TongHop_DoAn_BigData_DataWarehouse_Cloud_md.docx`.
> Điểm mấu chốt để đổi từ local sang GCP thật: chỉ sửa Airflow Connections/Variables
> (endpoint MinIO→GCS, DuckDB→BigQuery), không sửa logic nghiệp vụ.

## 2. Thiết kế IAM trên GCP (chuẩn bị sẵn cho khi deploy thật)

Nguyên tắc: **least privilege** — mỗi service account chỉ có đúng quyền cần cho vai
trò của nó, không dùng role `Owner`/`Editor` rộng cho service account nào.

| Service Account | Dùng cho | Roles đề xuất |
|---|---|---|
| `sa-ingestion` | Dataflow/Datastream nạp batch vào GCS + BigQuery staging | `roles/storage.objectAdmin` (giới hạn theo bucket, dùng IAM Condition theo prefix `raw/`), `roles/bigquery.dataEditor` (chỉ dataset `staging`) |
| `sa-dataproc` | Cluster Dataproc chạy PySpark batch + streaming | `roles/dataproc.worker`, `roles/storage.objectAdmin` (bucket data lake), `roles/bigquery.dataEditor` (dataset `curated`) |
| `sa-composer` | Cloud Composer environment (chạy DAG, gọi các service khác) | `roles/composer.worker`, `roles/dataproc.editor` (để submit job), `roles/bigquery.jobUser`, **không** cấp quyền IAM admin |
| `sa-bigquery-analytics` | dbt-bigquery + Looker Studio + BigQuery ML | `roles/bigquery.dataEditor` (dataset `marts`), `roles/bigquery.jobUser` |
| `sa-vertex-ai` | Vertex AI (gợi ý tối ưu tuyến) | `roles/aiplatform.user`, `roles/bigquery.dataViewer` (chỉ đọc mart cần thiết) |

Khuyến nghị bổ sung:
- Bật **Workload Identity** cho Composer thay vì dùng key JSON tải xuống máy.
- Tạo **IAM Condition theo bucket/prefix** thay vì cấp quyền toàn project.
- Bật **Cloud Audit Logs** cho BigQuery + Storage để theo dõi truy cập dữ liệu khách hàng
  (dataset có `Customer Email`, `Customer Zipcode`...).
- Dùng **Secret Manager** cho mọi key/credential thay vì hard-code trong DAG (điểm này
  DAG local hiện đang dùng biến môi trường Docker Compose, sẽ đổi sang Secret Manager
  khi deploy GCP).

## 3. Ước tính chi phí GCP (theo quy mô đồ án, không chạy 24/7)

Quy mô: dataset ~90-180 MB (~180k dòng), sự kiện streaming mô phỏng chạy trong các
buổi lab, cụm Dataproc/Composer chỉ bật khi cần demo/chạy DAG (không để chạy liên tục
suốt kỳ để tiết kiệm chi phí — đúng tinh thần dùng free trial credit).

| Hạng mục | Cấu hình giả định | Ước tính |
|---|---|---|
| Cloud Storage (GCS) | ~1-2 GB (raw+cleansed+curated, Parquet) | < $0.05/tháng |
| BigQuery — storage | vài trăm MB sau nén | < $0.02/tháng |
| BigQuery — query (on-demand) | vài GB quét/tháng khi test dbt + dashboard | $0 (nằm trong 1 TB free/tháng) |
| Dataproc | cluster ephemeral n1-standard-4 × 2-3 node, chỉ bật ~4-6 giờ/tuần lúc chạy job | ~$15-30/tháng |
| Managed Service for Apache Kafka | cluster nhỏ nhất, bật theo buổi lab | ~$50-100/tháng nếu để chạy liên tục — **khuyến nghị dùng Kafka tự host (đã làm) trong giai đoạn phát triển**, chỉ bật Managed Kafka thật vào tuần demo cuối kỳ (T15) |
| Cloud Composer (Airflow managed) | environment nhỏ nhất, bật theo phiên làm việc thay vì 24/7 | ~$0.35-0.85/giờ phí environment + phí compute theo mCPU/GiB — nếu chỉ bật ~3 giờ/ngày trong tuần demo: ước tính **$30-70/tháng**; nếu để chạy 24/7 cả tháng có thể lên **$250-400+/tháng** |
| Vertex AI (gợi ý tuyến, mô hình nhỏ) | inference theo lượt gọi, không train liên tục | ~$5-15/tháng |
| **Tổng ước tính** | | **~$100-250/tháng nếu bật theo phiên**, dễ dàng nằm trong gói **$300 free trial credit** của GCP cho một học kỳ nếu **luôn tắt/xoá tài nguyên sau mỗi buổi làm việc** |

> Số liệu trên tính theo đơn giá công khai của GCP tại thời điểm viết tài liệu (giá
> Cloud Composer, Dataproc, BigQuery có thể thay đổi) — dùng để có cảm nhận về độ lớn
> chi phí, **trước khi deploy thật cần chạy lại bằng GCP Pricing Calculator** với đúng
> region + cấu hình cuối cùng của nhóm.

### Khuyến nghị kiểm soát chi phí
1. **Luôn xoá Dataproc cluster** sau mỗi lần chạy job (dùng cluster ephemeral, không để
   cluster chạy thường trực) — đây là khoản dễ đội chi phí nhất nếu quên tắt.
2. Đặt **Budget Alert** trong Cloud Billing ở ngưỡng 50%/80%/100% của $300 credit.
3. Chỉ bật Cloud Composer khi cần demo/chạy DAG thật; phát triển & test DAG bằng
   Airflow local (Docker Compose, đã dựng ở Mục 1b) trong suốt các tuần T1-T9.
4. Dùng dataset mẫu nhỏ khi test BigQuery/Dataproc, tránh quét toàn bộ 180k dòng lặp
   lại nhiều lần trong lúc debug.

## 4. Theo dõi vận hành

- **Cloud Monitoring + Cloud Logging**: dashboard theo dõi DAG success rate, thời gian
  chạy từng task, lỗi Dataproc job.
- **Airflow UI** (local hoặc Composer): xem trạng thái từng task trong
  `logistics_dwh_pipeline`, log chi tiết khi task SKIP/FAIL.
- **Alert**: cấu hình email/Slack khi DAG fail quá N lần liên tiếp (đã đặt
  `retries=2`, `retry_delay=5 phút` trong DAG).

## 5. Việc đã hoàn thành / còn lại (vai trò Cường)

- [x] Docker Compose: thêm Airflow (webserver + scheduler + init) + Postgres, mount
      `airflow/dags/` và toàn bộ repo vào container.
- [x] Viết Airflow DAG `logistics_dwh_pipeline` điều phối đủ các bước: ingest batch →
      build/nạp dimension → (Spark xử lý Fact — chờ Khang) → nạp Fact → dbt (staging →
      marts) → dbt test → refresh dashboard, có retry + trigger_rule để không chặn DAG
      khi một nhánh chưa sẵn sàng dữ liệu.
- [x] Dựng khung placeholder cho PySpark job + script nạp Fact, đúng path DAG đang gọi,
      để Khang chỉ cần điền logic (không cần đụng DAG).
- [x] Thiết kế IAM (service account/role) cho khi deploy GCP thật.
- [x] Ước tính chi phí GCP theo quy mô đồ án + khuyến nghị kiểm soát chi phí.
- [x] Sơ đồ kiến trúc (target GCP + local hiện tại).
- [ ] Khi có tài khoản GCP: tạo project, bật API (Dataproc, BigQuery, Composer,
      Vertex AI), tạo các service account ở Mục 2, deploy Composer environment thật,
      trỏ DAG connections sang GCP.
