# Lambda handoff trên Iceberg + Trino

Tài liệu mô tả cơ chế chuyển các cửa sổ 5 phút đã hoàn tất từ Speed Layer sang Batch Layer. Toàn bộ chạy trên **Iceberg + Trino**, không có ClickHouse.

- Code: `jobs/batch/gold/lambda_handoff.py`, `jobs/common/lambda_control.py`
- View: `jobs/serving/trino_views.sql`
- Test: `jobs/tests/test_lambda_control.py`

## 1. Quy tắc hợp nhất

```
v_traffic_5min_unified =
    gold.traffic_5min_batch FOR VERSION AS OF 'published'   WHERE window_start_utc <  T_batch
  ∪ speed.traffic_5min_rt                                   WHERE window_start_utc >= T_batch
```

- **T_batch** là `t_batch` lớn nhất trong các dòng `PUBLISHED` của `gold.lambda_control` (bằng 1970-01-01 nếu chưa công bố lần nào). Giá trị luôn nằm trên ranh giới 5 phút.
- Hai điều kiện `<` và `>=` trên cùng một giá trị nên mỗi `(link_id, window_start_utc)` chỉ có thể đến từ **một** nguồn: không chồng lấn, không hở.
- Batch View được đọc qua **tag `published`**, không đọc nhánh `main`. Dữ liệu batch vừa ghi nhưng chưa kiểm chứng không bao giờ lọt ra Trino.
- Batch và Speed ghi vào hai namespace khác nhau (`gold.*` và `speed.*`), không bao giờ ghi chung một bảng hay một phân vùng.

## 2. Vì sao cần giao thức nhiều bước

Iceberg chỉ bảo đảm nguyên tử **trong một bảng**: mỗi commit là một snapshot. Một handoff phải đụng tới 3 bảng (`traffic_5min_batch`, `traffic_hourly`, `lambda_control`) nên **không thể** là một giao dịch.

Thiết kế dưới đây bảo đảm: tại **mọi** thời điểm giữa các bước, những gì Trino đọc được vẫn nhất quán và đúng.

| Bước | Hành động | Trino thấy gì nếu dừng ngay sau bước này |
|---|---|---|
| 0 | `pg_try_advisory_lock` trên Postgres của catalog. Có handoff khác đang chạy thì thoát (mã 2). Phục hồi handoff dang dở (mục 4). | Như trước |
| 1 | Ghi `PREPARING` vào `lambda_control` (append) | Như trước |
| 2 | Recompute Batch View các ngày UTC `[ngày bắt đầu, ngày(T_new − 1s)]`. Mỗi bảng ghi bằng **một** commit `overwrite(<khoảng ngày>)`, tạo snapshot S5 và SH trên nhánh `main`. | Như trước, vì tag `published` chưa đổi |
| 3 | Kiểm tra **trên chính S5/SH** (`VERSION AS OF`) cho toàn bộ khoảng đã recompute `[lo, T_new)`: đủ so với Silver, không trùng grain, 1 giờ = tổng 5 phút, phủ mọi cửa sổ realtime. Không đạt thì ghi `FAILED`, thoát 1. | Như trước |
| 4 | Ghi `VALIDATED` kèm S5, SH và kết quả kiểm tra | Như trước |
| 5 | `REPLACE TAG published AS OF VERSION S5` (rồi tới SH) | Batch mới **đã kiểm chứng** cho `window < T_old`; T_batch chưa đổi |
| 6 | Kiểm tra lại T_batch (không giảm), rồi ghi `PUBLISHED` với `t_batch = T_new` | Batch cho `[T_old, T_new)`, speed từ `T_new` |

Ở bước 5, dữ liệu mới cho các cửa sổ cũ hơn T_old có thể khác bản cũ, chẳng hạn khi đã bổ sung bản ghi đến muộn. Đó là bản sửa đã qua kiểm tra, không phải trạng thái lỗi.

## 3. Bất biến

- **T_batch không giảm.** `validate_move` từ chối `T_new < T_old`. Muốn giữ nguyên T_batch thì phải bật `--correction`. Bước 6 kiểm tra lại một lần nữa ngay trước khi ghi.
- **Không công bố trước khi Batch commit và kiểm chứng xong.** Tag chỉ được dời sau `VALIDATED`. `PUBLISHED` chỉ ghi sau khi tag đã dời.
- **Không chạy đồng thời.** PostgreSQL advisory lock gắn với kết nối, nên tiến trình chết thì khoá tự nhả. Airflow đặt thêm `max_active_runs=1`.
- **Mốc an toàn:** `T_new ≤ floor5(max event_time của Silver − watermark)`. Với `--until auto`, T_new bằng chính mốc này.

## 4. Phục hồi sau crash

Khi chạy, bước 0 tìm các handoff có trạng thái cuối chưa phải `PUBLISHED`/`FAILED` (code: `lambda_control.recovery_action`):

| Trạng thái dang dở | Điều kiện | Xử lý |
|---|---|---|
| `PREPARING` | — | `FAILED`. Batch có thể đã ghi vào `main` nhưng tag chưa dời nên Trino không thấy. |
| `VALIDATED` | tag 5 phút = S5 **và** tag 1 giờ = SH **và** `t_batch` > T_batch hiện hành | `PUBLISHED`: hoàn tất bước 6 |
| `VALIDATED` | còn lại | `FAILED` |

Kết quả kiểm thử thật (2026-11-13), đọc lại qua Trino:

| Crash ở | Sau crash | Lần chạy sau |
|---|---|---|
| `--fail-at after_validate` | T_batch giữ nguyên (1970), tag không đổi, unified chỉ lấy speed | Handoff cũ thành `FAILED`; chạy mới tới `PUBLISHED` T_batch = 2025-12-02 08:00 |
| `--fail-at after_tag` | T_batch vẫn là 08:00; batch cho `< 08:00` đã là snapshot mới (đã kiểm chứng); không trùng, không hở | Handoff được hoàn tất thành `PUBLISHED` 09:00 (`recovered from VALIDATED after crash -> PUBLISHED`) |
| 2 handoff chạy cùng lúc | Một bản `PUBLISHED` 10:00; bản kia thoát với mã 2 ("một handoff khác đang chạy") | — |
| `--until` nhỏ hơn T_batch | Từ chối: "T_batch không được giảm", mã 1 | — |

## 5. Sửa dữ liệu muộn và backfill

- Dữ liệu đến muộn hơn watermark của Speed Layer: realtime bỏ qua (có đếm số dòng bị loại). Handoff kế tiếp recompute từ Silver và bù lại.
  - Kiểm chứng: kịch bản b5 có 240 cửa sổ realtime bị thiếu; sau handoff, view hợp nhất khớp Parquet gốc 435.093/435.093 cửa sổ.
- Dữ liệu muộn cho ngày **đã chốt**:
  1. chạy `bronze_to_silver` cho ngày đó;
  2. chạy `lambda_handoff.py --correction --from-day <ngày UTC> --until <T_batch hiện hành>`.

  Handoff này công bố lại với cùng T_batch, sau khi đã kiểm tra lại.

## 6. Lệnh

```bash
SUBMIT="docker exec traffic-spark-master /opt/spark/bin/spark-submit"
$SUBMIT /opt/spark/jobs/batch/gold/lambda_handoff.py --until auto
$SUBMIT /opt/spark/jobs/batch/gold/lambda_handoff.py --until 2025-12-02T08:00:00 --fail-at after_validate   # thử crash
docker exec traffic-trino trino --execute "SELECT * FROM lakehouse.serving.v_lambda_status"
docker exec traffic-trino trino --execute "SELECT handoff_id, status, t_batch, message FROM lakehouse.gold.lambda_control ORDER BY created_at"
```

Trong Airflow, task `lambda_handoff` chạy với `--no-recompute`: dùng đúng snapshot mà `batch_5min`/`batch_hourly` vừa ghi và `dq_checks` vừa kiểm, rồi tự kiểm tra lại trước khi công bố.
