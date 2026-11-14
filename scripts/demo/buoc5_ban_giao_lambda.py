"""BƯỚC 5 — Kết quả bàn giao Lambda: mốc T_batch, nhật ký lambda_control, view hợp nhất.
Code chính: jobs/batch/gold/lambda_handoff.py + jobs/common/lambda_control.py + jobs/serving/trino_views.sql
"""
from _demo import say, short, table, title, tq

title("BƯỚC 5: BÀN GIAO LAMBDA — T_batch và view hợp nhất")

st = tq("""SELECT CAST(t_batch AS varchar), speed_ahead_of_batch_min, tag_snapshot_5min = snapshot_5min,
                  last_status, CAST(last_change_at AS varchar) FROM lakehouse.serving.v_lambda_status""")[0]
say(f"T_batch hiện tại = {short(st[0])} UTC   (trước mốc: số liệu batch chính thức; từ mốc trở đi: speed tạm)")
print(f"   nhãn 'published' trỏ đúng bản đã kiểm tra: {st[2]} · trạng thái cuối: {st[3]} lúc {short(st[4])}")

say("Nhật ký 6 dòng cuối của gold.lambda_control (mỗi bước bàn giao ghi 1 dòng, không sửa/xoá):")
table(tq("""SELECT substr(handoff_id, 9), status, CAST(t_batch_prev AS varchar), CAST(t_batch AS varchar),
                   CAST(created_at AS varchar), coalesce(substr(message, 1, 40), '')
            FROM lakehouse.gold.lambda_control ORDER BY created_at DESC LIMIT 6"""),
      ["handoff", "trạng thái", "T_batch cũ", "T_batch mới", "lúc ghi (giờ thật)", "ghi chú"])

say("View hợp nhất quanh mốc T_batch: mỗi cửa sổ lấy từ ĐÚNG MỘT nguồn")
table(tq("""SELECT layer, count(*), CAST(min(window_start_utc) AS varchar), CAST(max(window_start_utc) AS varchar)
            FROM lakehouse.serving.v_traffic_5min_unified
            WHERE window_start_utc >= (SELECT t_batch FROM lakehouse.serving.v_lambda_t_batch) - INTERVAL '6' HOUR
            GROUP BY 1 ORDER BY 3"""),
      ["nguồn", "số dòng 5 phút", "từ (UTC)", "tới (UTC)"])
dup = tq("SELECT count(*) - count(DISTINCT (link_id, window_start_utc)) FROM lakehouse.serving.v_traffic_5min_unified")[0][0]
say(f"Kiểm tra trùng trên toàn bộ view hợp nhất: {dup} dòng trùng" + ("  ✅" if dup == 0 else "  ❌"))
