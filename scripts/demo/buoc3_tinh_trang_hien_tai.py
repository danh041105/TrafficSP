"""BƯỚC 3 — Tình trạng hiện tại của 124 con đường (bảng latest, đọc qua Trino).
Code chính: jobs/stream/speed_layer.py (latest_source, merge_sql)  +  jobs/serving/trino_views.sql (v_traffic_latest)
"""
from _demo import say, short, table, title, tq

title("BƯỚC 3: TÌNH TRẠNG HIỆN TẠI — serving.v_traffic_latest (Trino)")

clock = tq("SELECT CAST(max(event_clock) AS varchar) FROM lakehouse.serving.v_traffic_latest")[0][0]
say(f"Đồng hồ sự kiện (bản ghi mới nhất toàn mạng): {short(clock)} UTC")

say("Đếm theo trạng thái (so tốc độ hiện tại với tốc độ 'đường trống' của chính con đường đó):")
table(tq("""SELECT traffic_state, count(*) FROM lakehouse.serving.v_traffic_latest GROUP BY 1 ORDER BY 2 DESC"""),
      ["trạng thái", "số đường"])

say("5 đường đang kẹt nhất (tốc độ / free-flow thấp nhất):")
table(tq("""SELECT substr(link_name, 1, 45), borough, speed, free_flow_speed,
                   round(speed / free_flow_speed, 2), traffic_state
            FROM lakehouse.serving.v_traffic_latest
            WHERE speed IS NOT NULL AND free_flow_speed > 0 AND traffic_state <> 'NO_VALID_DATA'
            ORDER BY speed / free_flow_speed LIMIT 5"""),
      ["đường", "quận", "tốc độ", "free-flow", "tỷ lệ", "trạng thái"])

say("Ví dụ cảm biến đang gửi giá trị lỗi (NO_VALID_DATA): quan trắc mới nhất không hợp lệ")
table(tq("""SELECT substr(link_name, 1, 45), CAST(last_observation_time AS varchar), last_invalid_reason,
                   CAST(last_valid_event_time AS varchar)
            FROM lakehouse.serving.v_traffic_latest WHERE traffic_state = 'NO_VALID_DATA' LIMIT 3"""),
      ["đường", "quan trắc cuối (UTC)", "lý do", "bản hợp lệ cuối (UTC)"])
print("Bảng latest chỉ có 124 dòng (1 dòng / đường); bản đến trễ không bao giờ đè bản mới hơn.")
