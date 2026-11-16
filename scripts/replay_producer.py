# replay_producer.py: đọc data/raw (traffic parquet + weather JSON) và đẩy vào Kafka.
#   --phase test : 1 ngày 01/01/2025
#   --phase gd1  : tháng 1–9, đẩy nhanh
#   --phase gd2  : tháng 10–12, phát dần theo đồng hồ mô phỏng (--speedup)
# Chạy: docker compose run --rm producer python scripts/replay_producer.py --phase test
import argparse
import json
import sys
import time
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from confluent_kafka import Producer
from common import config
from common.schemas import TRAFFIC_FIELDS, WEATHER_FIELDS, field_names

PHASES = {
    "test": {"months": [1], "traffic_day": "2025-01-01", "realtime": False},
    "gd1": {"months": list(config.HISTORY_MONTHS), "traffic_day": None, "realtime": False},
    "gd2": {"months": list(config.REPLAY_MONTHS), "traffic_day": None, "realtime": True},
}
# Các trường không nằm trong mảng "hourly" của file JSON; producer tự điền.
WEATHER_METADATA = ("cell_id", "latitude", "longitude", "time", "run_id")
WEATHER_VARS = [f for f in field_names(WEATHER_FIELDS) if f not in WEATHER_METADATA]

# traffic 
TRAFFIC_COLUMNS = [f for f in field_names(TRAFFIC_FIELDS) if f != "run_id"]
BATCH_ROWS = 50_000 # chia ra từng batch để tránh tràn RAM

def weather_in_phase(phase: str, t: str) -> bool:
    # t là chuỗi giờ UTC dạng "2025-01-01T00:00" 
    if phase == "test":
        return t.startswith("2025-01-01")
    if phase == "gd1":
        return t <= config.WEATHER_PHASE_BOUNDARY_UTC 
    return t > config.WEATHER_PHASE_BOUNDARY_UTC   # gd2

def read_weather(phase_name: str, run_id: str) -> list[dict]:
    #Đọc 16 file ifs_<lat>_<lon>.json, làm phẳng mảng hourly: 1 message = 1 ô × 1 giờ.
    files = sorted(config.RAW_WEATHER_DIR.glob("ifs_*.json"))
    if not files:
        raise FileNotFoundError(f"Không thấy file weather nào trong {config.RAW_WEATHER_DIR}")

    messages = []
    for path in files:
        cell_id = path.stem.removeprefix("ifs_")      # "ifs_40.5272_-74.2373" -> "40.5272_-74.2373"
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        hourly = data["hourly"]

        # Các mảng trong hourly song song nhau: phần tử thứ i của mọi mảng cùng thuộc giờ hourly["time"][i]
        for i, t in enumerate(hourly["time"]):
            if not weather_in_phase(phase_name, t):
                continue
            msg = {
                "cell_id": cell_id,
                "latitude": data["latitude"],
                "longitude": data["longitude"],
                "time": t,
            }
            for var in WEATHER_VARS:
                msg[var] = hourly[var][i]
            msg["run_id"] = run_id
            messages.append(msg)

    # Sắp theo giờ rồi theo ô: GĐ2 cần phát đúng thứ tự thời gian
    messages.sort(key=lambda m: (m["time"], m["cell_id"]))
    return messages

def iter_traffic(phase_name: str, run_id: str):
    # Đọc từng file 2025-MM.parquet, trả ra từng message (dict) theo thứ tự data_as_of.
    phase = PHASES[phase_name]
    for month in phase["months"]:
        path = config.RAW_TRAFFIC_DIR / f"2025-{month:02d}.parquet"
        table = pq.read_table(path, columns=TRAFFIC_COLUMNS)

        # data_as_of: timestamp[ns] (giờ New York, không có múi giờ) -> chuỗi "2025-01-01T00:03:03".
        # Ép về giây trước, nếu không strftime in thêm ".000000000".
        as_of = pc.strftime(pc.cast(table["data_as_of"], pa.timestamp("s")),
                            format="%Y-%m-%dT%H:%M:%S")
        table = table.set_column(table.schema.get_field_index("data_as_of"), "data_as_of", as_of)

        # Phase test: chỉ lấy 1 ngày
        if phase["traffic_day"]:
            table = table.filter(pc.starts_with(table["data_as_of"], phase["traffic_day"]))

        table = table.append_column("run_id", pa.array([run_id] * table.num_rows, pa.string()))

        # File gốc đã sắp theo data_as_of, đọc theo lô vẫn giữ nguyên thứ tự
        for batch in table.to_batches(max_chunksize=BATCH_ROWS):
            yield from batch.to_pylist()

# Đếm kết quả gửi. Kafka báo về qua callback, không phải ngay lúc gọi produce().
stats = {"ok": 0, "err": 0}
def on_delivery(err, msg):
    # Kafka gọi hàm này khi một message đã được ghi (hoặc ghi thất bại).
    if err is not None:
        stats["err"] += 1
        if stats["err"] <= 5:   # chỉ in vài lỗi đầu, tránh tràn màn hình
            print(f"[lỗi] topic={msg.topic()} key={msg.key()}: {err}")
    else:
        stats["ok"] += 1


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "enable.idempotence": True,   # gửi lại khi lỗi mạng không sinh bản trùng; giữ thứ tự theo key
        "acks": "all",                # chỉ coi là xong khi broker đã ghi
        "compression.type": "lz4",    # nén theo lô, giảm dung lượng topic nhiều lần
        "linger.ms": 50,              # chờ tối đa 50 ms để gom message thành lô lớn
    })

def send(producer: Producer, topic: str, key: str, msg: dict):
    value = json.dumps(msg, ensure_ascii=False).encode("utf-8")
    while True:
        try:
            producer.produce(topic, key=key.encode("utf-8"), value=value, on_delivery=on_delivery)
            break
        except BufferError:
            # Hàng đợi nội bộ đầy (producer đọc nhanh hơn Kafka nhận): chờ bớt rồi gửi lại
            producer.poll(0.5)
    producer.poll(0)   # cho phép callback on_delivery chạy

def parse_args():
    parser = argparse.ArgumentParser(description="Phát dữ liệu raw vào Kafka")
    parser.add_argument("--phase", required=True, choices=list(PHASES))
    parser.add_argument("--speedup", type=float, default=288,
                        help="chỉ dùng cho gd2: 288 nghĩa là 5 phút dữ liệu phát trong ~ 1 giây")
    # count-only: chỉ thực hiện đọc và đếm message chứ không gửi dữ liệu vào kafka 
    parser.add_argument("--count-only", action="store_true",
                        help="chỉ đọc và đếm message, không gửi Kafka")
    return parser.parse_args()

def main():
    args = parse_args()
    phase = PHASES[args.phase]
    run_id = args.phase  # nhãn run_id trùng tên phase: test / gd1 / gd2

    if phase["realtime"] and not args.count_only:
        sys.exit("[producer] gd2 chưa viết (Bước 5). Hiện chỉ chạy được gd2 với --count-only.")

    producer = None if args.count_only else make_producer()
    t0 = time.time()

    # 1) Weather trước: ít (tối đa ~105k), để khi traffic vào Bronze đã có thời tiết tương ứng
    weather = read_weather(args.phase, run_id)
    for msg in weather:
        if producer:
            send(producer, config.TOPIC_WEATHER, msg["cell_id"], msg)
    print(f"[weather] {len(weather):,} message, từ {weather[0]['time']} đến {weather[-1]['time']}")

    # 2) Traffic: đọc từng tháng, gửi dần
    n_traffic, first, last = 0, None, None
    for msg in iter_traffic(args.phase, run_id):
        if producer:
            send(producer, config.TOPIC_TRAFFIC, msg["link_id"], msg)
        if first is None:
            first = msg
        last = msg
        n_traffic += 1
        if n_traffic % 500_000 == 0:
            print(f"[traffic] đã xử lý {n_traffic:,} ({msg['data_as_of']}), {time.time() - t0:.0f}s")
    print(f"[traffic] {n_traffic:,} message, từ {first['data_as_of']} đến {last['data_as_of']}")

    if producer is None:
        print(f"[producer] --count-only: không gửi gì vào Kafka. {time.time() - t0:.0f}s")
        return

    # 3) Chờ các message còn trong hàng đợi được gửi hết
    remaining = producer.flush(120)
    total = len(weather) + n_traffic
    print(f"[kafka] thành công {stats['ok']:,}/{total:,}, lỗi {stats['err']:,}, "
          f"còn kẹt {remaining}, {time.time() - t0:.0f}s")
    if stats["err"] or remaining or stats["ok"] != total:
        sys.exit(1)   # mã thoát khác 0: báo cho người chạy (và Airflow sau này) biết là hỏng

if __name__ == "__main__":
    main()