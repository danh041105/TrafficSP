# replay_producer.py: đọc data/raw (traffic parquet + weather JSON) và đẩy vào Kafka.
#   --phase test : 1 ngày 01/01/2025
#   --phase gd1  : tháng 1–9, đẩy nhanh
#   --phase gd2  : tháng 10–12, phát dần theo ĐỒNG HỒ MÔ PHỎNG event-time (--speedup)
# Chạy: docker compose run --rm producer python scripts/replay_producer.py --phase test
#
# GD2 (realtime):
#   - Trục thời gian mô phỏng là event-time UTC của dữ liệu 2025 (KHÔNG dùng giờ hệ thống 2026).
#     traffic: data_as_of (giờ New York) đổi sang UTC chỉ để xếp lịch phát; payload giữ nguyên chuỗi gốc.
#     weather: bản ghi mốc T (UTC) mô tả nhiệt độ/gió tức thời tại T và mưa/tuyết của giờ (T-1h, T],
#              nên chỉ "quan sát được" từ T  ->  phát lúc T + --weather-delay-min.
#   - Đọc Parquet theo lô (iter_batches), không nạp cả tháng vào RAM.
#   - Tuỳ chọn benchmark: --burst-*, --skew-*, --ooo-*, --dup-fraction (có seed, tái lập được).
#     Bản ghi bị chèn thêm/làm muộn được gắn Kafka header "replay-kind" để kiểm chứng, payload không đổi.
import argparse
import datetime as dt
import heapq
import json
import random
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from confluent_kafka import Producer
from common import config
from common.schemas import TRAFFIC_FIELDS, WEATHER_FIELDS, field_names
from common.time_rules import classify_local, NY

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
BATCH_ROWS = 50_000  # chia ra từng batch để tránh tràn RAM
UTC = dt.timezone.utc


def weather_in_phase(phase: str, t: str) -> bool:
    # t là chuỗi giờ UTC dạng "2025-01-01T00:00"
    if phase == "test":
        return t.startswith("2025-01-01")
    if phase == "gd1":
        return t <= config.WEATHER_PHASE_BOUNDARY_UTC
    return t > config.WEATHER_PHASE_BOUNDARY_UTC   # gd2


def read_weather(phase_name: str, run_id: str) -> list[dict]:
    # Đọc 16 file ifs_<lat>_<lon>.json, làm phẳng mảng hourly: 1 message = 1 ô × 1 giờ.
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
    # Đọc từng file 2025-MM.parquet THEO LÔ, trả ra từng message (dict) theo thứ tự data_as_of của file.
    phase = PHASES[phase_name]
    for month in phase["months"]:
        path = config.RAW_TRAFFIC_DIR / f"2025-{month:02d}.parquet"
        for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH_ROWS, columns=TRAFFIC_COLUMNS):
            table = pa.Table.from_batches([batch])
            # data_as_of: timestamp[ns] (giờ New York, không có múi giờ) -> chuỗi "2025-01-01T00:03:03".
            # Ép về giây trước, rồi cast sang chuỗi ("2025-01-01 00:03:03") và thay ' ' bằng 'T'.
            # Không dùng pc.strftime vì trên Windows nó đòi cơ sở dữ liệu múi giờ (tzdata) dù cột không có múi giờ.
            as_of = pc.replace_substring(pc.cast(pc.cast(table["data_as_of"], pa.timestamp("s")), pa.string()), " ", "T")
            table = table.set_column(table.schema.get_field_index("data_as_of"), "data_as_of", as_of)
            # Phase test: chỉ lấy 1 ngày
            if phase["traffic_day"]:
                table = table.filter(pc.starts_with(table["data_as_of"], phase["traffic_day"]))
            table = table.append_column("run_id", pa.array([run_id] * table.num_rows, pa.string()))
            yield from table.to_pylist()


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


def send(producer: Producer, topic: str, key: str, msg: dict, headers=None):
    value = json.dumps(msg, ensure_ascii=False).encode("utf-8")
    while True:
        try:
            producer.produce(topic, key=key.encode("utf-8"), value=value, on_delivery=on_delivery, headers=headers)
            break
        except BufferError:
            # Hàng đợi nội bộ đầy (producer đọc nhanh hơn Kafka nhận): chờ bớt rồi gửi lại
            producer.poll(0.5)
    producer.poll(0)   # cho phép callback on_delivery chạy


# ============================== GD2: phát theo đồng hồ mô phỏng ==============================
def traffic_pacing_utc(data_as_of: str) -> float:
    """Epoch giây (UTC) dùng để XẾP LỊCH phát. Giờ DST mơ hồ lấy lần xuất hiện đầu (fold=0) chỉ để xếp lịch;
    payload vẫn giữ data_as_of gốc và Spark sẽ gắn cờ bản ghi này (không tự gán offset)."""
    local = dt.datetime.strptime(data_as_of[:19], "%Y-%m-%dT%H:%M:%S")
    return local.replace(tzinfo=NY, fold=0).timestamp()


def weather_pacing_utc(t: str, delay_min: float) -> float:
    return dt.datetime.strptime(t, "%Y-%m-%dT%H:%M").replace(tzinfo=UTC).timestamp() + delay_min * 60


def parse_utc_arg(s: str | None) -> float | None:
    if not s:
        return None
    return dt.datetime.fromisoformat(s.replace("Z", "")).replace(tzinfo=UTC).timestamp()


class Injector:
    """Sinh sự kiện phụ cho benchmark: trùng lặp, đến muộn (out-of-order), skew vào vài link nóng."""

    def __init__(self, args):
        self.rng = random.Random(args.seed)
        self.dup_fraction, self.dup_delay = args.dup_fraction, args.dup_max_delay_sec
        self.ooo_fraction, self.ooo_delay = args.ooo_fraction, args.ooo_max_delay_sec
        self.hot_links = set(args.skew_links.split(",")) if args.skew_links else set()
        self.hot_copies = args.skew_copies
        self.counts = {"dup": 0, "ooo": 0, "skew": 0}

    def expand(self, release: float, msg: dict):
        """Trả về các (release, kind) cho 1 bản ghi traffic. kind=None là bản gốc."""
        out = []
        if self.ooo_fraction and self.rng.random() < self.ooo_fraction:
            out.append((release + self.rng.uniform(1, self.ooo_delay), "ooo")); self.counts["ooo"] += 1
        else:
            out.append((release, None))
        if self.dup_fraction and self.rng.random() < self.dup_fraction:
            out.append((release + self.rng.uniform(0, self.dup_delay), "dup")); self.counts["dup"] += 1
        if msg["link_id"] in self.hot_links:
            for _ in range(self.hot_copies):
                out.append((release + self.rng.uniform(0, 5), "skew")); self.counts["skew"] += 1
        return out


def realtime_schedule(args, run_id: str, inj: Injector):
    """Sinh (release_epoch, topic, key, msg, kind) theo thứ tự release, trộn traffic + weather bằng heap.
    Traffic đọc tuần tự theo lô; chỉ giữ trong heap các bản ghi đang chờ (đến muộn/trùng) -> RAM nhỏ."""
    start, end = parse_utc_arg(args.start), parse_utc_arg(args.end)
    heap, seq = [], 0
    for w in read_weather("gd2", run_id):
        r = weather_pacing_utc(w["time"], args.weather_delay_min)
        if (start is None or r >= start) and (end is None or r < end):
            heapq.heappush(heap, (r, seq, config.TOPIC_WEATHER, w["cell_id"], w, None)); seq += 1
    n_seen = 0
    for msg in iter_traffic("gd2", run_id):
        base = traffic_pacing_utc(msg["data_as_of"])
        if start is not None and base < start:
            continue
        if end is not None and base >= end:
            break
        n_seen += 1
        if args.max_events and n_seen > args.max_events:
            break
        for r, kind in inj.expand(base, msg):
            heapq.heappush(heap, (r, seq, config.TOPIC_TRAFFIC, msg["link_id"], msg, kind)); seq += 1
        # Mọi bản ghi có release <= base đã chắc chắn đúng thứ tự (traffic đọc theo thời gian tăng dần)
        while heap and heap[0][0] <= base:
            yield heapq.heappop(heap)
    while heap:
        yield heapq.heappop(heap)


class SimClock:
    """Đồng hồ mô phỏng: sim_time chạy nhanh gấp `speed` lần đồng hồ thực; có thể có đợt burst."""

    def __init__(self, sim_start: float, args):
        self.sim, self.wall = sim_start, time.monotonic()
        self.wall0 = self.wall
        self.base, self.burst = args.speedup, args.burst_speedup
        self.every, self.length = args.burst_every_sec, args.burst_len_sec

    def speed(self) -> float:
        if self.every and self.burst and ((time.monotonic() - self.wall0) % self.every) < self.length:
            return self.burst
        return self.base

    def tick(self):
        now = time.monotonic()
        self.sim += (now - self.wall) * self.speed()
        self.wall = now

    def wait_until(self, release: float):
        if self.base <= 0:          # --speedup 0: không giới hạn tốc độ (đo thông lượng tối đa)
            self.sim = max(self.sim, release)
            return
        self.tick()
        while self.sim < release:
            time.sleep(min(0.05, (release - self.sim) / self.speed()))
            self.tick()


def run_realtime(args, run_id: str):
    inj = Injector(args)
    producer = None if args.count_only else make_producer()
    counts = {"traffic": 0, "weather": 0, "dst_ambiguous": 0, "by_kind": {}}
    first = last = None
    clock = None
    t0 = time.time(); last_log = t0; last_n = 0
    for release, _, topic, key, msg, kind in realtime_schedule(args, run_id, inj):
        if clock is None:
            clock = SimClock(release, args)
        if producer:
            clock.wait_until(release)
            headers = [("replay-kind", kind.encode())] if kind else None
            send(producer, topic, key, msg, headers)
        if topic == config.TOPIC_TRAFFIC:
            counts["traffic"] += 1
            if kind in (None, "ooo") and classify_local(dt.datetime.strptime(msg["data_as_of"][:19], "%Y-%m-%dT%H:%M:%S")) != "ok":
                counts["dst_ambiguous"] += 1
        else:
            counts["weather"] += 1
        counts["by_kind"][kind or "original"] = counts["by_kind"].get(kind or "original", 0) + 1
        first = first or release
        last = release
        now = time.time()
        if now - last_log >= args.log_every_sec:
            n = counts["traffic"] + counts["weather"]
            print(f"[gd2] sim={dt.datetime.fromtimestamp(release, UTC):%Y-%m-%d %H:%M}Z sent={n:,} "
                  f"rate={(n - last_n) / (now - last_log):,.0f} msg/s ok={stats['ok']:,} err={stats['err']} "
                  f"kinds={counts['by_kind']}", flush=True)
            last_log, last_n = now, n
    remaining = producer.flush(120) if producer else 0
    elapsed = time.time() - t0
    total = counts["traffic"] + counts["weather"]
    summary = {"run_id": run_id, "phase": "gd2", "count_only": args.count_only, "speedup": args.speedup,
               "sim_first_utc": dt.datetime.fromtimestamp(first, UTC).isoformat() if first else None,
               "sim_last_utc": dt.datetime.fromtimestamp(last, UTC).isoformat() if last else None,
               "traffic_messages": counts["traffic"], "weather_messages": counts["weather"],
               "original_dst_ambiguous": counts["dst_ambiguous"], "by_kind": counts["by_kind"],
               "injected": inj.counts, "delivered_ok": stats["ok"], "delivery_errors": stats["err"],
               "not_flushed": remaining, "elapsed_sec": round(elapsed, 2),
               "avg_msg_per_sec": round(total / elapsed, 1) if elapsed else None,
               "args": {k: v for k, v in vars(args).items()}}
    print("[gd2] summary " + json.dumps(summary, ensure_ascii=False))
    if args.summary_file:
        Path(args.summary_file).parent.mkdir(parents=True, exist_ok=True)
        Path(args.summary_file).write_text(json.dumps(summary, indent=1, ensure_ascii=False), encoding="utf-8")
    if producer and (stats["err"] or remaining or stats["ok"] != total):
        sys.exit(1)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Phát dữ liệu raw vào Kafka")
    parser.add_argument("--phase", required=True, choices=list(PHASES))
    parser.add_argument("--run-id", help="mặc định = tên phase; đặt khác (vd gd2-r2) để kiểm tra replay/duplicate")
    parser.add_argument("--months", help="chỉ phát các tháng này của phase, vd '9' hoặc '10,11'")
    parser.add_argument("--speedup", type=float, default=288,
                        help="gd2: 288 = 5 phút dữ liệu phát trong ~1 giây; 0 = không giới hạn tốc độ")
    # count-only: chỉ thực hiện đọc và đếm message chứ không gửi dữ liệu vào kafka
    parser.add_argument("--count-only", action="store_true", help="chỉ đọc và đếm message, không gửi Kafka")
    parser.add_argument("--start", help="gd2: mốc event-time UTC bắt đầu, vd 2025-10-01T04:00")
    parser.add_argument("--end", help="gd2: mốc event-time UTC kết thúc (không gồm)")
    parser.add_argument("--max-events", type=int, default=0, help="gd2: dừng sau N bản ghi traffic gốc")
    parser.add_argument("--weather-delay-min", type=float, default=0, help="gd2: weather mốc T phát lúc T + delay")
    parser.add_argument("--burst-every-sec", type=float, default=0, help="gd2: chu kỳ burst (giây thực)")
    parser.add_argument("--burst-len-sec", type=float, default=0, help="gd2: độ dài mỗi đợt burst")
    parser.add_argument("--burst-speedup", type=float, default=0, help="gd2: speedup trong đợt burst")
    parser.add_argument("--skew-links", default="", help="gd2: danh sách link_id nóng, phân cách bởi dấu phẩy")
    parser.add_argument("--skew-copies", type=int, default=0, help="gd2: số bản sao thêm cho mỗi bản ghi của link nóng")
    parser.add_argument("--ooo-fraction", type=float, default=0, help="gd2: tỷ lệ bản ghi bị phát muộn")
    parser.add_argument("--ooo-max-delay-sec", type=float, default=900, help="gd2: độ trễ tối đa (giây event-time)")
    parser.add_argument("--dup-fraction", type=float, default=0, help="gd2: tỷ lệ bản ghi bị phát trùng")
    parser.add_argument("--dup-max-delay-sec", type=float, default=300)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--log-every-sec", type=float, default=10)
    parser.add_argument("--summary-file", help="ghi tóm tắt JSON (thư mục phải ghi được)")
    return parser.parse_args(argv)


def main():
    args = parse_args()
    phase = PHASES[args.phase]
    run_id = args.run_id or args.phase  # nhãn run_id mặc định trùng tên phase: test / gd1 / gd2
    if args.months:                     # chỉ phát một phần các tháng của phase (vd gd1 --months 9)
        allowed = set(phase["months"])
        phase["months"] = [m for m in (int(x) for x in args.months.split(",")) if m in allowed]

    if phase["realtime"]:
        run_realtime(args, run_id)
        return

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
