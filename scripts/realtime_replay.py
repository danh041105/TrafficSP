# realtime_replay.py: GĐ2, phát traffic + weather theo đồng hồ mô phỏng giờ sự kiện (--speedup).
#   - Trục thời gian là giờ sự kiện UTC của dữ liệu 2025, không dùng giờ hệ thống.
#   - traffic: data_as_of (giờ NY) đổi sang UTC chỉ để xếp lịch; payload giữ nguyên chuỗi gốc.
#   - weather: mốc T mô tả giờ (T-1h, T] nên chỉ quan sát được từ T -> phát lúc T + --weather-delay-min.
# Được gọi từ replay_producer.py --phase gd2, không chạy riêng.
import datetime as dt
import heapq
import time
from zoneinfo import ZoneInfo
from common import config
from kafka_producer import KafkaProducer
from replay_source import iter_traffic, read_weather

UTC = dt.timezone.utc
NY = ZoneInfo(config.TRAFFIC_TIMEZONE)

def add_realtime_args(parser):
    # Tham số chỉ dùng cho gd2, gom thành một nhóm riêng trong --help
    g = parser.add_argument_group("gd2")
    g.add_argument("--speedup", type=float, default=288,
                   help="288 = 5 phút dữ liệu phát trong ~1 giây; 0 = không giới hạn tốc độ")
    g.add_argument("--start", help="mốc giờ sự kiện UTC bắt đầu, vd 2025-10-01T04:00")
    g.add_argument("--end", help="mốc giờ sự kiện UTC kết thúc (không gồm)")
    g.add_argument("--weather-delay-min", type=float, default=0, help="weather mốc T phát lúc T + delay")
    g.add_argument("--log-every-sec", type=float, default=10)


def parse_utc(s: str | None) -> float | None:
    return dt.datetime.fromisoformat(s).replace(tzinfo=UTC).timestamp() if s else None

def fmt_utc(epoch: float) -> str:
    return dt.datetime.fromtimestamp(epoch, UTC).strftime("%Y-%m-%d %H:%M")

def traffic_release(data_as_of: str) -> float:
    # Giờ DST lặp lại lấy lần đầu (fold=0) chỉ để xếp lịch; Silver vẫn đưa các giờ này vào quarantine
    local = dt.datetime.strptime(data_as_of[:19], "%Y-%m-%dT%H:%M:%S")
    return local.replace(tzinfo=NY, fold=0).timestamp()

def weather_release(t: str, delay_min: float) -> float:
    return dt.datetime.strptime(t, "%Y-%m-%dT%H:%M").replace(tzinfo=UTC).timestamp() + delay_min * 60

def schedule(args, run_id: str):
    # Sinh (release, seq, topic, key, msg) theo thứ tự release.
    # Weather ít nên nạp hết vào heap; traffic đọc dần theo thứ tự thời gian của file.
    start, end = parse_utc(args.start), parse_utc(args.end)
    heap, seq = [], 0   # seq phá hòa khi release bằng nhau, tránh Python so sánh hai dict
    for w in read_weather("gd2", run_id):
        r = weather_release(w["time"], args.weather_delay_min)
        if (start is None or r >= start) and (end is None or r < end):
            heapq.heappush(heap, (r, seq, config.TOPIC_WEATHER, w["cell_id"], w)); seq += 1
    for msg in iter_traffic("gd2", run_id):
        r = traffic_release(msg["data_as_of"])
        if start is not None and r < start:
            continue
        if end is not None and r >= end:
            break
        heapq.heappush(heap, (r, seq, config.TOPIC_TRAFFIC, msg["link_id"], msg)); seq += 1
        # Traffic đọc theo thời gian tăng dần nên mọi thứ có release <= r đã chắc chắn đúng thứ tự
        while heap and heap[0][0] <= r:
            yield heapq.heappop(heap)
    while heap:
        yield heapq.heappop(heap)

class SimClock:
    # Đồng hồ mô phỏng: giờ dữ liệu chạy nhanh gấp `speedup` lần giờ thật; speedup <= 0 thì không chờ
    def __init__(self, sim_start: float, speedup: float):
        self.sim0, self.wall0, self.speedup = sim_start, time.monotonic(), speedup

    def wait_until(self, release: float):
        if self.speedup <= 0:
            return
        wait = (release - self.sim0) / self.speedup - (time.monotonic() - self.wall0)
        if wait > 0:
            time.sleep(wait)

def run_realtime(args, run_id: str) -> bool:
    producer = None if args.count_only else KafkaProducer()
    counts = {config.TOPIC_WEATHER: 0, config.TOPIC_TRAFFIC: 0}
    clock = first = last = None
    t0 = last_log = time.time()
    for release, _, topic, key, msg in schedule(args, run_id):
        if clock is None:
            clock, first = SimClock(release, args.speedup), release
        if producer:
            clock.wait_until(release)
            producer.send(topic, key, msg)
        counts[topic] += 1
        last = release
        if time.time() - last_log >= args.log_every_sec:
            print(f"[gd2] giờ dữ liệu {fmt_utc(release)} UTC, traffic {counts[config.TOPIC_TRAFFIC]:,}, "
                  f"weather {counts[config.TOPIC_WEATHER]:,}, {time.time() - t0:.0f}s", flush=True)
            last_log = time.time()

    total = sum(counts.values())
    if total == 0:
        print("[gd2] không có message nào trong khoảng --start/--end")
        return True
    print(f"[gd2] {total:,} message (traffic {counts[config.TOPIC_TRAFFIC]:,}, "
          f"weather {counts[config.TOPIC_WEATHER]:,}), từ {fmt_utc(first)} đến {fmt_utc(last)} UTC, "
          f"{time.time() - t0:.0f}s")
    if producer is None:
        return True
    return producer.close(expected=total)