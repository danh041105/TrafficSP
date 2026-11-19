# replay_benchmark.py: phát dữ liệu gd2 giống realtime_replay.py nhưng chèn thêm "nhiễu" để đo speed layer.
#   - Injector: phát trùng (--dup-*), phát muộn / sai thứ tự (--ooo-*), dồn tải vào vài link nóng (--skew-*).
#     Bản ghi bị chèn thêm hoặc làm muộn gắn Kafka header "replay-kind" (dup / ooo / skew), payload giữ nguyên.
#   - Burst: định kỳ tăng tốc độ phát trong một khoảng giờ thật (--burst-*).
#   - Cuối lần chạy in tóm tắt JSON; ghi ra file nếu có --summary-file.
# Mọi ngẫu nhiên đều theo --seed nên chạy lại cùng tham số ra cùng kịch bản.
#
# Chạy: docker compose run --rm producer python scripts/benchmark/replay_benchmark.py \
#           --start 2025-10-01T04:00 --end 2025-10-01T06:00 --speedup 2880 --dup-fraction 0.01
# Lưu ý: gửi vào đúng topic thật (traffic_raw, weather_raw) nên dữ liệu benchmark cũng sẽ vào Bronze.
#        Dùng --run-id (mặc định "bench") để lọc riêng các dòng này.
import argparse
import heapq
import json
import random
import sys
import time
from pathlib import Path

# Chạy bằng đường dẫn file thì Python chỉ thêm scripts/benchmark/ vào sys.path;
# thêm scripts/ để import được các module của producer.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common import config
from kafka_producer import KafkaProducer
from realtime_replay import add_realtime_args, fmt_utc, schedule


class Injector:
    # Với mỗi bản traffic gốc, trả về list (release, kind); kind None là bản gốc phát đúng giờ.
    def __init__(self, args):
        self.args = args
        self.rng = random.Random(args.seed)
        self.hot_links = {x for x in args.skew_links.split(",") if x}
        self.counts = {"dup": 0, "ooo": 0, "skew": 0}

    def expand(self, release: float, msg: dict) -> list:
        a = self.args
        out = []
        # Phát muộn: chính bản gốc bị dời ra sau 1..ooo_max_delay_sec giây giờ dữ liệu
        if a.ooo_fraction and self.rng.random() < a.ooo_fraction:
            out.append((release + self.rng.uniform(1, a.ooo_max_delay_sec), "ooo"))
            self.counts["ooo"] += 1
        else:
            out.append((release, None))
        # Phát trùng: thêm một bản y hệt, đến sau bản gốc 0..dup_max_delay_sec giây
        if a.dup_fraction and self.rng.random() < a.dup_fraction:
            out.append((release + self.rng.uniform(0, a.dup_max_delay_sec), "dup"))
            self.counts["dup"] += 1
        # Skew: link nóng nhận thêm skew_copies bản trong vòng 5 giây
        if msg["link_id"] in self.hot_links:
            for _ in range(a.skew_copies):
                out.append((release + self.rng.uniform(0, 5), "skew"))
            self.counts["skew"] += a.skew_copies
        return out


def inject(events, inj: Injector, max_events: int):
    # Chèn bản phụ vào dòng sự kiện đã sắp của realtime_replay.schedule mà vẫn giữ thứ tự release.
    # Bản phụ luôn có release >= bản gốc, nên phần tử nào có release <= r hiện tại là đã chắc chắn đúng thứ tự.
    heap, seq, n_traffic = [], 0, 0
    for r, _, topic, key, msg in events:
        if topic == config.TOPIC_TRAFFIC:
            n_traffic += 1
            if max_events and n_traffic > max_events:
                break
            copies = inj.expand(r, msg)
        else:
            copies = [(r, None)]
        for release, kind in copies:
            heapq.heappush(heap, (release, seq, topic, key, msg, kind))
            seq += 1
        while heap and heap[0][0] <= r:
            yield heapq.heappop(heap)
    while heap:
        yield heapq.heappop(heap)


class BurstClock:
    # Đồng hồ mô phỏng có burst: cứ mỗi burst_every_sec giây thật thì có burst_len_sec giây chạy với burst_speedup.
    # Tốc độ đổi theo thời gian nên phải cộng dồn giờ mô phỏng theo từng nhịp nhỏ (khác SimClock của gd2).
    def __init__(self, sim_start: float, args):
        self.args = args
        self.sim, self.wall = sim_start, time.monotonic()
        self.wall0 = self.wall

    def speed(self) -> float:
        a = self.args
        in_burst = (a.burst_every_sec and a.burst_speedup
                    and (time.monotonic() - self.wall0) % a.burst_every_sec < a.burst_len_sec)
        return a.burst_speedup if in_burst else a.speedup

    def tick(self):
        now = time.monotonic()
        self.sim += (now - self.wall) * self.speed()
        self.wall = now

    def wait_until(self, release: float):
        if self.args.speedup <= 0:   # --speedup 0: không giới hạn tốc độ, đo thông lượng tối đa
            return
        self.tick()
        while self.sim < release:
            time.sleep(min(0.05, (release - self.sim) / self.speed()))
            self.tick()


def parse_args():
    p = argparse.ArgumentParser(description="Phát gd2 có chèn nhiễu để benchmark speed layer")
    p.add_argument("--run-id", default="bench", help="nhãn run_id trong message, để lọc riêng dữ liệu benchmark")
    p.add_argument("--count-only", action="store_true", help="chỉ dựng kịch bản và đếm, không gửi Kafka")
    p.add_argument("--max-events", type=int, default=0, help="dừng sau N bản ghi traffic gốc (0 = không giới hạn)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--summary-file", help="ghi tóm tắt JSON; trong container scripts/ và data/ là read-only, dùng /tmp")
    add_realtime_args(p)   # --speedup, --start, --end, --weather-delay-min, --log-every-sec

    g = p.add_argument_group("nhiễu")
    g.add_argument("--dup-fraction", type=float, default=0, help="tỷ lệ bản ghi traffic bị phát trùng")
    g.add_argument("--dup-max-delay-sec", type=float, default=300, help="bản trùng đến muộn tối đa (giây giờ dữ liệu)")
    g.add_argument("--ooo-fraction", type=float, default=0, help="tỷ lệ bản ghi traffic bị phát muộn")
    g.add_argument("--ooo-max-delay-sec", type=float, default=900, help="độ trễ tối đa (giây giờ dữ liệu)")
    g.add_argument("--skew-links", default="", help="danh sách link_id nóng, phân cách bởi dấu phẩy")
    g.add_argument("--skew-copies", type=int, default=0, help="số bản sao thêm cho mỗi bản ghi của link nóng")
    g.add_argument("--burst-every-sec", type=float, default=0, help="chu kỳ burst (giây thật)")
    g.add_argument("--burst-len-sec", type=float, default=0, help="độ dài mỗi đợt burst (giây thật)")
    g.add_argument("--burst-speedup", type=float, default=0, help="speedup trong đợt burst")
    return p.parse_args()


def main():
    args = parse_args()
    inj = Injector(args)
    producer = None if args.count_only else KafkaProducer()
    by_topic = {config.TOPIC_WEATHER: 0, config.TOPIC_TRAFFIC: 0}
    by_kind = {}
    clock = first = last = None
    t0 = last_log = time.time()
    n_at_last_log = 0

    for release, _, topic, key, msg, kind in inject(schedule(args, args.run_id), inj, args.max_events):
        if clock is None:
            clock, first = BurstClock(release, args), release
        if producer:
            clock.wait_until(release)
            headers = [("replay-kind", kind.encode("utf-8"))] if kind else None
            producer.send(topic, key, msg, headers)
        by_topic[topic] += 1
        by_kind[kind or "original"] = by_kind.get(kind or "original", 0) + 1
        last = release

        now = time.time()
        if now - last_log >= args.log_every_sec:
            n = sum(by_topic.values())
            print(f"[bench] giờ dữ liệu {fmt_utc(release)} UTC, đã phát {n:,}, "
                  f"{(n - n_at_last_log) / (now - last_log):,.0f} msg/s, loại {by_kind}", flush=True)
            last_log, n_at_last_log = now, n

    total = sum(by_topic.values())
    ok = producer.close(expected=total) if producer else True
    elapsed = time.time() - t0
    summary = {
        "run_id": args.run_id,
        "count_only": args.count_only,
        "speedup": args.speedup,
        "sim_first_utc": fmt_utc(first) if first else None,
        "sim_last_utc": fmt_utc(last) if last else None,
        "traffic_messages": by_topic[config.TOPIC_TRAFFIC],
        "weather_messages": by_topic[config.TOPIC_WEATHER],
        "by_kind": by_kind,
        "injected": inj.counts,
        "delivered": producer.n_delivered if producer else None,
        "failed": producer.n_failed if producer else None,
        "elapsed_sec": round(elapsed, 2),
        "avg_msg_per_sec": round(total / elapsed, 1) if elapsed else None,
        "args": vars(args),
    }
    print("[bench] tóm tắt " + json.dumps(summary, ensure_ascii=False))
    if args.summary_file:
        path = Path(args.summary_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(summary, indent=1, ensure_ascii=False), encoding="utf-8")
    if not ok:
        sys.exit(1)   # mã thoát khác 0: có message gửi lỗi hoặc còn kẹt


if __name__ == "__main__":
    main()
