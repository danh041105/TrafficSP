# replay_producer.py: entry point phát dữ liệu raw vào Kafka.
#   --phase test : 1 ngày 01/01/2025
#   --phase gd1  : tháng 1–9, đẩy nhanh
#   --phase gd2  : tháng 10–12, streaming theo đồng hồ mô phỏng (xem realtime_replay.py)
# Chạy: docker compose run --rm producer python scripts/replay_producer.py --phase test
import argparse
import sys
import time
from common import config

if __package__:
    from .kafka_producer import KafkaProducer
    from .replay_source import PHASES, iter_traffic, read_weather
    from .realtime_replay import add_realtime_args, run_realtime
else:
    from kafka_producer import KafkaProducer
    from replay_source import PHASES, iter_traffic, read_weather
    from realtime_replay import add_realtime_args, run_realtime


def parse_args():
    parser = argparse.ArgumentParser(description="Phát dữ liệu raw vào Kafka")
    parser.add_argument("--phase", required=True, choices=list(PHASES))
    parser.add_argument("--run-id", help="Optional replay identifier; defaults to the phase name")
    # count-only: chỉ đọc và đếm message, không gửi dữ liệu vào Kafka
    parser.add_argument("--count-only", action="store_true", help="chỉ đọc và đếm message, không gửi Kafka")
    add_realtime_args(parser)
    return parser.parse_args()


def run_batch(args, run_id: str) -> bool:
    # test / gd1: weather trước rồi traffic, đẩy hết tốc độ
    producer = None if args.count_only else KafkaProducer()
    t0 = time.time()

    # 1) Weather trước: ít (tối đa ~105k), để khi traffic vào Bronze đã có thời tiết tương ứng
    weather = read_weather(args.phase, run_id)
    for msg in weather:
        if producer:
            producer.send(config.TOPIC_WEATHER, msg["cell_id"], msg)
    print(f"[weather] {len(weather):,} message, từ {weather[0]['time']} đến {weather[-1]['time']}")

    # 2) Traffic: đọc từng tháng, gửi dần
    n_traffic, first, last = 0, None, None
    for msg in iter_traffic(args.phase, run_id):
        if producer:
            producer.send(config.TOPIC_TRAFFIC, msg["link_id"], msg)
        if first is None:
            first = msg
        last = msg
        n_traffic += 1
        if n_traffic % 500_000 == 0:
            print(f"[traffic] đã xử lý {n_traffic:,} ({msg['data_as_of']}), {time.time() - t0:.0f}s")
    print(f"[traffic] {n_traffic:,} message, từ {first['data_as_of']} đến {last['data_as_of']}")

    if producer is None:
        print(f"[producer] --count-only: không gửi gì vào Kafka. {time.time() - t0:.0f}s")
        return True

    # 3) Chờ gửi hết hàng đợi rồi kiểm tra kết quả
    ok = producer.close(expected=len(weather) + n_traffic)
    print(f"[producer] xong sau {time.time() - t0:.0f}s")
    return ok


def main():
    args = parse_args()
    run_id = args.run_id or args.phase
    run = run_realtime if PHASES[args.phase]["realtime"] else run_batch
    if not run(args, run_id):
        sys.exit(1)   # mã thoát khác 0: báo cho người chạy (và Airflow sau này) biết là hỏng

if __name__ == "__main__":
    main()
