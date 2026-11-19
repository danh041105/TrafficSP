import json
from confluent_kafka import Producer
from common import config

class KafkaProducer:
    def __init__(self):
        self.n_delivered = 0   # số message Kafka xác nhận đã ghi thành công
        self.n_failed = 0      # số message Kafka báo ghi thất bại
        self.producer = Producer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "enable.idempotence": True,   # gửi lại khi lỗi mạng không sinh bản trùng; giữ thứ tự theo key
            "acks": "all",                # chỉ coi là xong khi broker đã ghi
            "compression.type": "lz4",    # nén theo lô, giảm dung lượng topic nhiều lần
            "linger.ms": 50,              # chờ tối đa 50 ms để gom message thành lô lớn
        })

    def _on_delivery(self, err, msg):
        # Kafka gọi hàm này khi một message đã được ghi (hoặc ghi thất bại).
        if err is not None:
            self.n_failed += 1
            if self.n_failed <= 5:   # chỉ in vài lỗi đầu
                print(f"[lỗi] topic={msg.topic()} key={msg.key()}: {err}")
        else:
            self.n_delivered += 1

    def send(self, topic: str, key: str, msg: dict, headers: list | None = None):
        # headers: [(tên, bytes)], vd benchmark gắn ("replay-kind", b"dup"); payload không đổi
        value = json.dumps(msg, ensure_ascii=False).encode("utf-8")
        while True:
            try:
                self.producer.produce(topic, key=key.encode("utf-8"), value=value,
                                      headers=headers, on_delivery=self._on_delivery)
                break
            except BufferError:
                # Hàng đợi đầy (producer đọc nhanh hơn Kafka nhận): chờ khoảng 0.5s  rồi gửi lại
                self.producer.poll(0.5)
        self.producer.poll(0) 

    def close(self, expected: int, timeout: float = 120) -> bool:
        # Chờ gửi hết hàng đợi; trả về True nếu mọi message đều thành công
        remaining = self.producer.flush(timeout)
        print(f"[kafka] thành công {self.n_delivered:,}/{expected:,}, lỗi {self.n_failed:,}, còn kẹt {remaining}")
        return not (self.n_failed or remaining or self.n_delivered != expected)