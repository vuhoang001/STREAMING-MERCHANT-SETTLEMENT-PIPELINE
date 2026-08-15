# Bọc confluent-kafka: connect, send(key, value), flush
import json 
import sys 

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"


def _on_delivery(err, msg): 
    if err is not None: 
        print(f"Giao hàng thất bại: {err}", file=sys.stderr)

        
class kafkaEventProducer: 
    def __init__(self, bootstrap_servers:str = BOOTSTRAP_SERVERS):
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})

    def send(self, topic: str, key: str, event: dict) -> None: 
        payload = json.dumps(event).encode("utf-8")

        try: 
            self._producer.produce(topic, key=key.encode('utf-8'),value=payload, callback=_on_delivery)
        except BufferError:
            # Buffer đầy: poll để giải phóng rồi thử lại. Gtocha kinh điển của loop dài 

            self._producer.poll(0.5)
            self._producer.produce(topic, key=key.encode('utf-8'),value=payload, callback=_on_delivery)
            self._producer.poll(0)

    def flush(self, timeout: float = 10) -> int: 
        remaining = self._producer.flush(timeout)
        if remaining > 0: 
            print(f"Còn {remaining} mesage chưa được gửi.")
        return remaining

