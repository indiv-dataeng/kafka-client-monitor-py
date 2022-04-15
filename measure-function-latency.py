import time
import random
from kafka import KafkaProducer

TOPIC = "testing"
RECORD_SIZE = 1024
ACK = 1
SERVERS = ['localhost:9092', 'localhost:9093']
SAMPLE_SIZE_TIMER = 1000000
SAMPLE_SIZE_SEND = 1000


def current_milli_time():
    return int(time.perf_counter() * 1000)


def randbytes(size):
    return bytearray(random.getrandbits(8) for _ in range(size))


start = current_milli_time()
for i in range(SAMPLE_SIZE_TIMER):
    current_milli_time()
end = current_milli_time()
latency_timer = (end - start) / SAMPLE_SIZE_TIMER

producer = KafkaProducer(
    bootstrap_servers=SERVERS, acks=ACK)
start = current_milli_time()
for i in range(SAMPLE_SIZE_SEND):
    producer.send(TOPIC, randbytes(RECORD_SIZE))
end = current_milli_time()
latency_producer = (end - start) / SAMPLE_SIZE_SEND

print("time.perf_counter Latency: %.6fms" % latency_timer)
print("Producer.send Latency: %.6fms" % latency_producer)
