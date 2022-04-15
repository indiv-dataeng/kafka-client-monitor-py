# Based on
# https://github.com/apache/kafka/blob/19b585356555374755a84f1651bef9024680df70/tools/src/main/java/org/apache/kafka/tools/ProducerPerformance.java

import time
import random
import statistics
from kafka import KafkaProducer

TOPIC = "perf-test"
SERVERS = ['localhost:9092', 'localhost:9093']
RECORD_SIZE = 1024
ACK = 1
WINDOW_INTERVAL_MS = 5000
MSG_BATCH_SIZE = 5
# Estimated/Targeted throughput (message count per millisecond)
# Set as <= 0 to allow maximum throughput
TARGET_THROUGHPUT_MSGPMS = 0.500
SLEEP_STEP_MS = 1 / TARGET_THROUGHPUT_MSGPMS
# Minimum cumulative sleep deficit to trigger sleep (2 times of OS system clock tolerance/precision is well enough)
MIN_SLEEP_MS = 30


def current_milli_time():
    return int(time.perf_counter() * 1000)


def randbytes(size):
    return bytearray(random.getrandbits(8) for _ in range(size))


producer = KafkaProducer(
    bootstrap_servers=SERVERS, acks=ACK)

start_ms = current_milli_time()
start_window_ms = current_milli_time()
sleep_deficit_ms = 0
window_latencies = list()
window_msg_num = 0
while True:
    # Producer Send
    start_record_ms = current_milli_time()
    for _ in range(MSG_BATCH_SIZE):
        producer.send(TOPIC, randbytes(RECORD_SIZE))
    sent_record_ms = current_milli_time()
    latency = (sent_record_ms - start_record_ms) / MSG_BATCH_SIZE
    window_latencies.append(latency)
    window_msg_num += MSG_BATCH_SIZE

    # Sleep Calculation (Throughput Throttler)
    if TARGET_THROUGHPUT_MSGPMS > 0 and window_msg_num / (sent_record_ms - start_window_ms) > TARGET_THROUGHPUT_MSGPMS:
        sleep_deficit_ms += SLEEP_STEP_MS * MSG_BATCH_SIZE
    if TARGET_THROUGHPUT_MSGPMS > 0 and sleep_deficit_ms >= MIN_SLEEP_MS:
        start_sleep_ms = current_milli_time()
        time.sleep(sleep_deficit_ms / 1000)
        end_sleep_ms = current_milli_time()
        sleep_deficit_ms -= end_sleep_ms - start_sleep_ms
        sleep_deficit_ms = max(0, sleep_deficit_ms)

    # Interval Report
    if sent_record_ms - start_window_ms > WINDOW_INTERVAL_MS:
        elapsed_ms = sent_record_ms - start_window_ms
        total_bytes = window_msg_num * RECORD_SIZE
        latency_mean = statistics.fmean(window_latencies)
        latency_sd = statistics.stdev(window_latencies)
        print("Sent %d records (elapsed: %dms) of total size = %dB" %
              (window_msg_num, elapsed_ms, total_bytes))
        print("\tLatency: %.3fms AVG, %.3fms SD" %
              (latency_mean, latency_sd))
        print("\tThroughput: %.3fmsg/ms, %.3fB/ms" %
              (window_msg_num / elapsed_ms, total_bytes / elapsed_ms))
        window_latencies = list()
        start_window_ms = sent_record_ms
        window_msg_num = 0
