import time
import random
import statistics
from kafka import KafkaConsumer

TOPIC = "perf-test"
SERVERS = ['localhost:9092', 'localhost:9093']
GROUP_ID = "group01"
RECORD_SIZE = 1024
AUTO_OFFSET = "latest"
AUTO_COMMIT = True
WINDOW_INTERVAL_MS = 5000
# Estimated/Targeted throughput (message count per millisecond)
# Set as < 0 to allow maximum throughput
TARGET_THROUGHPUT_MSGPMS = 0.200
SLEEP_STEP_MS = 1 / TARGET_THROUGHPUT_MSGPMS
# Minimum cumulative sleep deficit to trigger sleep (2 times of OS system clock tolerance/precision is well enough)
MIN_SLEEP_MS = 30


def current_milli_time():
    return int(time.perf_counter() * 1000)


consumer = KafkaConsumer(bootstrap_servers=SERVERS, group_id=GROUP_ID,
                         auto_offset_reset=AUTO_OFFSET, enable_auto_commit=AUTO_COMMIT)
consumer.subscribe(topics=[TOPIC])

# start_ms = current_milli_time()
start_window_ms = current_milli_time()
sleep_deficit_ms = 0
poll_size = 0
window_poll_times = list()
window_msg_num = 0
while True:
    # Consumer Poll
    start_record_ms = current_milli_time()
    records_all = consumer.poll()
    received_record_ms = current_milli_time()
    poll_size = 0
    for tp, messages in records_all.items():
        if tp.topic == TOPIC:
            poll_size = len(messages)
            break
    window_msg_num += poll_size
    poll_time_ms = received_record_ms - start_record_ms
    window_poll_times.append(poll_time_ms)

    # Sleep Calculation (Throughput Throttler)
    if TARGET_THROUGHPUT_MSGPMS > 0 and window_msg_num / (received_record_ms - start_window_ms) > TARGET_THROUGHPUT_MSGPMS:
        sleep_deficit_ms += SLEEP_STEP_MS * poll_size
    if TARGET_THROUGHPUT_MSGPMS > 0 and sleep_deficit_ms >= MIN_SLEEP_MS:
        start_sleep_ms = current_milli_time()
        time.sleep(sleep_deficit_ms / 1000)
        end_sleep_ms = current_milli_time()
        sleep_deficit_ms -= end_sleep_ms - start_sleep_ms
        sleep_deficit_ms = max(0, sleep_deficit_ms)

    # Interval Report
    if received_record_ms - start_window_ms > WINDOW_INTERVAL_MS:
        elapse_window_ms = received_record_ms - start_window_ms
        total_bytes = window_msg_num * RECORD_SIZE
        poll_time_mean = statistics.mean(window_poll_times)
        poll_time_max = max(window_poll_times)
        print("Received %d records (elapsed: %dms) of total size = %dB" %
              (window_msg_num, elapse_window_ms, total_bytes))
        print("\tPolling Time Per Round: %.3fms AVG, %dms MAX" %
              (poll_time_mean, poll_time_max))
        print("\tThroughput: %.3fmsg/ms, %.3fB/ms" %
              (window_msg_num / elapse_window_ms, total_bytes / elapse_window_ms))
        window_poll_times = list()
        window_msg_num = 0
        start_window_ms = received_record_ms
