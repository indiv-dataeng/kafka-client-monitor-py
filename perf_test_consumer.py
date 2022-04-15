import time
import random
import statistics
from kafka import KafkaConsumer

TOPIC = "perf-test"
RECORD_SIZE = 1024
SERVERS = ['localhost:9092', 'localhost:9093']
GROUP_ID = "group01"
WINDOW_INTERVAL_MS = 5000
AUTO_OFFSET = "latest"
AUTO_COMMIT = True


def current_milli_time():
    return int(time.time() * 1000)


consumer = KafkaConsumer(bootstrap_servers=SERVERS, group_id=GROUP_ID,
                         auto_offset_reset=AUTO_OFFSET, enable_auto_commit=AUTO_COMMIT)
consumer.subscribe(topics=[TOPIC])

# start_ms = current_milli_time()
start_window_ms = current_milli_time()
window_poll_times = list()
window_msg_num = 0
while True:
    start_record_ms = current_milli_time()
    records_all = consumer.poll()
    received_record_ms = current_milli_time()
    for tp, messages in records_all.items():
        if tp.topic == TOPIC:
            window_msg_num += len(messages)
    poll_time_ms = received_record_ms - start_record_ms
    window_poll_times.append(poll_time_ms)
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

# print("Successful %d records sent (time used: %dms) with latency of %.3fms on avg, %dms on max" %
#       (count, current_milli_time() - start_ms, statistics.mean(latencies), max(latencies)))
