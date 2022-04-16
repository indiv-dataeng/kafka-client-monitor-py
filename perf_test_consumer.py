import time
import statistics
from kafka import KafkaConsumer
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

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
# Push to prometheus pushgateway
PUSH_PG = True
PG_ADDRESS = "localhost:9091"
PG_JOB_NAME = "kafka_consumer"
PG_GROUPING_KEY = "consumer01"


def current_milli_time():
    return int(time.perf_counter() * 1000)


registry = CollectorRegistry()
g_collected_time = Gauge("consumer_collected_time", "The time each metrics are being collected and summarized", registry=registry)
g_records_msg = Gauge("consumer_received_msg", "The number of records received on the testing topic", registry=registry)
g_poll_time_avg = Gauge("consumer_poll_time_avg", "The average polling time of the given interval period", registry=registry)
g_throughput_msg = Gauge("consumer_throughput_msgpms", "The throughput of the consumer in messages per millisecond", registry=registry)

consumer = KafkaConsumer(bootstrap_servers=SERVERS, group_id=GROUP_ID,
                         auto_offset_reset=AUTO_OFFSET, enable_auto_commit=AUTO_COMMIT)
consumer.subscribe(topics=[TOPIC])

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
        throughput_msgpms = window_msg_num / elapse_window_ms
        throughput_bpms = total_bytes / elapse_window_ms
        print("Received %d records (elapsed: %dms) of total size = %dB" %
              (window_msg_num, elapse_window_ms, total_bytes))
        print("\tPolling Time Per Round: %.3fms AVG, %dms MAX" %
              (poll_time_mean, poll_time_max))
        print("\tThroughput: %.3fmsg/ms, %.3fB/ms" %
              (throughput_msgpms, throughput_bpms))
        
        if PUSH_PG:
            g_collected_time.set_to_current_time()
            g_records_msg.set(window_msg_num)
            g_poll_time_avg.set(poll_time_mean)
            g_throughput_msg.set(throughput_msgpms)
            push_to_gateway(PG_ADDRESS, job=PG_JOB_NAME, registry=registry, grouping_key=PG_GROUPING_KEY)

        window_poll_times = list()
        window_msg_num = 0
        start_window_ms = received_record_ms
