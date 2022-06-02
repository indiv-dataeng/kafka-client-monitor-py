import time
import statistics


INITIAL_SAMPLE_SIZE = 500
WAIT_TIME_S = 5

actual_sleep_time = list()
for _ in range(INITIAL_SAMPLE_SIZE):
    start = time.perf_counter()
    time.sleep(1e-6)
    end = time.perf_counter()
    actual_sleep_time.append((end - start) * 1000)

sleep_time_mean = statistics.fmean(actual_sleep_time)
deciles = statistics.quantiles(actual_sleep_time, n=10)
print("time.sleep() Precision (Sample Size=%d)" % INITIAL_SAMPLE_SIZE)
print("\tMean = %.3fms, Median = %.3fms" %
      (sleep_time_mean, deciles[4]))
print("\tMin = %.3fms, P10 = %.3fms, P90 = %.3fms, Max = %.3fms" %
      (min(actual_sleep_time), deciles[0], deciles[-1], max(actual_sleep_time)))
print("OS Sleep Time Precision should be around mean=%.3fms" % sleep_time_mean)
print()


def test_sleep(sleep_ms):
    sleep_s = sleep_ms / 1000
    sample_size = int(WAIT_TIME_S / sleep_s)
    sleep_elapsed = list()

    for _ in range(sample_size):
        start = time.perf_counter()
        time.sleep(sleep_s)
        end = time.perf_counter()
        sleep_elapsed.append((end - start) * 1000)

    deciles = statistics.quantiles(sleep_elapsed, n=10)
    print("Input=%dms (Sample Size=%d)" %
          (sleep_ms, sample_size))
    print("\tMean = %.3fms, Median = %.3fms" %
          (statistics.fmean(sleep_elapsed), deciles[4]))
    print("\tMin = %.3fms, P10 = %.3fms, P90 = %.3fms, Max = %.3fms" %
          (min(sleep_elapsed), deciles[0], deciles[-1], max(sleep_elapsed)))


if sleep_time_mean >= 2:
    test_sleep(int(sleep_time_mean))
    test_sleep(int(sleep_time_mean) + 1)
    test_sleep(int(sleep_time_mean * 2) - 1)
    test_sleep(int(sleep_time_mean * 2))
    test_sleep(int(sleep_time_mean * 2) + 1)
