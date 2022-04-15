from functools import reduce
import time

SAMPLE_SIZE_CLOCKDELTA = 1000000


def current_milli_time():
    return int(time.perf_counter() * 1000)


def measure_clockdelta():
    t0 = time.perf_counter()
    t1 = t0
    while t1 == t0:
        t1 = time.perf_counter()
    return (t0, t1, t1-t0)


clock_precision = reduce(lambda a, b: a+b,
                         [measure_clockdelta()[2] for i in range(SAMPLE_SIZE_CLOCKDELTA)], 0.0) / SAMPLE_SIZE_CLOCKDELTA
print("time.perf_counter Precision: %fus" % (clock_precision * 1e6))
