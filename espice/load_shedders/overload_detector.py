import math
import os
import time
import threading

from experiments.consts import STATS_COUNT


class OverloadDetector:
    def __init__(self, evaluation_mechanism, latency_bound: float):
        self.evaluation_mechanism = evaluation_mechanism
        self.latency_processing = 0
        self.q_max_num_stats = STATS_COUNT
        self.period = 50
        self.throughput = 0
        self.qmax_throughput = 0
        self.arrival_rate = 0
        self.arrived = 0
        self.processed = 0
        self.qmax_start_ts, self.qmax_end_ts = 0, 0  # seconds
        self.qmax_processed = 0
        self.start_ts, self.end_ts = 0, 0  # seconds
        self.latency_bound = latency_bound
        self.q_max = None
        self.partition_size = None
        self.f = float(os.environ.get('F'))
        self._lock = threading.Lock()
        self.warm_up_finished = False

    def event_processed(self):
        with self._lock:
            self.processed += 1
            if self.processed == self.period:
                self.end_ts = time.time()
                delta = self.end_ts - self.start_ts
                self.throughput = self.processed / delta
                self.arrival_rate = self.arrived / delta
                self.detect_overload()
                self.restart_vals()
            # qmax calculation
            if self.qmax_start_ts == 0:
                self.qmax_start_ts = time.time()
            self.qmax_processed += 1
            if self.q_max is None and self.qmax_processed == self.q_max_num_stats:
                # updating q_max only once when there is no overload!
                self.qmax_end_ts = time.time()
                self.qmax_throughput = self.qmax_processed / (self.qmax_end_ts -
                                                              self.qmax_start_ts)
                self.set_q_max()

    def restart_vals(self):
        self.processed = 0
        self.start_ts, self.end_ts = time.time(), 0  # seconds
        self.arrived = 0

    def event_arrived(self):
        with self._lock:
            self.arrived += 1


    def set_q_max(self):
        processing_latency = 1 / self.qmax_throughput
        self.evaluation_mechanism.processing_latency = processing_latency
        self.q_max = math.ceil(self.latency_bound / processing_latency)
        buffer_size = self.q_max - self.q_max * self.f
        self.evaluation_mechanism.q_max = self.q_max
        self.evaluation_mechanism.init_partitions_vars(buffer_size)

    def detect_overload(self):
        if self.q_max is None or self.evaluation_mechanism.cdts is \
                None or not self.warm_up_finished:
            return
        if len(self.evaluation_mechanism.events) >= math.ceil(self.q_max *
                                                              self.f):
            # shedding happened
            delta = self.arrival_rate - self.throughput
            if self.arrival_rate <= self.throughput:
                return
            drop_amount = delta * \
                          self.evaluation_mechanism.partition_size / \
                          self.arrival_rate
            self.evaluation_mechanism.load_shedder.start_ls(drop_amount)
        else:
            # there is no shedding
            self.evaluation_mechanism.load_shedder.stop_ls()