import threading
import math


class LoadShedder:
    def __init__(self, evaluation_mechanism):
        self.threshold = None
        self.num_shedded = 0
        self.evaluation_mechanism = evaluation_mechanism
        self._lock = threading.Lock()

    def start_ls(self, num_to_drop: int):
        if self.evaluation_mechanism.thresholds is None:
            print('Still not collected enough info to get how much to drop!')
            return
        with self._lock:
            try:
                self.threshold = []
                for p in range(self.evaluation_mechanism.num_partitions):
                    num_to_drop_from_partition = int(math.floor(
                        num_to_drop*self.evaluation_mechanism.avg_occurence_partitions[p]))
                    thresholds_for_p = self.evaluation_mechanism.thresholds[p]
                    partition_threshold = thresholds_for_p[
                        num_to_drop_from_partition] if \
                        num_to_drop_from_partition < len(thresholds_for_p) \
                        else thresholds_for_p[-1]
                    self.threshold.append(partition_threshold)
            except Exception as e:
                print(f'Exception: {str(e)}, num to drop: {num_to_drop}, '
                      f'max threshold: {self.threshold}')

    def stop_ls(self):
        with self._lock:
            self.threshold = None

    def to_shed(self, pm_state, event_type, window_pos) -> bool:
        with self._lock:
            if self.threshold is None:
                return False
            utility = self.get_utility(pm_state, event_type, window_pos)
            partitions = self.evaluation_mechanism.get_partition(window_pos)
            threshold = max([self.threshold[p] for p in partitions])
            if utility <= threshold:
                self.num_shedded += 1
                if self.num_shedded % 10000 == 0:
                    print(f'num shedded: {self.num_shedded}')
                return True
            return False

    def get_utility(self, pm_state, event_type, window_pos) -> int:
        utility = self.evaluation_mechanism.get_utility(pm_state, event_type,
                                                          window_pos)
        return utility

