import threading


class LoadShedder:
    def __init__(self, evaluation_mechanism):
        self.threshold = None
        self.num_shedded = 0
        self.evaluation_mechanism = evaluation_mechanism
        self._lock = threading.Lock()

    def start_ls(self, x: int):

        if self.evaluation_mechanism.cdts is None:
            print('Still not collected enough info to get how much to drop!')
            return
        num_to_drop = x
        with self._lock:
            try:
                self.threshold = self.evaluation_mechanism.get_thresholds(
                    num_to_drop)
            except Exception as e:
                print(f'Exception: {str(e)}, num to drop: {num_to_drop}, '
                      f'max threshold: {self.threshold}, p: {x}')
                raise(e)

    def stop_ls(self):
        with self._lock:
            self.threshold = None

    def to_shed(self, event_type, window_pos) -> bool:
        with self._lock:
            if self.threshold is None:
                return False
            utility = self.get_utility(event_type, window_pos)
            partitions = self.evaluation_mechanism.get_partition(window_pos)
            threshold = max([self.threshold[p] for p in partitions])
            if utility <= threshold:
                self.num_shedded += 1
                if self.num_shedded % 10000 == 0:
                    print(f'num shedded: {self.num_shedded}')
                return True
            return False

    def get_utility(self,event_type, window_pos) -> int:
        utility = self.evaluation_mechanism.get_utility(event_type, window_pos)
        return utility

