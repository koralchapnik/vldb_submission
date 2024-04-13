import math
import os
import threading
import typing
from datetime import datetime

from load_shedders.load_shedder import LoadShedder
from load_shedders.overload_detector import OverloadDetector
from objects.event import Event
from objects.event_type import EventType
from objects.match import Match
from patterns.pattern import Pattern
from patterns.plans.tree.node.leaf_node import LeafNode
from patterns.plans.tree.tree_evaluation_plan import TreeEvaluationPlan
from patterns.plans.tree.tree_instance_storage import TreeInstanceStorage
from patterns.plans.tree.window import Window

NEW_SHEDDED, NOT_RELATED, BUFFER_SHEDDED = int, int, int


class EvaluationMechanism:
    """
    This class contains the overall data needed to evaluate
    a given pattern with the tree evaluation mechanism
    """
    def __init__(self, pattern: Pattern, evaluation_plan: TreeEvaluationPlan,
                 use_ls: bool):
        self.use_ls = use_ls
        self.root = evaluation_plan.build_tree_plan(pattern)
        self.event_types_to_leaves = self._init_event_types_to_leaves()
        self.events = []
        self.windows = []
        self.storage = TreeInstanceStorage(self.root)
        self.storage_size = -1
        self.total_storage_size = 0
        self.time_window = int(pattern.time_window)
        self._lock = threading.Lock()
        self.min_event_timestamp, self.max_event_timestamp = None, None
        self.load_shedder = LoadShedder(self)
        self.num_shedded = 0
        self.num_processed = 0
        self.num_matches = 0
        self.shedded_non_ls = []
        self.max_window_stats = int(os.environ.get('MAX_WINDOW_STATS'))
        self.virtual_window_size = 0
        self.estimated_window_size = int(float(os.environ.get('WS', 0)))
        self.load_shedder = LoadShedder(self)
        latency_bound = float(os.environ.get('LB', 1))
        self.overload_detector = OverloadDetector(self, latency_bound)
        self.ut_completion = self.init_stats()
        self.position_shares = self.init_stats()
        self.current_window_stats = 0
        self.partition_size = None
        self.num_partitions = None
        self.thresholds = None
        self.cdts = None
        self.num_added = 0
        self.matches_latency_sum = 0
        self.q_max = None
        self.processing_latency = None
        Window.init_global_args(self)

    def init_partitions_vars(self, buffer_size):
        print(f'buffer size: {buffer_size}')
        if buffer_size == 0:
            self.partition_size = self.estimated_window_size
        else:
            self.partition_size = math.ceil(buffer_size)
        self.num_partitions = math.ceil(self.estimated_window_size / self.partition_size)

    def add_to_window_stats(self, window: Window):
        if self.partition_size is None:
            return
        if self.current_window_stats == self.max_window_stats:
            return
        self.current_window_stats += 1

        if self.current_window_stats == self.max_window_stats:
            self.finish_utility_table_creation()
            self.finish_position_shares_creation()
            self.create_cdts()

    def finish_position_shares_creation(self):
        for pos in range(self.estimated_window_size):
            sum_pos = 0
            for event_type in self.event_types_to_leaves.keys():
                sum_pos += self.position_shares[event_type][pos]
            for event_type in self.event_types_to_leaves.keys():
                self.position_shares[event_type][pos] /= float(sum_pos)

    def finish_utility_table_creation(self):
        # normalizes the values of the utilities
        for event_type in self.event_types_to_leaves.keys():
            sum_event_type = sum(self.ut_completion[event_type].values())
            for pos in range(self.estimated_window_size):
                self.ut_completion[event_type][pos] = int((self.ut_completion[
                    event_type][pos] / sum_event_type) * 100) if \
                    sum_event_type else 0

    def get_utility(self,event_type: EventType,
                    window_positions: typing.List[int]) -> int:
        utilities = []
        for window_pos in window_positions:
            utility = Window.evaluation_mechanism.ut_completion[event_type][
                window_pos]
            utilities.append(utility)
        return int(sum(utilities) / len(utilities))

    def get_partition(self, indexes: typing.List[int]) -> typing.List[int]:
        res = []
        for index in indexes:
            res.append(math.floor(index / self.partition_size))
        return res

    def get_thresholds(self, x: float) -> typing.List[int]:
        thresholds = [0 for p in range(self.num_partitions)]
        for p in range(self.num_partitions):
            for u in range(0, 101):
                if self.cdts[p][u] >= x:
                    thresholds[p] = u
                    break

        return thresholds

    def create_cdts(self):
        partitions_dict = {p: {'cdt': {i: 0 for i in range(0, 101)},
                               'temp': {i: 0 for i in range(0, 101)}}
                           for p in range(self.num_partitions)}

        for event_type in self.event_types_to_leaves.keys():
            for pos in range(self.estimated_window_size):
                o = self.position_shares[event_type][pos]
                u = self.get_utility(event_type, [pos])
                partition = self.get_partition([pos])[0]
                partitions_dict[partition]['temp'][u] += o

        self.cdts = [None for p in range(self.num_partitions)]

        for p in range(self.num_partitions):
            cdt = partitions_dict[p]['cdt']
            temp = partitions_dict[p]['temp']
            cdt[0] = temp[0]
            for u in range(1, 101):
                cdt[u] = cdt[u-1] + temp[u]

            self.cdts[p] = cdt

    def init_stats(self):
        stats = dict()
        for event_type in self.event_types_to_leaves.keys():
            stats[event_type] = dict()
            for pos in range(self.estimated_window_size):
                stats[event_type][pos] = 0
        return stats

    def _init_event_types_to_leaves(self) -> typing.Dict[EventType, LeafNode]:
        leaves = self.root.get_subtree_leaves()
        return {node.event_type: node for node in leaves}

    def get_next_event(self) -> Event:
        # return the next event to process - which has the minimum timestamp
        with self._lock:
            if not self.events:
                return None
            return self.events.pop(0)

    def add_new_window_from_event(self, event: Event) -> None:
        new_window = Window(self.time_window, event.window_size)
        self.windows.append(new_window)

    def process(self) -> typing.List[Match]:
        next_event = self.get_next_event()
        if next_event is None:
            return None
        if self.num_processed % 1000 == 0:
            print(f'processed {self.num_processed} events, matches:'
                  f' {self.num_matches}')
        self.add_new_window_from_event(next_event)
        expired_windows = 0
        windows_matches = set()
        for window in self.windows:
            if window.expired(next_event.get_timestamp()):
                self.add_to_window_stats(window)
                expired_windows += 1
                continue
            new_matches = window.process_new_event(next_event)
            unique_matches = new_matches.keys() - windows_matches
            self.matches_latency_sum += sum(
                map(lambda m: new_matches.get(m),
                    unique_matches))
            windows_matches.update(unique_matches)
        self.overload_detector.event_processed()
        storage_size_temp = sum([w.storage.size for w in self.windows])
        if self.overload_detector.warm_up_finished:
            self.storage_size = storage_size_temp if storage_size_temp > \
                                                     self.storage_size else self.storage_size
        del self.windows[:expired_windows]
        self.total_storage_size += (storage_size_temp / float(10 ** 6))
        self.num_matches += len(windows_matches)
        self.num_processed += 1
        self.min_event_timestamp = next_event.get_timestamp()
        return len(windows_matches)

    def update_timestamps(self):
        timestamps = [l.last_evaluated_ts for l in
                      self.event_types_to_leaves.values()]
        if None in timestamps:
            return
        self.min_event_timestamp = min(timestamps)

    def add_event(self, event: Event) -> typing.Tuple[NEW_SHEDDED,
                                                      BUFFER_SHEDDED,
                                                      NOT_RELATED]:
        with self._lock:
            self.num_added += 1
            event.system_ts_in_adding = datetime.utcnow().timestamp()
            self.events.append(event)
            self.overload_detector.event_arrived()
        return 0, 0, 0

    def get_node(self, event: Event) -> LeafNode:
        return self.event_types_to_leaves.get(event.type, None)