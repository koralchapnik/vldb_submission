import os
import sys
import threading
import typing
from collections import deque
from datetime import datetime

from experiments.consts import NUM_TIME_SLICES
from objects.event import Event
from objects.event_type import EventType
from objects.match import Match
from patterns.pattern import Pattern
from patterns.plans.tree.node.leaf_node import LeafNode
from patterns.plans.tree.node.node import Node
from patterns.plans.tree.tree_evaluation_plan import TreeEvaluationPlan
from patterns.plans.tree.tree_instance import TreeInstance
from patterns.plans.tree.tree_instance_storage import TreeInstanceStorage

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
        self.put_num_left_conditions(self.root)
        self.storage = TreeInstanceStorage(self.root)
        self.storage_size = -1
        self.total_storage_size = 0
        self.time_window = pattern.time_window
        self._lock = threading.Lock()
        self.min_event_timestamp, self.max_event_timestamp = None, None
        self.num_shedded = 0
        self.num_pms_shedded = 0
        self.num_processed = 0
        self.num_matches = 0
        self.latency_bound = float(os.environ.get('LB', 1))
        self.num_added = 0
        self.set_final_buffer_sizes = False
        self.max_latency = int(os.environ.get('MAX_LATENCY'))
        self.time_slice_delta = self.time_window / NUM_TIME_SLICES
        # adds a classifier for each time slice
        self.classes_classifiers = list()
        self.active_windows = []
        self.current_time_slice_ts = None
        self.current_time_slice = None

        # contains list of classes for each node
        self.shedding_set = None


    def put_num_left_conditions(self, node):
        if node is None:
            return

        node.num_predicates_left = 0 if \
            node.parent is None else \
            (node.parent.num_predicates_left + len(node.parent.conditions))

        self.put_num_left_conditions(node.left_son)
        self.put_num_left_conditions(node.right_son)


    def finish_warm_up(self):
        # learn the classifiers
        self.storage.finish_warm_up()

    def update_current_time_slice(self, ts):
        # update the time slice according to the current arrived event
        if self.current_time_slice_ts is None or self.current_time_slice is None:
            self.current_time_slice = 0
            self.current_time_slice_ts = ts
            self.storage.current_time_slice = self.current_time_slice
            return

        # check if time_delta has been passed
        if ts - self.current_time_slice_ts > self.time_slice_delta:
            # update to new time slice
            self.current_time_slice = (ts - self.current_time_slice_ts) // \
                                      self.time_slice_delta
            self.current_time_slice_ts = ts
            self.storage.current_time_slice = self.current_time_slice
            self.storage.remove_old_instances(ts)
            if self.storage.finished_warm_up:
                self.storage.set_new_contrib_consump()

    def _init_event_types_to_leaves(self) -> typing.Dict[EventType, LeafNode]:
        leaves = self.root.get_subtree_leaves()
        return {node.event_type: node for node in leaves}

    def get_next_event(self) -> Event:
        with self._lock:
            available_events = []
            min_ts = None
            min_ts_system = None # system_ts_in_adding
            min_ts_leaf = None
            min_event = None
            for leaf in self.event_types_to_leaves.values():
                next_event = leaf.show_next_event()
                if next_event is None:
                    continue
                available_events.append(next_event)
                ts = next_event.get_timestamp()
                if min_ts is None:
                    min_ts = ts
                    min_ts_leaf = leaf
                    min_event = next_event
                elif ts < min_ts:
                    min_ts = ts
                    min_ts_leaf = leaf
                    min_event = next_event
                elif ts == min_ts and next_event.system_ts_in_adding < \
                        min_event.system_ts_in_adding:
                    min_ts = ts
                    min_ts_leaf = leaf
                    min_event = next_event

            if not available_events:
                return None
            next = min_ts_leaf.get_next_event()
            return next

    def process(self) -> int:
        next_event = self.get_next_event()

        if next_event is None:
            return None

        # update time slice
        self.update_current_time_slice(next_event.get_timestamp())

        new_matches = self.process_new_event(next_event)
        matches_len = len(new_matches)
        self.num_matches += matches_len
        return matches_len

    def process_new_event(self, event: Event) -> \
            typing.List[Match]:
        if event is None:
            return []
        if self.num_processed % 1000 == 0:
            print(f'processed {self.num_processed} events, matches:'
                  f' {self.num_matches}')
            print(f'storage size: {self.storage_size}')
        self.min_event_timestamp = event.get_timestamp()
        self.num_processed += 1
        leaf = self.event_types_to_leaves.get(event.type)
        leaf_instance = TreeInstance(self, leaf, None)
        leaf_instance.add_event(event)
        storage_size_temp = self.storage.size
        self.total_storage_size += storage_size_temp
        if self.storage.finished_warm_up:
            self.storage_size = storage_size_temp if storage_size_temp > \
                                                     self.storage_size else self.storage_size
        # check if to shed the event
        shed_amount = self.to_shed()
        if shed_amount > 0 and self.storage.finished_warm_up:
            # means we must shed events
            if self.num_processed % self.storage.knapsack_event_delta == 0 or\
                self.shedding_set is None:
                # calc extent of load
                W = float(shed_amount) / self.storage.avg_latency
                # solve the knapsack problem
                self.shedding_set = self.storage.solve_knapstack(W)
                # remove all matches in shedding set
                total_shedded = 0
                for node, pms in self.storage.node_to_instances.items():
                    if node == self.root:
                        continue
                    to_shed = list()
                    for pm in pms:
                        if pm.class_ in self.shedding_set[node]:
                            to_shed.append(pm)
                    self.num_pms_shedded += len(to_shed)
                    total_shedded += len(to_shed)
                    for pm in to_shed:
                        pms.remove(pm)
                print(f'solving knapsack for w: {W}, shedding_set: {total_shedded}')

            #input shedding
            features = [event.attrs[i] for i in [attr.split('--')[1] for attr in
                            leaf.features_order]]
            for node, classes in self.shedding_set.items():
                for c in classes:
                    pred = node.leaves_to_preds[c][leaf]
                    if pred is not None and pred[0](features):
                        self.num_shedded += 1
                        if self.num_shedded % 100 == 0:
                            print(f'num shedded: {self.num_shedded}')
                        return []

        if not leaf.validate_conditions([event]):
            return []

        self.activate_tree_processing(leaf_instance)
        leaf.last_evaluated_ts = event.get_timestamp()


        return self.storage.get_matches()

    def activate_tree_processing(self, leaf_instance: TreeInstance):
        """
        This method is responsible for creating the relevant matches created by
        inserting the new event to the system
        """

        instance_queue = deque()
        instance_queue.append(leaf_instance)
        self.storage.add_instance(leaf_instance)
        while instance_queue:
            current_instance = instance_queue.popleft()
            if current_instance != leaf_instance:
                self.storage.add_instance(current_instance)
            peer_node = self.get_peer(current_instance)
            if not peer_node:
                continue

            self.process_event_on_peer_instance_set(current_instance,
                                                        peer_node,
                                                        instance_queue)

    def get_peer(self, current_instance: TreeInstance) -> Node:
        return current_instance.current_node.get_peer()

    def process_event_on_peer_instance_set(self, current_instance: TreeInstance,
                                           peer_node: Node,
                                           instance_queue: deque):
        """
        checks if the newly arrived event can create more matches for the
        relevant instances of it's peers
        :param current_instance: the current instance from the queue
        :param peer_instances: the peers instances of the current_instance
        :param instance_queue: holds relevant instances we should process
        while evaluating the newly arrived event
        """
        peer_instances = self.storage.node_to_instances.get(peer_node, None)
        if not peer_instances:
            return
        sequence_delete, first_idx = None, None
        delete_indxes = []
        for i, peer_instance in enumerate(peer_instances):
            if peer_instance.is_expired(self.min_event_timestamp):
                if first_idx is None:
                    first_idx = i
                continue
            if first_idx is not None:
                max_idx = i - 1
                delete_indxes.append((first_idx, max_idx))
                first_idx = None
            parent_instance = current_instance.create_parent_instance(
                peer_instance)
            if parent_instance.validate_conditions() and not \
                    parent_instance.is_expired(self.min_event_timestamp):
                if parent_instance.current_node == self.root:
                    # its a match
                    for i in parent_instance.composed_from:
                        if not self.storage.finished_warm_up:
                            i.num_matches_created += 1
                        else:
                            i.current_node.temp_cluster_to_value[i.class_][
                                'num_matches'] += 1

                    match_latency = parent_instance.get_latency()
                    if match_latency <= self.max_latency:
                        self.storage.matches_latency_sum += match_latency
                        if len(self.storage.matches_latencies) == \
                                self.storage.num_latencies:
                            self.storage.matches_latencies.pop(0)
                        self.storage.matches_latencies.append(match_latency)
                        self.storage.avg_latency = float(sum(
                            self.storage.matches_latencies)) / len(
                            self.storage.matches_latencies)

                        instance_queue.append(parent_instance)
                else:
                    instance_queue.append(parent_instance)
        self.storage.delete_instances_by_indexes(peer_node, delete_indxes)

    def to_shed(self) -> float:
        if self.storage.avg_latency > self.latency_bound:
            return float(self.storage.avg_latency) - self.latency_bound
        else:
            return 0

    def add_event(self, event: Event) -> None:
        event.system_ts_in_adding = datetime.utcnow().timestamp()
        leaf_node = self.get_node(event)
        self.num_added += 1
        leaf_node.add_event(event)

    def get_node(self, event: Event) -> LeafNode:
        return self.event_types_to_leaves.get(event.type, None)
