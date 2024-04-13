from datetime import datetime
from typing import TYPE_CHECKING

import numpy as np

from objects.event import Event
from objects.event_buffer import EventBuffer
from objects.match import Match
from patterns.exceptions import NonMatchParents
from patterns.plans.tree.node.node import Node

if TYPE_CHECKING:
    pass


class TreeInstance:
    """
    This class represents an instance of a tree created during
    evaluation.
    """
    def __init__(self, tree, node: Node,
                 match_buffer: EventBuffer = None, composed_from=None):
        self.tree = tree
        self.current_node = node
        self.match_buffer = EventBuffer(window=self.tree.time_window)\
            if match_buffer is None else match_buffer
        self.size = sum([e.size() for e in self.match_buffer.events])
        self.composed_from = composed_from if composed_from else list()
        self.num_matches_created = 0
        self.time_slice = self.tree.current_time_slice
        self.total_cost = self.get_cost()
        self.contribs = 0
        self.consumps = 0
        self.class_ = None
        if match_buffer is not None:
            self.set_class()
        self.slice_to_label = 0

    def get_cost(self):
        return self.current_node.num_predicates_left

    def has_match(self):
        return self.current_node == self.tree.root

    def get_features(self):
        features = []
        for type_ in self.current_node.events_order:
            relevant_type = [event for event in self.match_buffer.events if
                             event.type.name == type_][0]
            for attr_name in self.current_node.events_to_attrs[type_]:
                features.append(relevant_type.attrs[attr_name])
        if self.tree.storage.finished_warm_up:
            return np.array(features).reshape(1, -1)
        else:
            return features

    def get_class(self):
        """
        :return: the class of the PM according to its time slice
        """
        features = self.get_features()
        class_ = self.current_node.classify(features)
        return class_

    def set_class(self):
        features = self.get_features()
        if self.current_node != self.tree.root and self.tree.storage.finished_warm_up:
            self.class_ = self.current_node.classify(features)

    def get_latency(self) -> float:
        now = datetime.utcnow().timestamp()
        ts_last_event_inserted = max(map(lambda e: e.system_ts_in_adding,
                                         self.match_buffer.events))
        return now - ts_last_event_inserted

    def add_event(self, event: Event):
        self.match_buffer.add_event(event)
        self.size += event.size()
        if self.class_ is None and self.tree.storage.finished_warm_up:
            self.set_class()

    def get_min_timestamp(self):
        return self.match_buffer.earliest_timestamp

    def get_max_timestamp(self):
        return self.match_buffer.latest_timestamp

    def validate_conditions(self) -> bool:
        events = self.match_buffer.events
        return self.current_node.validate_conditions(events)

    def get_match(self) -> Match:
        if not self.has_match() or self.match_buffer.is_expired():
            return None
        return Match(self.match_buffer.events,
                     self.match_buffer.latest_timestamp)

    def is_expired(self, timestamp: float) -> bool:
        """
        checks if the current time window has not passed yet
        """
        return self.match_buffer.is_expired() or (timestamp and timestamp - \
               self.match_buffer.earliest_timestamp > self.match_buffer.window)

    def create_parent_instance(self, peer_instance: 'TreeInstance') -> 'TreeInstance':
        if self.current_node.parent != peer_instance.current_node.parent:
            raise NonMatchParents()
        common_parent = self.current_node.parent
        new_event_buffer = self.match_buffer.clone()
        for event in peer_instance.match_buffer.events:
            new_event_buffer.add_event(event)

        add_left = [self] + self.composed_from
        add_right = [peer_instance] + peer_instance.composed_from

        res = TreeInstance(self.tree, common_parent, new_event_buffer,
                           composed_from=add_left + add_right)

        # updating consump for each event class
        for pm in res.composed_from:
            if self.tree.storage.finished_warm_up:
                try:
                    pm.current_node.temp_cluster_to_value[
                        pm.class_]['cost'] += res.total_cost
                except Exception as e:
                    print(str(e))
            pm.total_cost += res.total_cost
        return res

    def __str__(self):
        return f'node: {self.current_node}, buffer: {self.match_buffer}'

