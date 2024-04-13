import typing
from typing import TYPE_CHECKING
from datetime import datetime

from objects.event import Event
from objects.event_buffer import EventBuffer
from patterns.exceptions import NonMatchParents
from patterns.plans.tree.node.node import Node

if TYPE_CHECKING:
    from patterns.plans.tree.tree_evaluation_mechanism import EvaluationMechanism


class TreeInstance:
    """
    This class represents an instance of a tree created during
    evaluation.
    """
    num = 0

    def __init__(self, tree: 'EvaluationMechanism', node: Node,
                 match_buffer: EventBuffer = None):
        self.tree = tree
        self.current_node = node
        self.match_buffer = EventBuffer(window=self.tree.time_window)\
            if match_buffer is None else match_buffer
        self.id = TreeInstance.num
        self.num_events = 0 if not match_buffer else len(match_buffer.events)
        TreeInstance.num += 1
        self.created_from = [] # a list of (event, instance) which constitute
        # this PM
        self.time_detected_match = None
        self.match_latency = 0
        self.size = sum([e.size() for e in self.match_buffer.events])


    def get_events(self) -> typing.List[Event]:
        return self.match_buffer.events

    def has_match(self):
        return self.current_node == self.tree.root

    def add_event(self, event: Event):
        self.num_events += 1
        self.match_buffer.add_event(event)
        self.size += event.size()


    def get_min_timestamp(self):
        return self.match_buffer.earliest_timestamp

    def get_max_timestamp(self):
        return self.match_buffer.latest_timestamp

    def validate_conditions(self) -> bool:
        events = self.match_buffer.events
        return self.current_node.validate_conditions(events)

    def get_match(self) -> typing.Tuple[Event]:
        if not self.has_match() or self.match_buffer.is_expired():
            return None
        return tuple(self.match_buffer.events)

    def set_match_latency(self):
        time_detected_match = datetime.utcnow().timestamp()
        ts_last_event_inserted = max(map(lambda e: e.system_ts_in_adding,
                                         self.match_buffer.events))
        self.match_latency = time_detected_match - ts_last_event_inserted

    def is_expired(self, timestamp: float) -> bool:
        """
        checks if the current time window has not passed yet
        """
        return self.match_buffer.is_expired() or (timestamp and timestamp - \
               self.match_buffer.earliest_timestamp > self.match_buffer.window)

    def get_created_from(self, peer_instance: 'TreeInstance'):
        event, peer_event = None, None
        if self.num_events == 1:
            event = self.get_events()[0]
        if peer_instance.num_events == 1:
            peer_event = peer_instance.get_events()[0]
        created_from = []
        if event is not None:
            created_from.append((event, peer_instance))
        if peer_event is not None:
            created_from.append((peer_event, self))
        return  [t for t in self.created_from] + \
                [y for y in peer_instance.created_from] + created_from

    def create_parent_instance(self, peer_instance: 'TreeInstance'
                               ) -> 'TreeInstance':
        if self.current_node.parent != peer_instance.current_node.parent:
            raise NonMatchParents()
        common_parent = self.current_node.parent
        new_event_buffer = self.match_buffer.clone()
        for event in peer_instance.match_buffer.events:
            new_event_buffer.add_event(event)
        parent_instance = TreeInstance(self.tree, common_parent, new_event_buffer)
        parent_instance.created_from = self.get_created_from(peer_instance)
        return parent_instance

    def __str__(self):
        return f'node: {self.current_node}, buffer: {self.match_buffer}'

    def size(self):
        return sum([e.size() for e in self.match_buffer.events])
