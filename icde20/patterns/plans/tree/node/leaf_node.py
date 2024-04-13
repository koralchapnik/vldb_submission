from patterns.plans.tree.node.node import Node
from patterns.conditions.condition import Condition
from objects.event import Event
from objects.event_type import EventType
from objects.event_buffer import EventBuffer
import time
import typing
import math
import numpy as np


class LeafNode(Node):
    def __init__(self, parent: Node, conditions: typing.List[Condition],
                 event_type: EventType, window: float):
        super().__init__(None, None, parent, conditions)
        self.event_type = event_type
        self.max_size = None #events per second
        self.total_events = 0
        self.last_ts = None
        self.event_buffer = EventBuffer(None, self.max_size, window)
        self.event_buffer.start_seq = self.event_type.start_seq
        self.height_selectivity = 1
        self.last_evaluated_ts = None
        self.processed_conditions = []
        self.last_time_arrived = 0.0  # in nano - seconds
        self.time_to_next_event = 0.0  # in ns
        self.sum_buffer_sizes = 0
        self.number_insertions = 0

    def get_event_types(self) -> typing.List[EventType]:
        return [self.event_type]

    def get_subtree_leaves(self) -> typing.List['LeafNode']:
        return [self]

    def __str__(self):
        return f'{self.event_type.name}'

    def is_leaf(self) -> bool:
        return True

    def add_event(self, event: Event) -> bool:
        success = self.event_buffer.add_event(event)
        if success:
            self.sum_buffer_sizes += self.event_buffer.size
            self.number_insertions += 1
        return success

    def get_next_event(self) -> Event:
        event = self.event_buffer.get_next_event()
        return event

    def show_next_event(self) -> Event:
        return self.event_buffer.show_next_event()

    def get_min_ts(self) -> float:
        return self.event_buffer.earliest_timestamp

    def get_max_ts(self) -> float:
        return self.event_buffer.latest_timestamp

    def update_max_size(self, rate: float):
        self.max_size = rate
        self.event_buffer.max_size = math.ceil(self.max_size)
        print(f'leaf_instance: {self.event_type.name} buffer size:'
              f' {self.event_buffer.max_size}')

    def calc_height_selectivity(self):
        current_node = self
        while current_node is not None:
            self.height_selectivity *= current_node.selectivity
            current_node = current_node.parent

    def size(self) -> int:
        return self.event_type.size
