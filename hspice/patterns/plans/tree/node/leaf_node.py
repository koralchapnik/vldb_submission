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
        self.processed_conditions = []

    def get_event_types(self) -> typing.List[EventType]:
        return [self.event_type]

    def get_subtree_leaves(self) -> typing.List['LeafNode']:
        return [self]

    def __str__(self):
        return f'{self.event_type.name}'

    def is_leaf(self) -> bool:
        return True

    def size(self) -> int:
        return self.event_type.size

    def set_time_to_next_event(self):
        beta = 1.0 / float(self.exp_rate)
        self.time_to_next_event = np.random.exponential(scale=beta) * (10 ** 9)
        self.last_time_arrived = time.time_ns()

    def utility_func(self, event: Event) -> float:
        return 0
