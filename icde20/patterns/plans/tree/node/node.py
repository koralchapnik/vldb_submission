from patterns.conditions.condition import Condition
from objects.event_type import EventType
from objects.event import Event
from abc import ABC, abstractmethod
from experiments.consts import NUM_TIME_SLICES, NUM_CLASSES
import typing


class Node(ABC):
    def __init__(self, left_son, right_son, parent,
                 conditions: typing.List[Condition]):
        self.left_son = left_son
        self.right_son = right_son
        self.parent = parent
        self.conditions = conditions if conditions is not None else []
        self.selectivity = 1
        self.cost = 1
        self.load = 0
        self.subtree_load = 0
        self.subtree_leaves = None
        self.classifier = None
        # cluster -> (contrib, consump)
        self.cluster_to_value = {j: {'contrib': 0, 'consump': 0} for j in range(
            NUM_CLASSES)}

        self.temp_cluster_to_value = {j: {'num_matches': 0, 'cost': 0} for j
                                      in
                                  range(NUM_CLASSES)}
        self.num_predicates_left = 0

    def init_events_to_attrs(self, conds_attrs):
        if self.left_son is not None and self.right_son is not None:
            self.left_son.init_events_to_attrs(conds_attrs)
            self.right_son.init_events_to_attrs(conds_attrs)

        self.events_order = [t.name for t in self.get_event_types()]
        self.events_to_attrs = self.get_conditions_attrs(conds_attrs)
        self.features_order = []
        for type_name, attrs in self.events_to_attrs.items():
            for attr in attrs:
                self.features_order.append(f'{type_name}--{attr}')

    def get_conditions_attrs(self, conds_attrs):
        return {t: conds_attrs[t] for t in self.events_order}

    def classify(self, features):
        if self.classifier is None:
            return 1
        class_ = self.classifier.predict(features)
        return class_[0]

    def update_selectivity(self, n: float):
        """
        this method upfates the selectivity of the node in factor of n and
        traverses to the parent in order to update there too.
        :param n: the new factor of the selectivity
        """
        current_node = self
        while current_node is not None:
            current_node.selectivity *= n
            current_node = current_node.parent

    @abstractmethod
    def get_event_types(self) -> typing.List[EventType]:
        pass

    def get_subtree_nodes_list(self) -> typing.List:
        if self.subtree_leaves is not None:
            return self.subtree_leaves
        if self.left_son is None and self.right_son is None:
            return [self]
        self.subtree_leaves =  self.left_son.get_subtree_nodes_list() + \
               self.right_son.get_subtree_nodes_list() + [self]
        return self.subtree_leaves

    @abstractmethod
    def get_subtree_leaves(self) -> typing.List['Node']:
        pass

    def validate_conditions(self, events: typing.List[Event]) -> bool:
        for condition in self.conditions:
            # assuming events are ordered as tree leaves
            relevant_events = list(filter(lambda e: e.type in condition.event_types,
                                          events))
            if not condition.verify(*relevant_events):
                return False
        return True

    def get_peer(self) -> 'Node':
        if self.parent is None:
            return None
        if self == self.parent.right_son:
            return self.parent.left_son
        if self == self.parent.left_son:
            return self.parent.right_son
        return None

    @abstractmethod
    def size(self) -> int:
        # returns the expected size of the expected event buffer
        pass

    @abstractmethod
    def is_leaf(self) -> bool:
        pass
