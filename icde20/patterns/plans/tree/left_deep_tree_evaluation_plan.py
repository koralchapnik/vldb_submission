from patterns.conditions.condition import Condition
from patterns.exceptions import *
from patterns.pattern import Pattern
from patterns.plans.tree.node.internal_node import InternalNode
from patterns.plans.tree.node.leaf_node import LeafNode
from patterns.plans.tree.node.node import Node
from patterns.plans.tree.tree_evaluation_plan import TreeEvaluationPlan


class LeftDeepTreeEvaluationPlan(TreeEvaluationPlan):
    def __init__(self):
        super().__init__()

    def build_tree_plan(self, pattern: Pattern) -> Node:
        event_type = pattern.event_types[0]
        current_root = LeafNode(None, [], event_type, pattern.time_window)
        for event_type in pattern.event_types[1:]:
            right_node = LeafNode(None, [], event_type, pattern.time_window)
            parent = InternalNode(current_root, right_node, None, [],
                                  pattern.op)
            current_root.parent = parent
            right_node.parent = parent
            current_root = parent
        self.root = current_root
        self.put_conditions(pattern)
        conds_attrs = self.get_conditions_attrs(pattern)
        self.root.init_events_to_attrs(conds_attrs)
        return self.root


    def get_conditions_attrs(self, pattern:Pattern):

        res = {event_type.name: list() for event_type in pattern.event_types}

        for condition in pattern.conditions:
            for key, val in zip([t.name for t in condition.event_types],
                                [condition.attr_1, condition.attr_2]):
                if (val is not None) and (val != 'time'):
                    res[key] = sorted(list(set(res[key]) | {val}))

        return res


    def get_common_parent(self, condition: Condition) -> Node:
        if len(condition.event_types) == 1:
            return self.get_leaf_by_event_type(condition.event_types[0])
        current_node = self.root
        while True:
            if current_node is None:
                raise EventTypeNotInTree(f'one of the event'
                                         f' types {condition.event_types} '
                                         f'not found in tree')
            if current_node.right_son.event_type in condition.event_types:
                return current_node
            current_node = current_node.left_son
