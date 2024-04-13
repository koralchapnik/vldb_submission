from patterns.plans.tree.node.node import Node
from objects.match import Match
import typing
import os


class TreeInstanceStorage:
    """
    This class represents the storage of all the relevant instances
    created during the evaluation of the pattern.
    Creation and deletion of objects is dynamic.
    """

    def __init__(self, root: Node):
        self.root = root
        self.node_to_instances = {}
        for node in self.root.get_subtree_nodes_list():
            self.node_to_instances[node] = []
        self.size = 0
        self.max_latency = int(os.environ.get('MAX_LATENCY'))

    def add_instance(self, instance):
        self.node_to_instances.get(instance.current_node).append(instance)
        self.size += instance.size

    def get_matches(self) -> typing.Set[Match]:
        root_instances = self.node_to_instances.get(self.root)
        matches = {i.get_match(): i.match_latency for i in
                          root_instances if i.match_latency <= self.max_latency}
        self.delete_first_instances(self.root, len(root_instances))
        self.node_to_instances[self.root] = []
        return matches

    def delete_first_instances(self, node, n):
        self.size -= sum([i.size for i in self.node_to_instances.get(node)[
                                          :n]])
        del self.node_to_instances.get(node)[:n]

    def remove_old_instances(self, timestamp: float):
        for node in self.node_to_instances.keys():
            node_instances = self.node_to_instances.get(node)
            expired_instances = []
            for instance in node_instances:
                if instance.is_expired(timestamp):
                    expired_instances.append(instance)
            for expired_instance in expired_instances:
                self.size -= expired_instance.size()
                node_instances.remove(expired_instance)

    def clean(self):
        for node in self.node_to_instances.keys():
            self.node_to_instances[node] = []
        self.size = 0
