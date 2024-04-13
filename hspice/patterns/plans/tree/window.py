import typing
from collections import deque
from datetime import datetime
from objects.event import Event
from objects.match import Match
from patterns.plans.tree.node.node import Node
from patterns.plans.tree.tree_instance import TreeInstance
from patterns.plans.tree.tree_instance_storage import TreeInstanceStorage


class Window:
    total_shedded = 0

    @staticmethod
    def init_global_args(eval_mechanism: 'EvaluationMechanism'):
        Window.root = eval_mechanism.root
        Window.event_types_to_leaves = eval_mechanism.event_types_to_leaves
        Window.evaluation_mechanism = eval_mechanism
        Window.window_size = eval_mechanism.estimated_window_size

    def __init__(self, window_time, knwon_ws=None):
        self.storage = TreeInstanceStorage(Window.root)
        self.events = {}
        self.window_time = window_time
        self.start_ts = None
        self.events_info = {}  # event -> statistics
        self.virtual_window = self.init_virtual_window()
        self.threshold = None
        self.num_shedded = 0
        self.known_ws = knwon_ws

    def init_virtual_window(self):
        # virtual window contains tuples
        # (event_type, event_pos_in_window, state_of_pm, o - occurences)
        vw = {}
        for state in Window.root.get_subtree_nodes_list():
            vw[state] = {}
            for event_type in Window.event_types_to_leaves.keys():
                vw[state][event_type] = {}
                for pos in range(self.window_size):
                    vw[state][event_type][pos] = 0
        return vw

    def expired(self, timestamp) -> bool:
        if self.start_ts is None:
            return False
        return timestamp - self.start_ts > self.window_time

    def process_new_event(self, event: Event) -> \
            typing.Set[Match]:
        if event is None:
            return dict()
        self.events[event] = len(self.events)
        if self.start_ts is None:
            self.start_ts = event.get_timestamp()
        leaf = Window.event_types_to_leaves.get(event.type)
        if not leaf.validate_conditions([event]):
            return dict()
        leaf_instance = TreeInstance(Window.evaluation_mechanism, leaf)
        leaf_instance.add_event(event)
        self.activate_tree_processing(leaf_instance)
        leaf.last_evaluated_ts = event.get_timestamp()
        for match_instance in self.storage.node_to_instances.get(Window.root):
            for event_pm in match_instance.created_from:
                event, pm = event_pm[0], event_pm[1]
                self.update_completion(event, pm, match_instance.created_from)
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
        event = None
        if current_instance.num_events == 1:
            event = current_instance.get_events()[0]
        for peer_instance in peer_instances:
            peer_event = None
            complex_instance = None
            if event is not None:
                self.update_comparison(event, peer_instance)
                self.update_virtual_window_counter(event, peer_instance)
                if self.to_shed(event, peer_instance):
                    continue
            else:
                complex_instance = current_instance

            if peer_instance.num_events == 1:
                peer_event = peer_instance.get_events()[0]
                self.update_comparison(peer_event, current_instance)
                self.update_virtual_window_counter(peer_event, current_instance)
                if self.to_shed(peer_event, current_instance):
                    continue
            else:
                complex_instance = peer_instance

            if complex_instance is not None:
                # updating number of comparisons for the events it was
                # created form
                for item in complex_instance.created_from:
                    self.update_comparison(item[0], item[1])
            parent_instance = current_instance.\
                create_parent_instance(peer_instance)
            if parent_instance.validate_conditions():
                instance_queue.append(parent_instance)
                if parent_instance.current_node == \
                        self.evaluation_mechanism.root:
                    # match detected
                    parent_instance.set_match_latency()

    def update_comparison(self, event: Event, pm: TreeInstance):
        if Window.evaluation_mechanism.thresholds is not None:
            return
        window_positions = self.get_event_position(event)
        pm_state = pm.current_node
        event_type = event.type
        for window_pos in window_positions:
            Window.evaluation_mechanism.ut_comparisons[pm_state][event_type][
                window_pos] += 1

    def to_shed(self, event: Event, pm: TreeInstance) -> bool:
        window_pos = self.get_event_position(event)
        pm_state = pm.current_node
        event_type = event.type
        return Window.evaluation_mechanism.load_shedder.to_shed(
            pm_state,
            event_type,
            window_pos
        )

    def scale_down(self, index) -> int:
        # scale to the known_ws to the average window_size
        res = int((index / (self.known_ws - 1)) * (self.window_size - 1))
        return [res]

    def scale_up(self, index) -> typing.List[int]:
        if self.known_ws <= 1:
            return [0]
        scaling_factor = (self.window_size - 1) / (self.known_ws - 1)
        first_index = int(scaling_factor * (index - (self.known_ws - 1)) + \
                          (self.window_size - 1))
        second_index = int(scaling_factor * index)
        return [first_index, second_index]

    def get_event_position(self, event: Event) -> typing.List[int]:
        # TODO: why in paper its written that in scaling up, we get 2 indexes ?
        # the self.known_ws is not known and paper gives a way to predict it
        # but I don't want to implement that.
        index = self.events[event]
        if self.known_ws > Window.window_size:
            return self.scale_down(index)
        if self.known_ws < Window.window_size:
            return self.scale_up(index)
        return [index]

    def update_completion(self, event: Event, pm: TreeInstance, delete):
        if Window.evaluation_mechanism.thresholds is not None:
            return
        window_positions = self.get_event_position(event)
        pm_state = pm.current_node
        event_type = event.type
        for window_pos in window_positions:
            Window.evaluation_mechanism.ut_completion[pm_state][event_type][
                window_pos] += 1

    def update_virtual_window_counter(self, event: Event, pm: TreeInstance):
        if Window.evaluation_mechanism.thresholds is not None:
            return
        window_positions = self.get_event_position(event)
        pm_state = pm.current_node
        event_type = event.type
        for window_pos in window_positions:
            self.virtual_window[pm_state][event_type][window_pos] += 1

    def sum_virtual_window(self) -> int:
        sum = 0
        for state in Window.root.get_subtree_nodes_list():
            for event_type in Window.event_types_to_leaves.keys():
                for pos in range(self.window_size):
                    sum += self.virtual_window[state][event_type][pos]
        return sum

    def get_utility(self, event: Event, pm: TreeInstance) -> int:
        window_pos = self.get_event_position(event)
        pm_state = pm.current_node
        event_type = event.type
        utility = Window.evaluation_mechanism.get_utility(pm_state, event_type,
                                                          window_pos)
        return utility

    def drop(self, p: int, lambda_: int):
        """
        :param p: the number of events to drop from the window.
        :param lambda_: the drop interval, default is the window size
        :return:
        """
        # TODO: convert to drop from partial matches.
        avg_occurence = Window.evaluation_mechanism.avg_occurence
        if avg_occurence is None :
            print('Still not collected enough info to get how much to drop!')
            return
        num_to_drop = int(p * avg_occurence)
        self.threshold = Window.evaluation_mechanism.thresholds[num_to_drop]
