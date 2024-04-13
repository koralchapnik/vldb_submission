import typing

import numpy as np
from sklearn.cluster import KMeans
from sklearn.tree import DecisionTreeClassifier, export_text

from experiments.consts import NUM_CLASSES
from objects.match import Match
from patterns.plans.tree.node.node import Node


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
        self.matches_latencies = list()
        self.matches_latency_sum = 0
        self.num_latencies = 1000
        self.avg_latency = 0
        self.for_learning = {}
        for node in self.root.get_subtree_nodes_list():
            self.for_learning[node] = []
        self.finished_warm_up = False
        self.current_time_slice = None
        self.input_shedding_preds = None
        self.shedding_set = None
        self.knapsack_event_delta = 10

    def finish_warm_up(self):
        self.update_costs()
        print('finished update costs')
        self.learn_k_means()
        self.learn_classifiers()
        self.learn_input_classifiers_per_class()
        self.avg_latency = 0
        self.for_learning = None
        self.finished_warm_up = True

    def knapSack(self, W, wt, val, n):
        # (node, class, contrib, consump, contrib / consump)
        options = [(wt[i][0], wt[i][1], val[i][2], wt[i][2], val[i][2] / wt[i][2] ) for
                   i in range(n)]
        options = sorted(options, key=lambda x: x[4])
        cummulative_w = 0
        result = list()
        for item in options:
            if cummulative_w < W:
                if (item[2] > 0) or item[3] > 0:
                    result.append(item)
                    cummulative_w += item[3]

        return options

    def solve_knapstack(self, W):
        classes = {node: set() for node in self.node_to_instances.keys() if
                   node != self.root}
        for node, pms in self.node_to_instances.items():
            if node == self.root:
                continue
            node_classes = {pm.class_ for pm in pms}
            classes[node] = classes[node] | node_classes

        weights = list() # consumption
        vals = list() # contribution
        for node, class_list in classes.items():
            for class_ in class_list:
                weights.append((node, class_, node.cluster_to_value[class_][
                    'consump']))
                vals.append((node, class_, node.cluster_to_value[class_][
                    'contrib']))
        # shedding set: (node, class, contrib, consump, contrib / consump)
        shedding_set = self.knapSack(W, weights, vals, len(weights))
        result = {node: list() for node in \
                    self.node_to_instances.keys()}
        for item in shedding_set:
            result[item[0]].append(item[1])
        return result

    def tree_to_preds(self, clf: DecisionTreeClassifier):
        """
        each predicate is in the form of (feature name, lambda of pred)
        returns (class -> list of preds)
        """
        result = {c: list() for c in range(NUM_CLASSES)}

        n_nodes = clf.tree_.node_count
        children_left = clf.tree_.children_left
        children_right = clf.tree_.children_right

        feature = clf.tree_.feature
        threshold = clf.tree_.threshold

        node_depth = np.zeros(shape=n_nodes, dtype=np.int64)
        is_leaves = np.zeros(shape=n_nodes, dtype=bool)
        stack = [(0, 0)]  # start with the root node id (0) and its depth (0)
        while len(stack) > 0:
            # `pop` ensures each node is only visited once
            node_id, depth = stack.pop()
            node_depth[node_id] = depth

            # If the left and right child of a node is not the same we have a split node
            is_split_node = children_left[node_id] != children_right[node_id]
            # If a split node, append left and right children and depth to `stack`
            # so we can loop through them
            if is_split_node:
                stack.append((children_left[node_id], depth + 1))
                stack.append((children_right[node_id], depth + 1))
            else:
                is_leaves[node_id] = True

        leaves_nodes = [n for n in range(n_nodes) if is_leaves[n]]
        for leaf in leaves_nodes:
            class_ = np.argmax(clf.tree_.value[leaf])
            # (feature_index, lambda)
            preds = list()
            current = leaf
            while current is not None:
                current_parent = None
                for parent, child in enumerate(children_left):
                    if child == current:
                        preds.append((feature[parent], lambda x: x <= threshold[
                            parent], f'{feature[parent]} <= '
                                     f'{threshold[parent]}'))
                        current_parent = parent
                for parent, child in enumerate(children_right):
                    if child == current:
                        preds.append((feature[parent], lambda x: x > threshold[
                            parent], f'{feature[parent]} > '
                                     f'{threshold[parent]}'))
                        current_parent = parent
                current = current_parent
            result[class_].append(preds)
        return result


    def learn_input_classifiers_per_class(self):
        """
        For each type extracts the conditions of the classifiers
        :return:
        """
        # class -> node -> list of or predicates

        for node in self.for_learning.keys():
            if node == self.root:
                continue
            node.leaves_to_preds = {c: {leaf: None
                                   for leaf in self.node_to_instances.keys() if
                                   leaf.is_leaf()}
                               for c in range(NUM_CLASSES)}
            if node.classifier is None:
                continue
            # getting the node predicates
            print(f'for node: {str(node)}, classifier:'
                  f'\n{export_text(node.classifier)}')
            class_to_preds = self.tree_to_preds(node.classifier)
            print(f'node: {str(node)}')
            final_pred_str, pred_str = '', ''
            for leaf_node in node.leaves_to_preds[0].keys():
                features_indexes = [node.features_order.index(feature_name) for
                                    feature_name in leaf_node.features_order
                                    if feature_name in node.features_order]
                new_indexes = {origin_index: i for i, origin_index in
                                enumerate(features_indexes)}
                print(f'leaf node: {str(leaf_node)}')
                if features_indexes:
                    for class_, class_preds in class_to_preds.items():
                        print(f'class: {class_}')
                        or_preds = [] if class_preds else [lambda x: False]
                        print('(')
                        final_pred_str += ' ('
                        for and_preds_list in class_preds:
                            and_pred = []
                            print(' (')
                            final_pred_str += ' ('
                            for (feature, pred, pred_str) in and_preds_list:
                                and_pred.append(lambda x: pred(x[new_indexes[feature]]))
                                print(' ' + pred_str)
                                final_pred_str += f'  {pred_str} '
                                print(' and ')
                                final_pred_str += ' and '
                            print(' )')
                            final_pred_str += ' )'
                            final_and_pred = lambda x: all([pred(x) for pred in
                                                           and_pred])
                            or_preds.append(final_and_pred)
                            print(' or ')
                            final_pred_str += ' or '
                            print(')')
                            final_pred_str += ' ) '
                        final_or_pred = lambda x: any([pred(x) for pred in
                                                       or_preds])
                        node.leaves_to_preds[class_][leaf_node] = (
                            final_or_pred, pred_str)

    def learn_k_means(self):
        """
        Takes all the contrib and consump values and divide to k classes.
        :return:
        """
        for node, pms in self.for_learning.items():
            if node == self.root:
                continue
            # contains [contrib, consump]
            kmeans_features = []
            for pm in pms:
                kmeans_features.append([pm.contribs,
                                         pm.consumps])
            if not kmeans_features:
                print(f'no data for node: {str(node)}')
            # here we have the features for clustering
            features = np.array(kmeans_features)
            if np.isnan(features).sum().sum():
                print(f'kmeans has nan values')
            features = np.array([x for x in features if not np.isnan(x).sum()])
            features = np.unique(features, axis=0)
            num_clusters = NUM_CLASSES if NUM_CLASSES <= features.shape[0] \
                else features.shape[0]
            if features.shape[0] <= NUM_CLASSES:
                print(f'for node: {node} num samples: '
                      f'{features.shape[0]} < {NUM_CLASSES}')
            if num_clusters > 0:
                kmeans = KMeans(n_clusters=num_clusters).fit(features)
            # updating labels for learning later
            for i, pm in enumerate(pms):
                if num_clusters > 0:
                    pm.class_ = kmeans.predict(np.array([pm.contribs,
                                                         pm.consumps]).reshape(1,-1))[0]
                else:
                    pm.class_ = 1

            # checking 95th percentile for contrib and consumps values
            # for each class
            for c in range(NUM_CLASSES):
                contrib = np.array([pm.contribs for pm in pms if pm.class_ ==
                                    c])
                contrib = contrib[np.isfinite(contrib)]

                consump = np.array([pm.consumps for pm in pms if pm.class_ ==
                                    c])
                consump = consump[np.isfinite(consump)]
                try:
                    node.cluster_to_value[c]['contrib'] = np.percentile(
                        contrib, 90)if contrib.shape[0] > 0 else 0
                    node.cluster_to_value[c]['consump'] = np.percentile(
                        consump, 90) if consump.shape[0] > 0 else 0
                except Exception:
                    print(f'Exception!!! contrib: {contrib}\n consump: '
                          f'{consump}')

    def set_new_contrib_consump(self):
        for node in self.node_to_instances.keys():
            if node == self.root:
                continue
            all_contribs = 0
            all_consumps = 0
            for c in range(NUM_CLASSES):
                all_contribs += node.temp_cluster_to_value[c]['num_matches']
                all_consumps += node.temp_cluster_to_value[c]['cost']

            for c in range(NUM_CLASSES):
                if all_contribs:
                    node.cluster_to_value[c]['contrib'] = \
                        0.5 *  node.cluster_to_value[c]['contrib'] + \
                        0.5 * (float(node.temp_cluster_to_value[c][
                                         'num_matches']) /all_contribs)
                if all_consumps:
                    node.cluster_to_value[c]['consump'] = \
                        0.5 * node.cluster_to_value[c]['consump'] + \
                        0.5 * (float(node.temp_cluster_to_value[c][
                                         'cost']) / all_consumps)

    def learn_classifiers(self):
        """
        learns the classifiers for each state
        """
        for node, pms in self.for_learning.items():
            if node == self.root:
                continue
            features = []
            labels = []
            for pm in pms:
                features.append(pm.get_features())
                labels.append(pm.class_)
            # features and labels for time slice
            features = np.array(features)
            labels = np.array(labels)
            tree = DecisionTreeClassifier(class_weight=None, criterion='gini',
                                          max_depth=NUM_CLASSES,max_features=None,
                                          max_leaf_nodes=None, min_samples_leaf=1,
                                          min_samples_split=2, min_weight_fraction_leaf=0.0)
            try:
                tree.fit(features, labels)
                node.classifier = tree
            except Exception:
                # the tree cannot be fitted since it has no features
                # participating in query
                print(f'cannot fit classifier for node: {node}, features: {features}')
                node.classifier = None

    def update_costs(self):
        """
        updates contribs - consumps vlaues
        :return:
        """
        if self.for_learning is not None:
            for node, pms in self.for_learning.items():
                if node == self.root:
                    continue
                all_matches = 0
                all_costs = 0
                for pm in pms:
                    all_matches += pm.num_matches_created
                    all_costs += pm.total_cost

                for pm in pms:
                    pm.contribs = (pm.num_matches_created / all_matches ) if \
                        all_matches else 0
                    pm.consumps = (pm.total_cost / all_costs) if all_costs \
                        else 0
        else:
            for node, pms in self.node_to_instances.items():
                if node == self.root:
                    continue
                all_matches = 0
                all_costs = 0
                for pm in pms:
                    all_matches += pm.num_matches_created
                    all_costs += pm.total_cost

                for pm in pms:
                    pm.contribs = pm.num_matches_created / all_matches
                    pm.consumps = pm.total_cost / all_costs

    def add_instance(self, instance):
        self.node_to_instances.get(instance.current_node).append(instance)
        if not self.finished_warm_up:
            self.for_learning.get(instance.current_node).append(instance)

        self.size += instance.size

    def get_matches(self) -> typing.List[Match]:
        matches = [i.get_match() for i in self.node_to_instances.get(self.root)]

        self.size -= sum([i.size for i in self.node_to_instances[self.root]])
        self.node_to_instances[self.root] = []
        return matches

    def delete_first_instances(self, peer_node, n):
        self.size -= sum([i.size for i in self.node_to_instances.get(peer_node)[
                                      :n]])
        del self.node_to_instances.get(peer_node)[:n]

    def delete_instances_by_indexes(self, node, array_indexes: typing.List[
        typing.Tuple[int, int]]):
        expired = []
        array = self.node_to_instances.get(node)
        for indexes in reversed(array_indexes):
            i, j = indexes[0], indexes[1]
            expired.extend(array[i: j+1])
            del self.node_to_instances.get(node)[i: j+1]
        self.size -= sum([i.size for i in expired])

    def remove_old_instances(self, timestamp: float):
        for node in self.node_to_instances.keys():
            node_instances = self.node_to_instances.get(node)
            expired_instances = []
            for instance in node_instances:
                if instance.is_expired(timestamp):
                    expired_instances.append(instance)
            for expired_instance in expired_instances:
                node_instances.remove(expired_instance)
                self.size -= expired_instance.size

    def clean(self):
        for node in self.node_to_instances.keys():
            self.node_to_instances[node] = []
