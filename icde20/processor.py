import socket
import threading
import time
import typing
import signal
from datetime import datetime

import experiments.results_consts as rc
from experiments.configurations_map import config, boost_size, boost_peaks
from experiments.consts import *
from objects.match import Match
from parsers.parser import Parser
from patterns.plans.tree.left_deep_tree_evaluation_plan import \
    LeftDeepTreeEvaluationPlan
from patterns.plans.tree.tree_evaluation_mechanism import EvaluationMechanism
import signal
import socket
import threading
import time
import typing
from datetime import datetime

import experiments.results_consts as rc
from experiments.configurations_map import config, boost_size, boost_peaks
from experiments.consts import *
from objects.match import Match
from parsers.parser import Parser
from patterns.plans.tree.left_deep_tree_evaluation_plan import \
    LeftDeepTreeEvaluationPlan
from patterns.plans.tree.tree_evaluation_mechanism import EvaluationMechanism


class Processor:
    def __init__(self, use_ls: bool):

        self.config = config
        self.num_events = config[TOTAL_EVENTS]
        self.last_tp_time = None
        self.tp_events_sum, self.num_processed, self.new_shedded, \
        self.buffer_shedded, self.not_related, self.num_overloaded = [0] * 6
        self.num_reset = 0
        self.throughputs = []
        self._lock = threading.Lock()
        self.boost = config[BOOST]
        self.allow_sleeping = config[ALLOW_SLEEPING]
        self.use_ls = use_ls
        self.parser = Parser(config[EVENT_TYPES])
        self.types, self.pattern = self.parser.parse_pattern(config[PATTERN],
                                                           config.get(SELECTIVITY),
                                                           config[
                                                                 ATTRS_DIST_PARAMS], config[ATTRS_TYPES])

        self.evaluation_mechanism = EvaluationMechanism(self.pattern,
                                                        LeftDeepTreeEvaluationPlan(),
                                                        use_ls)
        self.matches = 0
        self.start_time, self.end_time, self.time_warm_up = 0, 0, 0

    def add_event(self, event_str: str):
            with self._lock:
                event = self.parser.parse_event_from_str(event_str)
                self.evaluation_mechanism.add_event(event)

    def add_events_route(self, msg):
        event_str, event_ws = msg.split(SEPARATOR)
        event_ws = int(event_ws)
        self.add_event(event_str)

    def add_events(self):
        server_address = ('0.0.0.0', 80)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(server_address)
        sock.listen(1)

        conn, address = sock.accept()
        x = conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        print(f'got connection from {str(address)}')
        while True:
            data = conn.recv(2048).decode()

            if data:
                if data == FINISH:
                    conn.close()
                    break
                elif data == CHECK_FINISHED_WARM_UP:
                    if self.num_processed == WARM_UP:
                        conn.send(FINISHED_WARM_UP.encode())
                        self.evaluation_mechanism.finish_warm_up()
                        print(f'finished warm up!, matches: {self.matches}')
                        self.time_warm_up = datetime.utcnow().timestamp()
                    else:
                        conn.send(NOT_FINISHED_WARM_UP.encode())
                else:
                    conn.send(GOT_MESSAGE.encode())
                    self.add_events_route(data)

    def update_results(self, result):
        result.update({
            rc.NEW_SHEDDED: self.new_shedded,
            rc.BUFFER_SHEDDED: self.buffer_shedded,
            rc.NOT_RELATED: self.not_related
        })

    def process_events(self, matches: typing.List[Match], result: typing.Dict):
        time.sleep(5)  # waiting for starting getting events

        while self.num_processed != self.num_events:
            new_matches = self.evaluation_mechanism.process()
            if new_matches is None:
                continue  # there is no available event
            self.matches += new_matches
            with self._lock:
                self.num_processed += 1

    def write_results(self, signum=None, frame=None):
        if signum is not None:
            stopped = 1
            signum = signum
            self.end_time = datetime.utcnow().timestamp()
        else:
            stopped = 0
            signum = -1
        results = {
            'queue_size': -1,
            'processing_latency': -1,
            'num_shedded': self.evaluation_mechanism.num_shedded,
            'num_pms_shedded': self.evaluation_mechanism.num_pms_shedded,
            'found_matches': self.matches,
            'matches_latency':
                self.evaluation_mechanism.storage.matches_latency_sum
                / self.matches if self.matches else 0,
            'known_ws': float(os.environ.get('KNOWN_WS', -1)),
            'pm_size_per_window': float(os.environ.get('PM_PER_WIN', -1)),
            'match_size_per_window': float(os.environ.get('MATCHES_PER_WIN',
                                                          -1)),
            'pattern': os.environ.get('PATTERN'),
            'lb': float(os.environ.get('LB', -1)),
            'data': DATASET_KIND,
            'time': self.end_time - self.start_time,
            'time_warm_up': self.time_warm_up - self.start_time,
            'warm_up': WARM_UP,
            'stopped': stopped,
            'signum': signum,
            'pattern_len': len(self.pattern.event_types),
            'window_time': self.pattern.time_window,
            'sleep_reg': float(os.environ.get('SLEEP_REG', -1)),
            'sleep_load': float(os.environ.get('SLEEP_LOAD', -1)),
            'port': os.environ.get('PORT'),
            'boost_size': boost_size,
            'boost_peaks': boost_peaks,
            'throughput': DATA_SIZE / (self.end_time - self.start_time),
            'data_size': DATA_SIZE,
            'num_processed': self.num_processed,
            'storage_size': self.evaluation_mechanism.storage_size,
            'storage_size_avg': self.evaluation_mechanism.total_storage_size
                                / float(self.num_processed)
        }

        print(results)
        # port = int(os.environ.get('PORT', -1))

        # out_file = f'results_final/{port}.json'
        # with open(out_file, 'w', encoding='utf-8') as f:
        #     json.dump(results, f, ensure_ascii=False, indent=4)

    def run(self) -> None:
        print(config[PATTERN])
        result = dict()
        self.start_time = datetime.utcnow().timestamp()
        matches = []
        t2 = threading.Thread(target=self.process_events,
                              args=(matches, result,))
        t2.start()
        self.add_events()
        t2.join()

        self.end_time = datetime.utcnow().timestamp()

        print(f'num of matches {self.matches}')
        self.write_results()


if __name__ == '__main__':
    is_ls = bool(int(os.getenv('IS_LS', '1')))
    processor = Processor(is_ls)
    signal.signal(signal.SIGTERM, processor.write_results)
    processor.run()
