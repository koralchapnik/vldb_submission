import os

DATA_SIZE = int(os.getenv('DATA_SIZE', '1000000'))
EVENT_TYPES = 'event_types'
PATTERN = 'pattern'
PATTERN_LEN = 'pattern_len'
DEFAULT_PATTERN_LEN = 'default_pattern_len'
DEFAULT_COND_LEN = 'default_cond_len'
SELECTIVITY = 'conditions_selectivity'
EXPECTED_RATE = 'event_types_expected_rate'
ATTRS_DIST_PARAMS = 'attrs_dist_params'
SORT_DELTA = 'sort_delta'
MAX_LOAD = 'max_load'
PEAK_DATA = os.environ.get('DATA', None)
CONFIG = 'config.json'
PORT = int(os.getenv('PORT', '80'))
IP = os.getenv('IP', 'localhost')
MAX_LOAD_ARR = 'max_load_arr'
TIME_ARR = 'time_arr'
MATCHES_ARR = 'matches_arr'
SHEDDED_ARR = 'shedded_arr'
REMOVED_ARR = 'removed_arr'
PENALIZED_ARR = 'penalized_arr'
THROUGHPUT_ARR = 'throughput_arr'
LOAD_SHEDDING_ARR = 'is_load_shedding'
OVERLOADED_ARR = 'overloaded'
STATS = [LOAD_SHEDDING_ARR, MAX_LOAD_ARR, TIME_ARR, MATCHES_ARR, SHEDDED_ARR,
         REMOVED_ARR, PENALIZED_ARR, OVERLOADED_ARR, THROUGHPUT_ARR]
WITH_LS = 'with load shedding'
WITHOUT_LS = 'without load shedding'
THROUGHPUT_DELTA = 'throughput_delta'
MAX_MATCHES = 'max_matches'
BOOST = 'boost'  # the peak size
ALLOW_SLEEPING = 'allow_sleeping'
RAND_INDEX = 'rand_index'
BUFFER_SIZES = 'buffer_size'

GOOGLE = 'Google'
MICROSOFT = 'Microsoft'
APPLE = 'Apple'
TIME = 'time'
SCORE = 'score'
PRICE = 'price'

MEAN_MAX_LOAD = 'mean_max_load'
TOTAL_MATCHES = 'total_matches'
TOTAL_EVENTS = 'total_events'
COND_LEN = 'condition_len'


UPDATE_RATE = 2

GOT_MESSAGE = 'OK'
FINISH = 'FINISH'
CHECK_FINISHED_WARM_UP = 'CHECK_FINISHED_WARM_UP'
FINISHED_WARM_UP = 'FINISHED_WARM_UP'
NOT_FINISHED_WARM_UP = 'NOT_FINISHED_WARM_UP'
SLEEP_WARM_UP_SEC = 10

SEPARATOR = '|'
WARM_UP = int(os.environ.get('WARM_UP'))
STATS_COUNT = min([5000, WARM_UP])
ATTRS_TYPES = 'attrs_types'
ORIGIN_DATA_DIR = 'experiments/datasets'
DATASET_KIND = os.environ.get('DATASET_KIND')


########## classifier constants ##############
# how many classes of PMs should be
NUM_CLASSES = int(os.environ.get('NUM_CLASSES',  3))
RESULT_SUFFIX = os.environ.get('RESULT_SUFFIX', '')
# how many time slices should be
NUM_TIME_SLICES = int(os.environ.get('NUM_TIME_SLICES', 4))

