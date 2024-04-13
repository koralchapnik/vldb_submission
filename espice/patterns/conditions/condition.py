from objects.event_type import EventType
from objects.event import Event
from abc import ABC, abstractmethod
from patterns.exceptions import InvalidConditionParams
import scipy.stats
import typing


class Condition(ABC):
    def __init__(self, desc: str, event_types: typing.List[EventType],
                 attr_1: str, attr_2: str, const: str, op_name: str, op_func:
            typing.Callable, selectivity: float = None, user_input: bool =
                 True, double_by=None, free_var_op=None, free_var=None):
        self.desc = desc
        self.event_types = event_types
        self.attr_1 = attr_1
        self.attr_2 = attr_2
        self.const = const
        self.op_name = op_name
        self.op_func = op_func
        self.selectivity = selectivity
        self.double_by = None if double_by is None else float(double_by)
        self.abs = False
        self.func = self._gen_func()
        self.user_input = user_input

    def __len__(self):
        return len(self.event_types)

    def get_other_type(self, event_type) -> EventType:
        for type_ in self.event_types:
            if type_ != event_type:
                return type_
        return None

    def _gen_func(self):
        if self.attr_2 is None:
            return lambda e: self.op_func(e.attrs[self.attr_1], self.const)
        else:
            return lambda e_1, e_2: self.op_func(e_1.attrs[self.attr_1],
                                            e_2.attrs[self.attr_2])

    def verify(self, *events: Event) -> bool:
        self._check_arguments(events)
        ordered_events = self._order_events(events)
        return self.func(*ordered_events)

    def _order_events(self, events: typing.Tuple[Event]) -> typing.Iterable[Event]:
        # Assuming len(events) <= 2
        return events if self.event_types[0].name == events[0].type.name else\
            reversed(events)

    def _check_arguments(self, events: typing.Tuple[Event]):
        if len(events) != len(self.event_types):
            raise InvalidConditionParams('Not enough or too much events')
