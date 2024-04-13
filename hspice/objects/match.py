from objects.event import Event
from datetime import datetime
import typing


class Match:
    def __init__(self, primitive_events: typing.List[Event],
                 latest_event_timestamp: float):
        self.primitive_events = primitive_events
        self.detection_latency = datetime.utcnow().timestamp() - \
                                 latest_event_timestamp

    def __str__(self):
        match_str = ', '.join(str(e) for e in self.primitive_events)
        return f'[{match_str}]'

    def __eq__(self, other):
        return [e.name for e in self.primitive_events] == [o.name for o in \
                other.primitive_events]

    def clone(self) -> 'Match':
        primitive_events = [event for event in self.primitive_events]
        return Match(primitive_events)

    def size(self):
        return sum(map(lambda e: e.size(), self.primitive_events))
