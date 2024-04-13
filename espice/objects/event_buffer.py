import typing

from objects.event import Event


class EventBuffer:
    def __init__(self, events: typing.List[Event] = None,
                 max_size: float = None, window: float = None):
        self.events = events if events is not None else []
        self.sorted_events = None
        self.utility_func = None
        self.earliest_timestamp = None
        self.latest_timestamp = None
        self.max_size = int(max_size) if max_size is not None else None
        self.window = window
        self.size = 0
        self.start_seq = None
        self.min_utility = None

    def is_full(self):
        if self.max_size is None:
            return False
        return self.size >= self.max_size

    def add_event(self, event: Event, limit: bool = False) -> bool:
        if self.is_full() and limit:
            return False
        self.events.append(event)
        self.update_timestamp(event)
        self.size += 1
        return True

    def update_min_timestamp(self):
        self.earliest_timestamp = self.events[0].get_timestamp() if \
            self.events else None

    def update_timestamp(self, event: Event):
        timestamp = event.get_timestamp()
        if self.earliest_timestamp is None or timestamp < \
                self.earliest_timestamp:
            self.earliest_timestamp = timestamp
        if self.latest_timestamp is None or timestamp > self.latest_timestamp:
            self.latest_timestamp = timestamp

    def is_expired(self) -> bool:
        return self.latest_timestamp - self.earliest_timestamp > self.window

    def clone(self) -> 'EventBuffer':
        events = [event for event in self.events]
        new_event_buffer = EventBuffer(events, self.max_size, self.window)
        new_event_buffer.earliest_timestamp = self.earliest_timestamp
        new_event_buffer.latest_timestamp = self.latest_timestamp
        return new_event_buffer

    def left_size(self) -> int:
        return self.max_size - self.size

    def __str__(self):
        return f'{self.events}'
