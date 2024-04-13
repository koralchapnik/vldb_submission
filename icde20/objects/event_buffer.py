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

    def add_event(self, event: Event, limit: bool = False) -> bool:
        self.events.append(event)
        self.update_timestamp(event)
        self.size += 1
        return True

    def update_min_timestamp(self):
        self.earliest_timestamp = self.events[0].get_timestamp() if \
            self.events else None

    def get_next_event(self, from_sorted: bool = False) -> Event:
        if not self.events:
            return None
        res = self.events.pop(0)
        self.size -= 1
        self.update_min_timestamp()
        return res

    def show_next_event(self) -> Event:
        if not self.events:
            return None
        res = self.events[0]
        return res

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

    def __str__(self):
        return f'{self.events}'
