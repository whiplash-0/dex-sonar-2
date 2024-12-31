from datetime import datetime, timedelta
from typing import Generic, Sequence, TypeVar


Value = TypeVar('Value')
Timestamp = datetime
Timestep = timedelta
Index = int
Values = list[Value]
Timestamps = list[Timestamp]


class InvalidTimeRange(Exception):
    def __init__(self, start, end, ts_start=None, ts_end=None, message=None):
        interval = f'{start} - {end}'
        message = ': ' + message if message else ''
        if ts_start is None: super().__init__(f'{interval}{message}')
        else: super().__init__(f'Can\'t update time series {ts_start} - {ts_end} with range {interval}{message}')


class TimeSeries(Generic[Value]):
    def __init__(   self, step: Timestep):
        self.values: Values = []
        self.start: Timestamp = None
        self.step: Timestep = step
        self.final: Index = -1

    def __len__(self):
        return len(self.values)

    def __getitem__(self, index: Index | slice) -> Value | list[Value]:
        return self.values[index]

    def __repr__(self):
        if self.is_empty():
            return f'{self.__class__.__name__}()'
        else:
            format = "%m/%d-%H:%M"
            return f'{self.__class__.__name__}({self.start.strftime(format)} -> {self.end.strftime(format)}, length={len(self)})'

    def is_empty(self):
        return not self.values

    def get_last_index(self) -> Index:
        return self.last

    def get_time_step(self) -> Timestep:
        return self.step

    def get_timestamp(self, index: Index) -> Timestamp:
        return self.start + self.step * (index % len(self.values))

    def get_values(self) -> Values:
        return self.values

    def get_timestamps(self) -> Timestamps:
        return [self.start + self.step * i for i in range(len(self.values))]

    def update(self, values: Value | Sequence[Value], timestamps: Timestamp | Sequence[Timestamp], is_final: bool = False):
        return self._update_single(values, timestamps, is_final) if isinstance(timestamps, Timestamp) else self._update_bulk(values, timestamps, is_final)

    @property
    def end(self) -> Timestamp:
        return self.start + self.step * (len(self.values) - 1)

    @property
    def last(self) -> Index:
        return len(self.values) - 1

    def _timestamp_to_index(self, timestamp: Timestamp, start: Timestamp = None) -> Index:
        return (timestamp - (self.start if start is None else start)) // self.step

    def _index_to_timestamp(self, index: Index, start: Timestamp = None) -> Timestamp:
        return self.start if start is None else start + self.step * index

    def _update_final(self, index):
        self.final = max(index, self.final)

    def _update_single(self, value: Value, timestamp: Timestamp, is_final: bool = False):
        if self.is_empty():
            index = 0
            self.values.append(value)
            self.start = timestamp
        else:
            index = self._timestamp_to_index(timestamp)

            if 0 <= index <= self.last:
                if index > self.final: self.values[index] = value
            elif index - self.last == 1:
                self.values.append(value)
            else:
                raise InvalidTimeRange(timestamp, timestamp, self.start, self.end)

        if is_final: self._update_final(index)

    def _pad(self, values: Sequence[Value], timestamps: Sequence[Timestamp]) -> Values:
        indices = [self._timestamp_to_index(x, start=timestamps[0] if self.is_empty() else self.start) for x in timestamps]
        differences = [indices[i + 1] - indices[i] for i in range(len(indices) - 1)]
        need_padding = any([x > 1 for x in differences])

        if need_padding:
            padded_values = []

            for i, d in enumerate(differences):
                if d == 1: padded_values.append(values[i])
                else: padded_values.extend([values[i]] * d)
            padded_values.append(values[-1])

            values = padded_values

        return values

    def _update_bulk(self, values: Sequence[Value], timestamps: Sequence[Timestamp], is_final: bool = False):
        start, end = timestamps[0], timestamps[-1]
        ts_start = start if self.is_empty() else self.start
        first, last = self._timestamp_to_index(start, start=ts_start), self._timestamp_to_index(end, start=ts_start)

        # validation
        if self.is_empty():
            if not (start <= end): raise InvalidTimeRange(start, end)
        else:
            if not (0 <= first <= self.last + 1 and first <= last): raise InvalidTimeRange(start, timestamps[1], self.start, self.end, message='Start timestamp doesn\'t satisfy satisfy range conditions')

        # cut the new values to the final index (if possible to speed up the next phase)
        if first <= self.final:
            if last <= self.final:
                return
            else:
                first = self.final + 1

                for i, timestamp in enumerate(reversed(timestamps)):
                    ts_index = self._timestamp_to_index(timestamp, start=ts_start)

                    # find the first value for new series
                    if ts_index <= first:
                        local_index = len(timestamps) - i - 1
                        values = values[local_index:]
                        if ts_index == first: timestamps = [self._index_to_timestamp(first, start=ts_start)] + timestamps[local_index + 1:]
                        break

        # pad the missing values (to make it a continuous sequence) and assign them
        if self.is_empty(): self.start = start
        self.values[first:last + 1] = self._pad(values, timestamps)

        if is_final: self._update_final(last)
