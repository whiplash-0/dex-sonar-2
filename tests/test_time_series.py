from datetime import datetime, timedelta
from unittest import TestCase, main

from src.core.time_series import TimeSeries


START = datetime.utcnow()
STEP = timedelta(minutes=1)


class TestTimeSeries(TestCase):
    def test_update(self):
        N = 5
        values = list(range(N))
        timestamps = [START + STEP * i for i in range(N)]

        ts = TimeSeries(step=STEP)
        for i in range(N): ts.update(values[i], timestamps[i])

        self.assertEqual(ts.get_values(), values)
        self.assertEqual(ts.get_timestamps(), timestamps)

    def test_update_bulk(self):
        N = 5
        values = list(range(N))
        timestamps = [START + STEP * i for i in range(N)]

        ts = TimeSeries(step=STEP)
        ts.update(values, timestamps)

        self.assertEqual(ts.get_values(), values)
        self.assertEqual(ts.get_timestamps(), timestamps)

    def test_update_bulk_2_times(self):
        N = 5
        values = list(range(N))
        timestamps = [START + STEP * i for i in range(N)]

        N_2 = 7
        N_2_START = 3
        values_2 = list(range(N_2))
        timestamps_2 = [START + STEP * (N_2_START + i) for i in range(N_2)]

        final_values = list(range(N_2_START)) + list(range(N_2))
        final_timestamps = [START + STEP * i for i in range(N_2_START + N_2)]

        ts = TimeSeries(step=STEP)
        ts.update(values, timestamps)
        ts.update(values_2, timestamps_2)

        self.assertEqual(ts.get_values(), final_values)
        self.assertEqual(ts.get_timestamps(), final_timestamps)

    def test_update_bulk_2_times_final(self):
        N = 5
        values = list(range(N))
        timestamps = [START + STEP * i for i in range(N)]

        N_2 = 7
        N_2_START = 3
        values_2 = list(range(N_2))
        timestamps_2 = [START + STEP * i for i in range(N_2_START, N_2_START + N_2)]

        final_values = list(range(N)) + list(range(N_2))[N - N_2_START:]
        final_timestamps = [START + STEP * i for i in range(N_2_START + N_2)]

        ts = TimeSeries(step=STEP)
        ts.update(values, timestamps, is_final=True)
        ts.update(values_2, timestamps_2)

        self.assertEqual(ts.get_values(), final_values)
        self.assertEqual(ts.get_timestamps(), final_timestamps)


if __name__ == '__main__':
    main()
