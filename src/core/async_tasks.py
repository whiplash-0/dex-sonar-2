import asyncio
import logging
import time as pytime
from datetime import timedelta
from typing import Callable, Coroutine, Optional


logger = logging.getLogger(__name__)


RawCoroutine = Callable[[], Coroutine]


class AsyncSequentialTasks:
    def __init__(self, *coroutines: Coroutine):
        self.coroutines = coroutines
        self.loop: Optional[asyncio.AbstractEventLoop] = None

    async def run(self):
        self.loop = asyncio.get_event_loop()
        for x in self.coroutines: await x

    def schedule_coroutine_in_async_thread(self, coroutine: Coroutine):
        asyncio.run_coroutine_threadsafe(coroutine, loop=self.loop)


class AsyncConcurrentTasks:
    """
    In the case of non-blocking run, exceptions should be handled at the individual task level.
    If an exception occurs, all other related tasks should be cancelled accordingly, this won't be done automatically
    """
    def __init__(self, *coroutines: Coroutine):
        self.coroutines = coroutines
        self.tasks: list[asyncio.Task] = []
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self._are_cancelled = False

    async def run(self, blocking=False):
        try:
            self.loop = asyncio.get_event_loop()
            self.tasks = [asyncio.create_task(x) for x in self.coroutines]
            if blocking: await asyncio.gather(*self.tasks)

        except asyncio.CancelledError:
            logger.debug('Caught `CancelledError`. Cancelling all tasks and waiting for them to complete')

        finally:
            await self.cancel_all()


    async def cancel_all(self):
        self._are_cancelled = True

        for x in self.tasks:
            if not x.done(): x.cancel()

        await asyncio.gather(*self.tasks, return_exceptions=True)  # supress raising exception, instead handle on task level

    def are_cancelled(self):
        return self._are_cancelled

    def run_coroutine_threadsafe(self, coro: Coroutine):
        asyncio.run_coroutine_threadsafe(coro, self.loop)


class AsyncConcurrentPollingTasks(AsyncConcurrentTasks):
    def __init__(self, *raw_coroutines_and_poll_intervals: tuple[RawCoroutine, timedelta]):
        super().__init__(*[self._wrap_coroutine_with_polling(x, y) for x, y in raw_coroutines_and_poll_intervals])

    @staticmethod
    async def _wrap_coroutine_with_polling(raw_coroutine, poll_interval):
        while True:
            start = pytime.monotonic()
            await raw_coroutine()
            await asyncio.sleep(max(poll_interval.total_seconds() - (pytime.monotonic() - start), 0))
