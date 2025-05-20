import asyncio
import logging
import signal
import time as pytime
from abc import ABC
from asyncio import AbstractEventLoop, Task as GeneralAsyncioTask
from typing import Any, Callable, Coroutine, Generator, Iterator, Optional

from src.utils.time import Timedelta


logger = logging.getLogger(__name__)


Task = Coroutine[Any, Any, None]
RawTask = Callable[[], Task]
AsyncioTask = GeneralAsyncioTask[None]
TerminationSignalHandler = Callable[[], None]



class AsyncTasksBase(ABC):
    def __init__(
            self,
            *tasks: Task,
            termination_signal_handler: Optional[TerminationSignalHandler] = None,
    ):
        self.pending_tasks: Iterator[Task] = iter(tasks)
        self.started_tasks: list[AsyncioTask] = []
        self.termination_signal_handler: Optional[TerminationSignalHandler] = termination_signal_handler
        self.event_loop: Optional[AbstractEventLoop] = None
        self._are_cancelled = False

    def run(self):
        """
        Must be called at the beginning of the overridden method in subclasses
        """
        self.event_loop = asyncio.get_event_loop()

        if self.termination_signal_handler:
            self.event_loop.add_signal_handler(signal.SIGINT, self.termination_signal_handler)
            self.event_loop.add_signal_handler(signal.SIGTERM, self.termination_signal_handler)

    async def cancel(self):
        self._are_cancelled = True

        for x in self.pending_tasks:  # ensure there are no non-awaited coroutine objects to avoid runtime warning
            x.close()

        for x in self.started_tasks:
            if not x.done(): x.cancel()

        await asyncio.gather(
            *self.started_tasks,
            return_exceptions=True,  # supress raising exception, instead just keep it in task metadata
        )

    def are_cancelled(self):
        return self._are_cancelled

    def schedule_task_in_async_thread(self, task: Task):
        asyncio.run_coroutine_threadsafe(task, loop=self.event_loop)

    def _start(self) -> Generator[AsyncioTask, None, None]:
        for x in self.pending_tasks:
            self.started_tasks.append(asyncio.create_task(x))
            yield self.started_tasks[-1]



class AsyncSequentialTasks(AsyncTasksBase):
    async def run(self):
        super().run()

        try:
            for x in self._start(): await x

        except asyncio.CancelledError:
            logger.debug('Tasks or some task were cancelled. Skipping all following')

        finally:
            await self.cancel()



class AsyncConcurrentTasks(AsyncTasksBase):
    """
    In the case of non-blocking run, exceptions should be handled at the individual task level.
    If an exception occurs, all other related tasks should be cancelled accordingly, this won't be done automatically
    """
    async def run(self, blocking=False):
        super().run()

        try:
            started_tasks = [x for x in self._start()]
            if blocking: await asyncio.gather(*started_tasks)

        except asyncio.CancelledError:
            logger.debug('Tasks or some task were cancelled. Cancelling remaining tasks and waiting for them to complete')

        finally:
            if blocking: await self.cancel()



class AsyncConcurrentPollingTasks(AsyncConcurrentTasks):
    def __init__(self, *raw_tasks_and_poll_intervals: tuple[RawTask, Timedelta]):
        super().__init__(
            *[self._wrap_task_with_polling(x, y) for x, y in raw_tasks_and_poll_intervals]
        )

    @staticmethod
    async def _wrap_task_with_polling(raw_task, poll_interval):
        while True:
            start = pytime.monotonic()
            await raw_task()
            await asyncio.sleep(max(poll_interval.total_seconds() - (pytime.monotonic() - start), 0))
