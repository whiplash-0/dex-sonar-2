import asyncio
import concurrent
import logging
import signal
from asyncio import AbstractEventLoop, Task, TaskGroup
from concurrent.futures import Future
from typing import Any, Callable, Coroutine as GeneralCoroutine, Iterable, Iterator, Optional, Sequence, TypeVar

from src.utils.time import Time, Timedelta



logger = logging.getLogger(__name__)



VoidFunction = Callable[..., None]
CoroutineObject = GeneralCoroutine[Any, Any, None]
Coroutine = Callable[[], CoroutineObject]
TerminationSignalHandler = Callable[[], None]



T = TypeVar('T')


class ThreadedTasks:
    def __init__(self, function: VoidFunction, args: Sequence[Iterable[T]], max_workers: int = 10):  # requests / urllib3 supports only 10 connections
        self.function = function
        self.args = args
        self.max_workers = max_workers

    def run(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_index = {
                executor.submit(self.function, *args): i
                for i, args in enumerate(self.args)
            }

            results = [None] * len(self.args)

            try:
                for future in concurrent.futures.as_completed(future_to_index):
                    results[future_to_index[future]] = future.result()

            except Exception as e:
                for f in future_to_index:
                    f.cancel()
                raise e

            return results

    @staticmethod
    def tupleize_single(iterable: Iterable[T]) -> list[tuple[T]]:
        return [(x,) for x in iterable]



class AsyncRunner:
    event_loop: Optional[AbstractEventLoop] = None
    termination_signal_handler: Optional[TerminationSignalHandler] = None
    
    @classmethod
    def init(cls, termination_signal_handler: Optional[TerminationSignalHandler] = None):
        cls.termination_signal_handler = termination_signal_handler

    @classmethod
    def run(cls, coroutine_object: CoroutineObject):
        async def wrap():
            cls.event_loop = asyncio.get_running_loop()

            if cls.termination_signal_handler:
                cls.event_loop.add_signal_handler(signal.SIGINT, cls.termination_signal_handler)
                cls.event_loop.add_signal_handler(signal.SIGTERM, cls.termination_signal_handler)

            return await coroutine_object

        asyncio.run(wrap())
    
    @classmethod
    def schedule(cls, coroutine_object: CoroutineObject) -> Future:
        """
        Schedules a task for execution, but doesn't necessarily execute it immediately
        """
        return asyncio.run_coroutine_threadsafe(coroutine_object, loop=cls.event_loop)

    @classmethod
    def schedule_and_wait(cls, coroutine_object: CoroutineObject) -> Any:
        return cls.schedule(coroutine_object).result()



class AsyncTasks:
    def __init__(self, *coroutine_objects: CoroutineObject, concurrent=False):
        self.coroutine_objects: Iterator[CoroutineObject] = iter(coroutine_objects)
        self.task: Optional[Task] = None
        self.are_concurrent = concurrent

    async def run(self):
        async def wrap():
            try:
                for coroutine_objects in (
                        [self.coroutine_objects]
                        if self.are_concurrent else
                        [[x] for x in self.coroutine_objects]
                ):
                    async with TaskGroup() as g:
                        for x in coroutine_objects: g.create_task(x)

            except asyncio.CancelledError:
                logger.debug('Tasks or some task were cancelled')

        self.task = asyncio.create_task(wrap())
        await self.task

    async def stop(self):
        self.task.cancel()
        await self.task



class AsyncPollingTasks(AsyncTasks):
    def __init__(self, *coroutines_and_poll_intervals: tuple[Coroutine, Timedelta]):
        super().__init__(
            *[
                self._wrap_task_with_polling(x, y)
                for x, y in coroutines_and_poll_intervals
            ],
            concurrent=True,
        )

    @staticmethod
    async def _wrap_task_with_polling(coroutine, poll_interval):
        while True:
            start = Time.monotonic()
            await coroutine()
            await asyncio.sleep(max(poll_interval.total_seconds() - (Time.monotonic() - start), 0))
