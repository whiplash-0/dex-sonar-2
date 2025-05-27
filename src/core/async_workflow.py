import asyncio
import logging
import signal
from asyncio import AbstractEventLoop, Task, TaskGroup
from typing import Any, Callable, Coroutine as GeneralCoroutine, Iterator, Optional

from src.utils import time
from src.utils.time import Timedelta



logger = logging.getLogger(__name__)


CoroutineObject = GeneralCoroutine[Any, Any, None]
Coroutine = Callable[[], CoroutineObject]
TerminationSignalHandler = Callable[[], None]



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
    def schedule(cls, coroutine_object: CoroutineObject):
        """
        Schedules a task for execution, but doesn't necessarily execute it immediately
        """
        asyncio.run_coroutine_threadsafe(coroutine_object, loop=cls.event_loop)



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
            start = time.get_monotonic()
            await coroutine()
            await asyncio.sleep(max(poll_interval.total_seconds() - (time.get_monotonic() - start), 0))
