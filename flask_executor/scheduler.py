from concurrent.futures import Future
from concurrent.futures._base import PENDING
import threading
import time

from sortedcontainers import SortedList


class ScheduledFuture(Future):

    def __init__(self, executor, fn, due, *args, **kwargs):
        self.executor = executor
        self.fn = fn
        self.due = due
        self.args = args
        self.kwargs = kwargs
        super(ScheduledFuture, self).__init__()

    def pending(self):
        with self._condition:
            return self._state == PENDING

    def set_result_from_future(self, future):
        try:
            result = future.result()
        except BaseException as exc:
            self.set_exception(exc)
        else:
            self.set_result(result)

    def submit(self):
        future = self.executor._executor.submit(self.fn, *self.args, **self.kwargs)
        future.add_done_callback(self.set_result_from_future)
        return future


class Scheduler:

    def __init__(self, executor):
        self.executor = executor
        self.scheduled_futures = SortedList(key=lambda sf: sf.due)
        self.scheduler = threading.Thread(target=self.cycle, daemon=True)

    def add(self, fn, delay, *args, **kwargs):
        if not self.scheduler.is_alive():
            self.scheduler.start()
        due = time.time() + delay
        sf = ScheduledFuture(self.executor, fn, due, *args, **kwargs)
        self.scheduled_futures.add(sf)
        return sf

    def cycle(self):
        while True:
            if not self.scheduled_futures:
                time.sleep(0.1)
                continue
            sf = self.scheduled_futures[0]
            if sf.due > time.time():
                continue
            if sf.pending():
                sf.submit()
            self.scheduled_futures.remove(sf)
