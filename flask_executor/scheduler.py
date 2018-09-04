from concurrent.futures import Future
from concurrent.futures._base import PENDING, CancelledError
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
        self.future = None
        super(ScheduledFuture, self).__init__()

    def pending(self):
        with self._condition:
            return self._state == PENDING

    def running(self):
        if self.future:
            return self.future.running() and self.set_running_or_notify_cancel()
        return super(ScheduledFuture, self).running()

    def set_result_from_future(self, future):
        try:
            result = future.result()
        except BaseException as exc:
            if isinstance(exc, CancelledError):
                super(ScheduledFuture, self).cancel()
            else:
                self.set_exception(exc)
        else:
            self.set_result(result)

    def submit(self):
        if self.future is None:
            self.future = self.executor._executor.submit(self.fn, *self.args, **self.kwargs)
            self.future.add_done_callback(self.set_result_from_future)
        return self.future

    def cancel(self):
        if self.future:
            return self.future.cancel()
        return super(ScheduledFuture, self).cancel()


class Scheduler:

    def __init__(self, executor):
        self.executor = executor
        self.scheduled_futures = SortedList(key=lambda sf: sf.due)
        self.scheduler = threading.Thread(target=self.cycle)
        self.scheduler.daemon = True

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
