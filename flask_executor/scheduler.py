from concurrent.futures import Future
from concurrent.futures._base import PENDING


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
