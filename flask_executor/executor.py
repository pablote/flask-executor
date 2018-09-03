import concurrent.futures
import sys

from flask import current_app

from flask_executor.scheduler import Scheduler


workers_multiplier = {
    'thread': 1,
    'process': 5
}


def default_workers(executor_type):
    if sys.version_info.major == 3 and sys.version_info.minor in (3, 4):
        try:
            from multiprocessing import cpu_count
        except ImportError:
            def cpu_count():
                return None
        return (cpu_count() or 1) * workers_multiplier[executor_type]
    return None


def with_app_context(fn, app):
    def wrapper(*args, **kwargs):
        with app.app_context():
            return fn(*args, **kwargs)
    return wrapper


class ExecutorJob:

    def __init__(self, executor, fn):
        self.executor = executor
        self.fn = fn

    def submit(self, *args, **kwargs):
        future = self.executor.submit(self.fn, *args, **kwargs)
        return future

    def delay(self, delay, *args, **kwargs):
        future = self.executor.delay(self.fn, delay, *args, **kwargs)
        return future


class Executor:

    def __init__(self, app=None):
        self.app = app
        self._executor = None
        self._scheduler = None
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        app.config.setdefault('EXECUTOR_TYPE', 'thread')
        executor_type = app.config['EXECUTOR_TYPE']
        executor_max_workers = default_workers(executor_type)
        app.config.setdefault('EXECUTOR_MAX_WORKERS', executor_max_workers)
        self._executor = self._make_executor(app)
        self._scheduler = Scheduler(self)
        app.extensions['executor'] = self

    def _make_executor(self, app):
        executor_type = app.config['EXECUTOR_TYPE']
        executor_max_workers = app.config['EXECUTOR_MAX_WORKERS']
        if executor_type == 'thread':
            _executor = concurrent.futures.ThreadPoolExecutor
        elif executor_type == 'process':
            _executor = concurrent.futures.ProcessPoolExecutor
        else:
            raise ValueError("{} is not a valid executor type.".format(executor_type))
        return _executor(max_workers=executor_max_workers)

    def _prepare_fn(self, fn):
        if isinstance(self._executor, concurrent.futures.ThreadPoolExecutor):
            fn = with_app_context(fn, current_app._get_current_object())
        return fn

    def submit(self, fn, *args, **kwargs):
        fn = self._prepare_fn(fn)
        return self._executor.submit(fn, *args, **kwargs)

    def delay(self, fn, delay, *args, **kwargs):
        fn = self._prepare_fn(fn)
        return self._scheduler.add(fn, delay, *args, **kwargs)

    def job(self, fn):
        if isinstance(self._executor, concurrent.futures.ProcessPoolExecutor):
            raise TypeError("Can't decorate {}: Executors that use multiprocessing "
                            "don't support decorators".format(fn))
        return ExecutorJob(executor=self, fn=fn)



