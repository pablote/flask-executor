import concurrent.futures
import time

from flask import Flask
import pytest

from flask_executor import Executor
from flask_executor.executor import ExecutorJob
from flask_executor.scheduler import ScheduledFuture


EXECUTOR_MAX_WORKERS = 10

def fib(n):
    if n <= 2:
        return 1
    else:
        return fib(n-1) + fib(n-2)


def test_init(app):
    executor = Executor(app)
    assert 'executor' in app.extensions

def test_factory_init(app):
    executor = Executor()
    executor.init_app(app)
    assert 'executor' in app.extensions

def test_default_executor(app):
    executor = Executor(app)
    with app.app_context():
        executor.submit(fib, 5)
        assert isinstance(executor._executor, concurrent.futures.ThreadPoolExecutor)

def test_thread_executor(app):
    app.config['EXECUTOR_TYPE'] = 'thread'
    executor = Executor(app)
    with app.app_context():
        executor.submit(fib, 5)
        assert isinstance(executor._executor, concurrent.futures.ThreadPoolExecutor)

def test_process_executor(app):
    app.config['EXECUTOR_TYPE'] = 'process'
    executor = Executor(app)
    with app.app_context():
        executor.submit(fib, 5)
        assert isinstance(executor._executor, concurrent.futures.ProcessPoolExecutor)

def test_submit_result(app):
    executor = Executor(app)
    with app.app_context():
        future = executor.submit(fib, 5)
        assert isinstance(future, concurrent.futures.Future)
        assert future.result() == fib(5)

def test_thread_workers(app):
    app.config['EXECUTOR_TYPE'] = 'thread'
    app.config['EXECUTOR_MAX_WORKERS'] = EXECUTOR_MAX_WORKERS
    executor = Executor(app)
    with app.app_context():
        executor.submit(fib, 5)
        assert executor._executor._max_workers == EXECUTOR_MAX_WORKERS

def test_process_workers(app):
    app.config['EXECUTOR_TYPE'] = 'process'
    app.config['EXECUTOR_MAX_WORKERS'] = EXECUTOR_MAX_WORKERS
    executor = Executor(app)
    with app.app_context():
        executor.submit(fib, 5)
        assert executor._executor._max_workers == EXECUTOR_MAX_WORKERS

def test_thread_decorator(app):
    app.config['EXECUTOR_TYPE'] = 'thread'
    executor = Executor(app)
    @executor.job
    def decorated(n):
        return fib(n)
    assert isinstance(decorated, ExecutorJob)
    with app.app_context():
        future = decorated.submit(5)
        assert isinstance(future, concurrent.futures.Future)
        assert future.result() == fib(5)

def test_process_decorator(app):
    ''' Using decorators should fail with a TypeError when using the ProcessPoolExecutor '''
    app.config['EXECUTOR_TYPE'] = 'process'
    executor = Executor(app)
    try:
        @executor.job
        def decorated(n):
            return fib(n)
    except TypeError:
        pass
    else:
        assert 0

def test_delay_thread_executor(app):
    app.config['EXECUTOR_TYPE'] = 'thread'
    executor = Executor(app)
    with app.app_context():
        DELAY = 2
        future = executor.delay(fib, DELAY, 5)
        time.sleep(DELAY / 1.25)
        assert future.pending() == True
        assert future.result() == fib(5)
        assert future.pending() == False
        assert isinstance(future, concurrent.futures.Future)
        assert isinstance(future, ScheduledFuture)

def test_delay_process_executor(app):
    app.config['EXECUTOR_TYPE'] = 'process'
    app.config['EXECUTOR_MAX_WORKERS'] = EXECUTOR_MAX_WORKERS
    executor = Executor(app)
    with app.app_context():
        DELAY = 2
        future = executor.delay(fib, DELAY, 5)
        time.sleep(DELAY / 1.25)
        assert future.pending() == True
        assert future.result() == fib(5)
        assert future.pending() == False
        assert isinstance(future, concurrent.futures.Future)
        assert isinstance(future, ScheduledFuture)

def test_delay_thread_decorator(app):
    app.config['EXECUTOR_TYPE'] = 'thread'
    executor = Executor(app)
    @executor.job
    def decorated(n):
        return fib(n)
    with app.app_context():
        DELAY = 2
        future = decorated.delay(DELAY, 5)
        time.sleep(DELAY / 1.25)
        assert future.pending() == True
        assert future.result() == fib(5)
        assert future.pending() == False
        assert isinstance(future, concurrent.futures.Future)
        assert isinstance(future, ScheduledFuture)

def test_repeat_thread_executor(app):
    app.config['EXECUTOR_TYPE'] = 'thread'
    executor = Executor(app)
    with app.app_context():
        REPEAT = 10
        DELAY = 0.2
        futures = executor.repeat(fib, REPEAT, DELAY, 5)
        assert len(futures) == 10
        time.sleep(REPEAT * DELAY / 1.25)
        assert any(future.pending() for future in futures)
        assert not all(future.pending() for future in futures)
        assert all(future.result() == fib(5) for future in futures)
        assert not any(future.pending() for future in futures)
        assert all(isinstance(future, concurrent.futures.Future) for future in futures)
        assert all(isinstance(future, ScheduledFuture) for future in futures)

def test_repeat_process_executor(app):
    app.config['EXECUTOR_TYPE'] = 'process'
    app.config['EXECUTOR_MAX_WORKERS'] = EXECUTOR_MAX_WORKERS
    executor = Executor(app)
    with app.app_context():
        REPEAT = 10
        DELAY = 0.2
        futures = executor.repeat(fib, REPEAT, DELAY, 5)
        assert len(futures) == 10
        time.sleep(REPEAT * DELAY / 1.25)
        assert any(future.pending() for future in futures)
        assert not all(future.pending() for future in futures)
        assert all(future.result() == fib(5) for future in futures)
        assert not any(future.pending() for future in futures)
        assert all(isinstance(future, concurrent.futures.Future) for future in futures)
        assert all(isinstance(future, ScheduledFuture) for future in futures)

def test_repeat_thread_decorator(app):
    app.config['EXECUTOR_TYPE'] = 'thread'
    executor = Executor(app)
    @executor.job
    def decorated(n):
        return fib(n)
    with app.app_context():
        REPEAT = 10
        DELAY = 0.2
        futures = decorated.repeat(REPEAT, DELAY, 5)
        assert len(futures) == 10
        time.sleep(REPEAT * DELAY / 1.25)
        assert any(future.pending() for future in futures)
        assert not all(future.pending() for future in futures)
        assert all(future.result() == fib(5) for future in futures)
        assert not any(future.pending() for future in futures)
        assert all(isinstance(future, concurrent.futures.Future) for future in futures)
        assert all(isinstance(future, ScheduledFuture) for future in futures)
