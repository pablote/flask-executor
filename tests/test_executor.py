import concurrent.futures
import random

from flask import Flask, current_app, g, request
import pytest

from flask_executor import Executor, ExecutorJob


# Reusable functions for tests

def fib(n):
    if n <= 2:
        return 1
    else:
        return fib(n-1) + fib(n-2)

def app_context_test_value(_=None):
    return current_app.config['TEST_VALUE']

def request_context_test_value(_=None):
    return request.test_value

def g_context_test_value(_=None):
    return g.test_value


def test_init(app):
    executor = Executor(app)
    assert 'executor' in app.extensions
    assert isinstance(executor._executor, concurrent.futures._base.Executor)

def test_factory_init(app):
    executor = Executor()
    executor.init_app(app)
    assert 'executor' in app.extensions
    assert isinstance(executor._executor, concurrent.futures._base.Executor)

def test_thread_executor_init(default_app):
    default_app.config['EXECUTOR_TYPE'] = 'thread'
    executor = Executor(default_app)
    assert isinstance(executor._executor, concurrent.futures.ThreadPoolExecutor)

def test_process_executor_init(default_app):
    default_app.config['EXECUTOR_TYPE'] = 'process'
    executor = Executor(default_app)
    assert isinstance(executor._executor, concurrent.futures.ProcessPoolExecutor)

def test_default_executor_init(default_app):
    executor = Executor(default_app)
    assert isinstance(executor._executor, concurrent.futures.ThreadPoolExecutor)

def test_submit(app):
    executor = Executor(app)
    future = executor.submit(fib, 5)
    assert future.result() == fib(5)

def test_max_workers(app):
    EXECUTOR_MAX_WORKERS = 10
    app.config['EXECUTOR_MAX_WORKERS'] = EXECUTOR_MAX_WORKERS
    executor = Executor(app)
    assert executor._executor._max_workers == EXECUTOR_MAX_WORKERS

def test_thread_decorator_submit(default_app):
    default_app.config['EXECUTOR_TYPE'] = 'thread'
    executor = Executor(default_app)
    @executor.job
    def decorated(n):
        return fib(n)
    with default_app.test_request_context(''):
        future = decorated.submit(5)
    assert future.result() == fib(5)

def test_thread_decorator_map(default_app):
    iterable = list(range(5))
    default_app.config['EXECUTOR_TYPE'] = 'thread'
    executor = Executor(default_app)
    @executor.job
    def decorated(n):
        return fib(n)
    with default_app.test_request_context(''):
        results = decorated.map(iterable)
    for i, r in zip(iterable, results):
        assert fib(i) == r

def test_process_decorator(default_app):
    ''' Using decorators should fail with a TypeError when using the ProcessPoolExecutor '''
    default_app.config['EXECUTOR_TYPE'] = 'process'
    executor = Executor(default_app)
    try:
        @executor.job
        def decorated(n):
            return fib(n)
    except TypeError:
        pass
    else:
        assert 0

def test_submit_app_context(app):
    test_value = random.randint(1, 101)
    app.config['TEST_VALUE'] = test_value
    executor = Executor(app)
    future = executor.submit(app_context_test_value)
    assert future.result() == test_value

def test_submit_g_context_process(app):
    test_value = random.randint(1, 101)
    executor = Executor(app)
    with app.test_request_context(''):
        g.test_value = test_value
        future = executor.submit(g_context_test_value)
    assert future.result() == test_value

def test_submit_request_context(app):
    test_value = random.randint(1, 101)
    executor = Executor(app)
    with app.test_request_context(''):
        request.test_value = test_value
        future = executor.submit(request_context_test_value)
    assert future.result() == test_value

def test_map_app_context(app):
    test_value = random.randint(1, 101)
    iterator = list(range(5))
    app.config['TEST_VALUE'] = test_value
    executor = Executor(app)
    with app.test_request_context(''):
        results = executor.map(app_context_test_value, iterator)
    for r in results:
        assert r == test_value

def test_map_g_context_process(app):
    test_value = random.randint(1, 101)
    iterator = list(range(5))
    executor = Executor(app)
    with app.test_request_context(''):
        g.test_value = test_value
        results = executor.map(g_context_test_value, iterator)
    for r in results:
        assert r == test_value

def test_map_request_context(app):
    test_value = random.randint(1, 101)
    iterator = list(range(5))
    executor = Executor(app)
    with app.test_request_context('/'):
        request.test_value = test_value
        results = executor.map(request_context_test_value, iterator)
    for r in results:
        assert r == test_value
