import threading
import time

from datetime import timedelta
from functools import wraps, update_wrapper, partial
from typing import Iterator, Generator

from celery import _state, current_app, current_task, group
from celery.exceptions import TimeoutError as CeleryTimeoutError
from celery.result import AsyncResult, GroupResult, ResultSet
from celery.utils.abstract import CallableSignature, CallableTask

from .backends import default_backend
from .mixins import LoggerMixin
from .states import PROGRESS
from .signals import subtask_success
from .utils import gen_chunk, total_size, is_uuid_format


class TaskThread(threading.Thread):

    def __init__(self, _locals=None, _local_stacks=None, *args, **kwargs):
        self._stash_states()
        super().__init__(*args, **kwargs)

    def _stash_states(self):
        self._tls_copy = {}
        for name in _state._tls.__dict__:
            if name.startswith('__'):
                continue
            value = threading.local.__getattribute__(_state._tls, name)
            self._tls_copy[name] = value

        self._task_stack = _state._task_stack.stack
        self._task_request_stacks = []
        for task in _state._task_stack.stack:
            request_stack = task.request_stack.stack
            self._task_request_stacks.append(request_stack)

    def _restore_states(self):
        for name, value in self._tls_copy.items():
            threading.local.__setattr__(_state._tls, name, value)

        for task, request_stack in zip(self._task_stack, self._task_request_stacks):
            for request in request_stack:
                task.request_stack.push(request)
            _state._task_stack.push(task)

    def run(self):
        self._restore_states()
        super().run()


class ProgressManager(LoggerMixin):

    def __init__(self, progress_update_frequency=None):
        self._progress_update_frequency = progress_update_frequency
        self._lock = threading.RLock()
        # initialize states of progress
        self._start_time = None
        self._completed_count = 0
        self._completed_last_update_count = 0
        self._total_count = 0
        self._total_last_update_count = 0

    def init_progress(self):
        self._start_time = time.time()
        self._completed_count = 0
        self._completed_last_update_count = 0
        self._total_count = 0
        self._total_last_update_count = 0

    def update_progress_completed(self, amount: int = 1):
        with self._lock:
            self._completed_count += amount
            self._update_progress(self._completed_count, self._total_count, self._progress_update_frequency)

    def update_progress_total(self, amount: int = 1):
        with self._lock:
            self._total_count += amount
            self._update_progress(self._completed_count, self._total_count, self._progress_update_frequency)

    def _update_progress(self, completed_count=None, total_count=None, update_frequency=1):
        """
        Update the task backend with both an estimated percentage complete and
        number of seconds remaining until completion.
        ``completed_count`` Number of task "units" that have been completed out
        of ``total_count`` total "units."
        ``update_frequency`` Only actually store the updated progress in the
        background at most every ``N`` ``completed_count``.

        Refer to:
        https://github.com/PolicyStat/jobtastic/blob/master/jobtastic/task.py
        """
        if (not update_frequency) or (completed_count - self._completed_last_update_count < update_frequency and
                                      total_count - self._total_last_update_count < update_frequency):
            # We've updated the progress too recently. Don't stress out the
            # result backend
            return
        # Store progress for display
        progress_percent, time_remaining = self._calc_progress(completed_count, total_count)
        self.logger.debug("Updating progress: %s percent, %s remaining", progress_percent, time_remaining)
        if current_task.request.id:
            self._completed_last_update_count = completed_count
            self._total_last_update_count = total_count
            current_task.update_state(None, PROGRESS, meta={
                "completed_count": completed_count,
                "total_count": total_count,
                "progress_percent": progress_percent,
                "time_remaining": time_remaining,
            })

    def _calc_progress(self, completed_count, total_count):
        """
        Calculate the percentage progress and estimated remaining time based on
        the current number of items completed of the total.
        Returns a tuple of ``(percentage_complete, seconds_remaining)``.

        Refer to:
        https://github.com/PolicyStat/jobtastic/blob/master/jobtastic/task.py
        """
        current_time = time.time()
        time_spent = current_time - self._start_time
        self.logger.debug("Progress time spent: %s", time_spent)

        if total_count == 0:
            return 100, 0
        elif completed_count == 0:
            return 0, -1

        completion_fraction = completed_count / total_count
        total_time = time_spent / completion_fraction
        time_remaining = total_time - time_spent

        completion_display = completion_fraction * 100
        if completion_display == 100:
            return 100, 0

        return completion_display, time_remaining


class dispatch(LoggerMixin, threading.local):

    def __init__(self, *args, **kwargs):
        # initialize configuration
        self._result_backend = current_app.conf.get('dispatcher_result_backend')
        self._chunk_size = current_app.conf.get('dispatcher_chunk_size', 1000)
        self._subtask_timeout = current_app.conf.get('dispatcher_subtask_timeout', 60*60)
        self._result_expires = current_app.conf.get('result_expires')
        if isinstance(self._result_expires, timedelta):
            self._result_expires = self._result_expires.total_seconds()

        # initialize dispatching
        progress_update_frequency = current_app.conf.get('dispatcher_progress_update_frequency', 1)
        self._progress_manager = ProgressManager(progress_update_frequency)
        self._dispatch_finished = threading.Event()
        self._backend = None  # set in _create_task_wrapper

        if len(args) == 1 and callable(args[0]):
            self._wrapped_directly = True
            task = args[0]
            self._wrapper = self._create_task_wrapper()
            self._wrapped_task = self._wrapper(task)
            # patch self to act as the task function
            update_wrapper(self, task)
            origin_call = self.__call__
            self.__call__ = partial(origin_call, self)
            self.__call__ = wraps(origin_call)(self.__call__)  # partial miss to copy some attributes from the original function
        else:
            self._wrapped_directly = False
            self._wrapper = self._create_task_wrapper(*args, **kwargs)
            self._wrapped_task = None

    def __call__(self, *args, **kwargs):
        if self._wrapped_directly:
            return self._wrapped_task(*args, **kwargs)
        else:
            task = args[0]
            self._wrapped_task = self._wrapper(task)
            return self._wrapped_task

    def _create_task_wrapper(self, options=None, receiver=None, backend=None):
        def wrapper(task):
            @wraps(task)
            def wrapped_task(*args, **kwargs):
                # start dispatching
                gen_task = task(*args, **kwargs)
                if not isinstance(gen_task, Generator):
                    raise RuntimeError('Task should be generator: {0!r}'.format(task))

                self._dispatch_from_task(gen_task, **options)
                # start collecting
                self._collect_from_task(receiver, auto_ignore=True)

            return wrapped_task

        if not options:
            options = {}

        if backend:
            self._backend = backend(self._result_backend)
        else:
            self._backend = default_backend(self._result_backend)

        return wrapper

    @property
    def _dispatch_key(self):
        root_id = current_task.request.root_id
        return 'celery-task-meta-{0}:subtask'.format(root_id)

    def _dispatch_from_task(self, *tasks, **options):
        if len(tasks) == 1:
            tasks = tasks[0]

        if isinstance(tasks, group):
            tasks = tasks.tasks
        elif isinstance(tasks, CallableSignature):
            tasks = [tasks.clone()]

        # TODO: waiting for celery 4.5 to use the generator-friendly group()/chord()
        # refer to the pr: https://github.com/celery/celery/pull/4459
        #
        # for now, this way cost 2 times the memory of tasks list in the following structure:
        # `group.tasks` and `GroupResult`
        #
        # results = group(task for task in tasks)(**options)
        # self._handle_results(results, self.receive_result)

        self._progress_manager.init_progress()
        self._dispatch_finished.clear()
        self.logger.info('Start dispatching')

        dispatch_thread = TaskThread(
            target=self._store_sub_task_results,
            args=(self._progress_manager, self._dispatch_finished, tasks,),
            kwargs=options)
        dispatch_thread.start()
        self.logger.debug('Dispatching sub-tasks: %r, options: %r', tasks, options)

    def _store_sub_task_results(self, progress_manager, dispatch_finished, tasks, **options):
        for results in gen_chunk(self._apply_tasks(tasks, **options), self._chunk_size):
            self.logger.debug('%s tasks have been applied', len(results))
            progress_manager.update_progress_total(len(results))
            self._backend.bulk_push(self._dispatch_key, map(lambda r: r.task_id, results), self._result_expires)

        time.sleep(0)
        dispatch_finished.set()
        self.logger.info('Finished dispatching')

    def _apply_tasks(self, tasks, **options) -> Iterator[AsyncResult]:
        with current_app.producer_or_acquire() as producer:
            for yield_task in tasks:
                task, args, kwargs, task_options = self._parse_yield_task(yield_task)
                _options = options.copy()
                _options.update(task_options)
                result = task.apply_async(args=args, kwargs=kwargs, producer=producer, add_to_parent=False, **_options)
                task_id = result.task_id
                self.logger.debug('Task %s applied', task_id)
                yield result

    def _parse_yield_task(self, yield_task):
        task = None
        args = ()
        kwargs = {}
        options = {}
        if isinstance(yield_task, CallableTask):
            task = yield_task
        elif isinstance(yield_task, (tuple, list)):
            if len(yield_task) == 1:
                task = yield_task[0]
            elif len(yield_task) == 2:
                task, args = yield_task
            elif len(yield_task) == 3:
                task, args, kwargs = yield_task
            elif len(yield_task) == 4:
                task, args, kwargs, options = yield_task

        if not task:
            raise ValueError('Invalid yield task: {0!r}'.format(yield_task))

        return task, args, kwargs, options

    def _collect_from_task(self, receiver, auto_ignore: bool = True):
        self.logger.info('Start collecting')
        for result in self._gen_sub_result():
            try:
                self._handle_results(result, receiver, auto_ignore=auto_ignore)
            except CeleryTimeoutError:
                self.logger.exception('Results timeout expired, task_id: %s', result.task_id)

        self.logger.info('Finished collecting')

    def _gen_sub_result(self, interval=0.5) -> Iterator[AsyncResult]:
        while True:
            task_id = self._backend.pop(self._dispatch_key)
            if not task_id:
                if self._dispatch_finished.is_set():
                    return
                time.sleep(interval)
                continue
            result = current_app.AsyncResult(task_id.decode('utf-8'))
            yield result

    def _handle_results(self, results, receiver, other_worker=False, auto_ignore=False, timeout=None, interval=0.5,
                        on_interval=None, propagate=False, disable_sync_subtasks=False):
        callback = partial(self._on_task_finished, receiver=receiver, auto_ignore=auto_ignore)
        timeout = timeout or self._subtask_timeout
        self.logger.debug('Waiting for the results, task_id: %s, timeout: %s', results, timeout)
        if other_worker:
            self.logger.debug('Task is not ready on other worker, task_id: %s', results.task_id)
            self._wait_until_ready(results, timeout)

        if isinstance(results, AsyncResult):
            result = results.get(
                timeout=timeout,
                interval=interval,
                on_interval=on_interval,
                propagate=propagate,
                disable_sync_subtasks=disable_sync_subtasks)
            callback(results.task_id, result)
        elif isinstance(results, ResultSet):
            results.get(
                callback=callback,
                timeout=timeout,
                interval=interval,
                on_interval=on_interval,
                propagate=propagate,
                disable_sync_subtasks=disable_sync_subtasks)
        else:
            raise ValueError('Invalid results type: {0!r}'.format(results))

        if auto_ignore:
            results.forget()

    def _wait_until_ready(self, result, timeout=None, interval=0.5):
        time_start = time.monotonic()
        self.logger.debug('Waiting for the task, task_id: %s, timeout: %s', result.task_id, timeout)
        while not result.ready():
            if timeout and (time.monotonic() - time_start >= timeout):
                raise TimeoutError('Failed to wait for the result, task_id: %s', result.task_id)
            time.sleep(interval)

    def _on_task_finished(self, task_id, value, receiver, auto_ignore=False):
        if isinstance(value, Exception):
            self.logger.error('Failed to save result, task_id: %s', task_id, exc_info=value)

        elif isinstance(value, GroupResult):
            self.logger.debug('Received GroupResult, task_id: %s', task_id)
            self._progress_manager.update_progress_total(len(value))
            self._handle_results(value, receiver=receiver, auto_ignore=auto_ignore)

        elif self._is_groupresult_meta(value):
            self.logger.debug('Received GroupResult META, task_id: %s', task_id)
            for results in gen_chunk(self._gen_result_from_groupresult_meta(value), self._chunk_size):
                self._progress_manager.update_progress_total(len(results))
                for result in results:
                    self._handle_results(result, receiver=receiver, other_worker=True, auto_ignore=auto_ignore)

        else:
            self.logger.debug('Received common result, task_id: %s, size: %d bytes', task_id, total_size(value))

            root_id = current_task.request.root_id
            if subtask_success.receivers:
                task = _state.get_current_task()
                subtask_success.send(sender=task, root_id=root_id, task_id=task_id, retval=value)
            if receiver:
                try:
                    receiver(root_id=root_id, task_id=task_id, retval=value)
                except Exception as exc:
                    self.logger.exception('Failed to save result, task_id: %s', task_id)

        self._progress_manager.update_progress_completed()

    def _is_groupresult_meta(self, value):
        # get this result when result serializer is `json`
        # refer to https://docs.celeryproject.org/en/latest/_modules/celery/result.html#GroupResult.as_tuple
        try:
            return isinstance(value, list) and len(value) == 2 and \
                   isinstance(value[0][0], str) and is_uuid_format(value[0][0])
        except Exception as exc:
            return False

    def _gen_result_from_groupresult_meta(self, value):
        children_meta = value[1]
        for task_meta in children_meta:
            task_id = task_meta[0][0]
            result = current_app.AsyncResult(task_id)
            yield result
