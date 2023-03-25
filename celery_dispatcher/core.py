import pickle
import threading
import time
import traceback
from datetime import timedelta
from functools import partial, total_ordering, update_wrapper, wraps
from queue import Empty, PriorityQueue
from typing import Generator, Iterator

from billiard.exceptions import SoftTimeLimitExceeded
from celery import _state, current_app, current_task, group
from celery.exceptions import TimeoutError as CeleryTimeoutError
from celery.result import AsyncResult, GroupResult, ResultSet
from celery.utils.abstract import CallableSignature, CallableTask

from .backends import DEFAULT_BACKEND
from .mixins import LoggerMixin
from .signals import subtask_success
from .states import PROGRESS
from .utils import gen_chunk, is_uuid_format, total_size


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

    def update_progress_completed(self, amount: int = 1, callback=None):
        with self._lock:
            self._completed_count += amount
            self._update_progress(self._completed_count, self._total_count, self._progress_update_frequency)

        if callback:
            callback()

    def update_progress_total(self, amount: int = 1, callback=None):
        with self._lock:
            self._total_count += amount
            self._update_progress(self._completed_count, self._total_count, self._progress_update_frequency)

        if callback:
            callback()

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
        self.logger.debug('Updating progress: %s percent, %s remaining', progress_percent, time_remaining)
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
        self.logger.debug('Progress time spent: %s', time_spent)

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


@total_ordering
class PrioritizedItem:
    def __init__(self, priority, item):
        self.timestamp = int(time.time())
        self.priority = priority
        self.item = item

    def __lt__(self, other):
        return self.priority < other.priority

    def __eq__(self, other):
        return self.priority == other.priority


class dispatch(LoggerMixin, threading.local):

    def __init__(self, *args, **kwargs):
        # initialize configuration
        self._result_backend = current_app.conf.get('dispatcher_result_backend')
        self._batch_size = current_app.conf.get('dispatcher_batch_size', 100)
        self._poll_size = current_app.conf.get('dispatcher_poll_size', 100)
        self._poll_timeout = current_app.conf.get('dispatcher_poll_timeout', 10)
        self._subtask_timeout = current_app.conf.get('dispatcher_subtask_timeout', 60 * 60)
        self._failure_on_subtask_timeout = current_app.conf.get('dispatcher_failure_on_subtask_timeout', False)
        self._failure_on_subtask_exception = current_app.conf.get('dispatcher_failure_on_subtask_exception', False)
        self._result_expires = current_app.conf.get('result_expires')
        if isinstance(self._result_expires, timedelta):
            self._result_expires = self._result_expires.total_seconds()

        # initialize dispatching
        progress_update_frequency = current_app.conf.get('dispatcher_progress_update_frequency', 1)
        self._progress_manager = ProgressManager(progress_update_frequency)
        self._backend = None  # set in _create_task_wrapper
        self._auto_ignore = True

        if len(args) == 1 and callable(args[0]):
            self._wrapped_directly = True
            task = args[0]
            self._wrapper = self._create_task_wrapper()
            self._wrapped_task = self._wrapper(task)
            # patch self to act as the task function
            update_wrapper(self, task)
            origin_call = self.__call__
            self.__call__ = partial(origin_call, self)
            # partial miss to copy some attributes from the original function
            self.__call__ = wraps(origin_call)(self.__call__)
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

    def _create_task_wrapper(self, options=None, receiver=None, backend=None, auto_ignore=True):
        def wrapper(task):
            @wraps(task)
            def wrapped_task(*args, **kwargs):
                try:
                    gen_task = task(*args, **kwargs)
                    if not isinstance(gen_task, Generator):
                        raise RuntimeError('Task should be generator: {0!r}'.format(task))

                    self._progress_manager.init_progress()
                    stash_finished = threading.Event()
                    restore_finished = threading.Event()
                    result_queue = PriorityQueue(self._poll_size)
                    result_queue_free = threading.Semaphore(self._poll_size)

                    self._dispatch_from_task(stash_finished, gen_task, **options)
                    self._collect_from_task(stash_finished, restore_finished, result_queue, result_queue_free, receiver)
                except SoftTimeLimitExceeded as err:
                    self._revoke_from_task(stash_finished, restore_finished, result_queue, result_queue_free)
                    raise err

            return wrapped_task

        if not options:
            options = {}

        if backend:
            self._backend = backend(self._result_backend)
        else:
            self._backend = DEFAULT_BACKEND(self._result_backend)

        self._auto_ignore = auto_ignore

        return wrapper

    @property
    def _dispatch_key(self):
        root_id = current_task.request.root_id
        return 'celery-task-meta-{0}:subtask'.format(root_id)

    def _dispatch_from_task(self, stash_finished, *tasks, **options):
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
        # self._handle_result(results, self.receive_result)

        self.logger.debug('Dispatching subtasks: %r, options: %r', tasks, options)
        stash_thread = TaskThread(
            target=self._stash_subtask_results,
            args=(self._progress_manager, stash_finished, tasks,),
            kwargs=options)
        stash_thread.start()

    def _stash_subtask_results(self, progress_manager, stash_finished, tasks, **options):
        self.logger.debug('Stashing started')
        self._backend.delete(self._dispatch_key)
        for results in gen_chunk(self._apply_tasks(tasks, **options), self._batch_size):
            self.logger.debug('%s subtasks have been applied', len(results))
            progress_manager.update_progress_total(len(results))
            self._backend.bulk_push(self._dispatch_key, map(pickle.dumps, results), self._result_expires)

        stash_finished.set()
        self.logger.debug('Stashing finished')

    def _apply_tasks(self, tasks, **common_options) -> Iterator[AsyncResult]:
        with current_app.producer_or_acquire() as producer:
            for task_info in tasks:
                task, args, kwargs, task_options = self._parse_task_info(task_info)
                options = common_options.copy()
                options.update(task_options)
                result = task.apply_async(args=args, kwargs=kwargs, producer=producer, add_to_parent=False, **options)
                if result.parent:
                    result_ids = []
                    last_result = result
                    while last_result:
                        result_ids.append(last_result.id)
                        last_result = last_result.parent

                    self.logger.debug('Subtask applied by chain: %s', ' | '.join(result_ids[::-1]))
                else:
                    self.logger.debug('Subtask %s applied', result.task_id)

                yield result

    def _parse_task_info(self, task_info):
        task = None
        args = ()
        kwargs = {}
        options = {}
        if isinstance(task_info, CallableTask):
            task = task_info
        elif isinstance(task_info, (tuple, list)):
            if len(task_info) == 1:
                task = task_info[0]
            elif len(task_info) == 2:
                task, args = task_info
            elif len(task_info) == 3:
                task, args, kwargs = task_info
            elif len(task_info) == 4:
                task, args, kwargs, options = task_info

        if not task:
            raise ValueError('Invalid task info: {0!r}'.format(task_info))

        return task, args, kwargs, options

    def _collect_from_task(self, stash_finished, restore_finished, result_queue, result_queue_free, receiver):
        self.logger.debug('Collecting started')
        restore_thread = TaskThread(
            target=self._restore_subtask_results,
            args=(stash_finished, restore_finished, result_queue, result_queue_free))
        restore_thread.start()

        while True:
            try:
                result_item = result_queue.get(timeout=self._poll_timeout)
                timestamp = result_item.timestamp
                priority = result_item.priority
                result = result_item.item
                self.logger.debug(
                    'During collecting, get result_item from result_queue, subtask_id: %s, priority: %s, timestamp: %d, qsize: %d, free: %d',
                    result.task_id, time.strftime('%Y%m%d%H%M%S', time.localtime(priority)), timestamp,
                    result_queue.qsize(), result_queue_free._value)
            except Empty:
                if restore_finished.is_set():
                    break

                continue

            if restore_finished.is_set():
                timeout = self._subtask_timeout
                handle_timeout_result = True
            else:
                timeout = self._poll_timeout
                handle_timeout_result = False

            try:
                self._handle_result(result, receiver, timeout=timeout, on_finish=result_queue_free.release)
            except SoftTimeLimitExceeded as err:
                result_queue.put(result_item)
                self.logger.debug(
                    'During collecting, put result_item back to result_queue, subtask_id: %s, priority: %s, timestamp: %d, qsize: %d, free: %d',
                    result.task_id, time.strftime('%Y%m%d%H%M%S', time.localtime(result_item.priority)), timestamp,
                    result_queue.qsize(), result_queue_free._value)
                raise err
            except CeleryTimeoutError as err:
                if handle_timeout_result or (time.time() - timestamp) >= self._subtask_timeout:
                    self.logger.error('Subtask timeout, subtask_id: %s', result.task_id)
                    if self._failure_on_subtask_timeout:
                        raise err

                    self._progress_manager.update_progress_completed(callback=result_queue_free.release)
                else:
                    result_item.priority += self._poll_timeout
                    result_queue.put(result_item)
                    self.logger.debug(
                        'During collecting, put result_item back to result_queue, subtask_id: %s, priority: %s, timestamp: %d, qsize: %d, free: %d',
                        result.task_id, time.strftime('%Y%m%d%H%M%S', time.localtime(result_item.priority)), timestamp,
                        result_queue.qsize(), result_queue_free._value)
            except Exception as err:
                self.logger.error('Subtask raised exception, subtask_id: %s', result.task_id)
                self.logger.error(traceback.format_exc())
                if self._failure_on_subtask_exception:
                    raise err

                self._progress_manager.update_progress_completed(callback=result_queue_free.release)

        self.logger.debug('Collecting finished')

    def _restore_subtask_results(self, stash_finished, restore_finished, result_queue, result_queue_free):
        self.logger.debug('Restoring started')
        while True:
            result = self._backend.pop(self._dispatch_key)
            if not result:
                if stash_finished.is_set():
                    break

                time.sleep(self._poll_timeout)
                continue

            priority = time.time()
            result = pickle.loads(result)
            result_item = PrioritizedItem(priority, result)

            result_queue_free.acquire()
            result_queue.put(result_item)
            self.logger.debug(
                'During restoring, put result to result_queue, subtask_id: %s, priority: %s, timestamp: %d, qsize: %d, free: %d',
                result.task_id, time.strftime('%Y%m%d%H%M%S', time.localtime(priority)), result_item.timestamp,
                result_queue.qsize(), result_queue_free._value)

        restore_finished.set()
        self.logger.debug('Restoring finished')

    def _handle_result(self, result, receiver, other_worker=False, timeout=None, interval=0.5, on_interval=None,
                       propagate=True, disable_sync_subtasks=False, on_finish=None):
        callback = partial(self._on_task_finished, receiver=receiver, callback=on_finish)
        self.logger.debug('Waiting for the result, subtask_id: %s, timeout: %s', result.task_id, timeout)
        if other_worker:
            self.logger.debug('Result is not ready on other worker, subtask_id: %s', result.task_id)
            self._wait_until_ready(result, timeout)

        if isinstance(result, AsyncResult):
            value = result.get(
                timeout=timeout,
                interval=interval,
                on_interval=on_interval,
                propagate=propagate,
                disable_sync_subtasks=disable_sync_subtasks)
            callback(result.task_id, value)
        elif isinstance(result, ResultSet):
            result.get(
                callback=callback,
                timeout=timeout,
                interval=interval,
                on_interval=on_interval,
                propagate=propagate,
                disable_sync_subtasks=disable_sync_subtasks)
        else:
            raise ValueError('Invalid result type: {0!r}'.format(result))

        if self._auto_ignore:
            result.forget()

    def _wait_until_ready(self, result, timeout=None, interval=0.5):
        time_start = time.monotonic()
        self.logger.debug('Waiting for the subtask, subtask_id: %s, timeout: %s', result.task_id, timeout)
        while not result.ready():
            if timeout and (time.monotonic() - time_start >= timeout):
                raise TimeoutError('Failed to wait for the result, subtask_id: %s', result.task_id)
            time.sleep(interval)

    def _on_task_finished(self, task_id, value, receiver, callback=None):
        if isinstance(value, Exception):
            self.logger.error('Failed to save result, subtask_id: %s', task_id, exc_info=value)

        elif isinstance(value, GroupResult):
            self.logger.debug('Received GroupResult, subtask_id: %s', task_id)
            self._progress_manager.update_progress_total(len(value))
            self._handle_result(value, receiver=receiver)

        elif self._is_groupresult_meta(value):
            self.logger.debug('Received GroupResult META, subtask_id: %s', task_id)
            for results in gen_chunk(self._gen_result_from_groupresult_meta(value), self._batch_size):
                self._progress_manager.update_progress_total(len(results))
                for result in results:
                    self._handle_result(result, receiver=receiver, other_worker=True)

        else:
            self.logger.debug('Received common result, subtask_id: %s, size: %d bytes', task_id, total_size(value))

            root_id = current_task.request.root_id
            if subtask_success.receivers:
                task = _state.get_current_task()
                subtask_success.send(sender=task, root_id=root_id, task_id=task_id, retval=value)
            if receiver:
                try:
                    receiver(root_id=root_id, task_id=task_id, retval=value)
                except Exception as exc:
                    self.logger.exception('Failed to save result, subtask_id: %s', task_id)

        self._progress_manager.update_progress_completed(callback=callback)

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

    def _revoke_from_task(self, stash_finished, restore_finished, result_queue, result_queue_free):
        self.logger.debug('Revoking started')
        while True:
            try:
                result_item = result_queue.get(timeout=self._poll_timeout)
                timestamp = result_item.timestamp
                priority = result_item.priority
                result = result_item.item
                self.logger.debug(
                    'During revoking, get result_item from result_queue, subtask_id: %s, priority: %s, timestamp: %d, qsize: %d, free: %d',
                    result.task_id, time.strftime('%Y%m%d%H%M%S', time.localtime(priority)), timestamp,
                    result_queue.qsize(), result_queue_free._value)
            except Empty:
                if stash_finished.is_set() and restore_finished.is_set():
                    break

                continue

            self._revoke_chain(result)
            result_queue_free.release()
            self.logger.debug('Subtask revoked, subtask_id: %s', result.task_id)

        self.logger.debug('Revoking finished')

    def _revoke_chain(self, last_result):
        self.logger.debug('Revoking %s', last_result.task_id)
        last_result.revoke(terminate=True)
        if last_result.parent is not None:
            self._revoke_chain(last_result.parent)
