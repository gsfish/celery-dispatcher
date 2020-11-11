# celery-dispatcher

An extension for celery to dispatch a large amount of sub-tasks within a main task, and process the results separately. 

## Installation

```
pip install celery-dispatcher
```

## Usage

> NOTICE: `celery-dispatcher` use tls to store running info, so the pool implementation using coroutines(like eventlet/gevent) can not be used

Fistly, yield subtask and its parameters in the main task by following order:

1. task: signature
2. args: tupple/list
3. kwargs: dict
4. options: dict

Then register the result handler for each subtask using signal:

```
from celery import shared_task
from celery_dispatcher import dispatch
from celery_dispatcher.signals import subtask_success

@shared_task
def sqrt(i):
    return i * i

@dispatch
@shared_task
def calc():
    for i in range(10):
        yield sqrt, (i,)

@subtask_success.connect(sender=calc)
def handle_result(root_id, task_id, retval, **kwargs):
    print(retval)
```

Or register in the decorator directly:

```
from celery import shared_task
from celery_dispatcher import dispatch

@shared_task
def sqrt(i):
    return i * i

def handle_result(root_id, task_id, retval, **kwargs):
    print(retval)

@dispatch(receiver=handle_result)
@shared_task
def calc():
    for i in range(10):
        yield sqrt, (i,)
```

## General settings

### dispatcher_result_backend

Default: No result backend enabled by default.

The backend used to store subtask info. Can be one of the following:

- redis: Use [Redis](https://redis.io/) to store the results. See [Redis backend settings](https://docs.celeryproject.org/en/stable/userguide/configuration.html#conf-redis-result-backend).

### dispatcher_batch_size

Default: 1000

The batch size of subtask dispatching, or the result retrieving.

### dispatcher_subtask_timeout

Default: 3600

The default timeout in seconds before `celery-dispatcher` gives up retrieving the result of each subtask.

### dispatcher_progress_update_frequency

Default: 1

The number of steps to record. `celery-dispatcher` only actually store the updated progress in the background at most every N steps.
