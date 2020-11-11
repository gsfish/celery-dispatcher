# celery-dispatcher

An extension for celery to dispatch a large amount of sub-tasks within a main task, and process the results separately. 

## Installation

```
pip install celery-dispatcher
```

## Usage

> NOTICE: celery-dispatcher use tls to store running info, so the pool implementation using coroutines(like eventlet/gevent) can not be used

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
