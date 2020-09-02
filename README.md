# celery-dispatcher

## Installation

```
pip install celery-dispatcher
```

## Usage

```
from celery import 
from celery_dispatcher import dispatch

def handle_result(root_id, task_id, retval, **kwargs):
    print(retval)

@shared_task
def sqrt(i):
    return i * i

@dispatch(receiver=handle_result)
@shared_task
def calc():
    for i in range(10):
        yield sqrt, (i,)
```
