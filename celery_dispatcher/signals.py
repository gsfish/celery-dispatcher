from celery.utils.dispatch import Signal

subtask_success = Signal(
    name='subtask_success',
    providing_args=['task_id', 'retval'],
)
