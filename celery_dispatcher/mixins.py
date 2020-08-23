from celery.utils.log import get_task_logger


class LoggerMixin:
    _logger = None

    @property
    def logger(self):
        if self._logger is None:
            self._logger = get_task_logger(__name__)
        return self._logger
