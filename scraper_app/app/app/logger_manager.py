import logging
import os


FORMAT_MESSAGE = "%(asctime)s | %(levelname)s | %(name)s | %(threadName)s | %(funcName)s | %(message)s"
FORMAT_DATE = "%Y-%m-%d %H:%M:%S"


class CustomLogger:

    def __init__(self, name=__name__, path=""):
        self.name = name
        self.file_path = path

        self.logger = logging.getLogger(name=self.name)
        self.logger.propagate = False
        self.logger.setLevel(logging.INFO)

        self._formatter = logging.Formatter(fmt=FORMAT_MESSAGE,
                                            datefmt=FORMAT_DATE)

        handler = logging.StreamHandler()
        handler.setFormatter(self._formatter)
        self.logger.addHandler(handler)

    def __del__(self):

        if self.logger:

            for filter_ in self.logger.filters:
                try:
                    self.logger.removeFilter(filter_)
                except:
                    pass
            for handle_ in self.logger.handlers:
                try:
                    self.logger.removeHandler(handle_)
                except:
                    pass
            try:
                del self.logger
            except:
                pass

    def _log(self, level=0, message=""):

        fn, lno, func, sinfo = self.logger.findCaller(stack_info=False, stacklevel=1)

        log_record = self.logger.makeRecord(name=self.name, level=level, fn=fn, lno=lno,
                                            msg=message, args={}, exc_info=None,
                                            func=func,
                                            extra={})

        self.logger.handle(log_record)
        log_result = self._formatter.format(log_record)

        if self.file_path:
            new_line = "\n" if os.path.exists(self.file_path) else ""
            with open(self.file_path, "a") as file_:
                if new_line:
                    file_.write(new_line)
                file_.write(log_result)

        return log_result

    def debug(self, message=""):
        self._log(level=logging.DEBUG, message=message)

    def info(self, message=""):
        self._log(level=logging.INFO, message=message)

    def error(self, message=""):
        self._log(level=logging.ERROR, message=message)

    def warning(self, message=""):
        self._log(level=logging.WARNING, message=message)

    def critical(self, message=""):
        self._log(level=logging.CRITICAL, message=message)
