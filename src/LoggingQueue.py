import logging


class LoggingQueue:
    """Queue-like sink for single-process logging/progress updates."""

    def __init__(self, logger, record_count, strt, endp, show_progress):
        self.logger = logger if logger else logging
        self.record_count = max(1, int(record_count))
        self.strt = strt
        self.endp = endp
        self.show_progress = show_progress
        self.level_map = {
            "CRITICAL": self.logger.critical,
            "ERROR": self.logger.error,
            "WARNING": self.logger.warning,
            "INFO": self.logger.info,
            "DEBUG": self.logger.debug,
        }
        self.done = 0
        self.delta_v = endp - strt

    def put(self, item, block=True, timeout=None):
        if item is None:
            return

        try:
            level, message = item
        except Exception:
            self.logger.error("Invalid log format detected: %s", item)
            return

        lvl = str(level).upper()

        if lvl == "PROG":
            if self.show_progress:
                self.done += int(message)
                progress = self.strt + round(self.delta_v * self.done / self.record_count)
                print(f"Progress: {progress}%", flush=True)
            return

        if lvl == "STOP":
            return

        log_func = self.level_map.get(lvl)
        if log_func:
            log_func(message)
        else:
            self.logger.warning("Unknown log level: %s - %s", lvl, message)

    def put_nowait(self, item):
        self.put(item)

    def close(self):
        return None

    def join_thread(self):
        return None
