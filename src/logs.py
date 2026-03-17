import logging
import os
from pathlib import Path

WORKER_LOG_Q = None


LEVEL_MAP = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "DEBUG": logging.DEBUG,
}


def init_process_worker(log_q):
    global WORKER_LOG_Q
    WORKER_LOG_Q = log_q


def write_log(log, level, message):
    method = getattr(log, str(level).lower(), None)
    if method:
        method(message)
    else:
        log.error(f"Unknown log level: {message}")


def write_logs_to_logger(log_list, logger=None):
    log = logger if logger else logging
    for level, message in log_list:
        write_log(log, level, message)


def logging_worker(queue, record_count, strt, endp, show_progress, logger=None):
    done = 0
    delta_v = endp - strt
    log = logger if logger else logging
    while True:
        msg = queue.get()

        if msg is None:
            break
        try:
            level, message = msg
        except Exception:
            log.error(f"Invalid log format detected: {msg}")
            continue
        if level == "prog" and show_progress:
            n = message
            done += n
            print(f"Progress: {strt + round((delta_v) * done / record_count)}%", flush=True)
        elif level == 'STOP':
            break
        else:
            write_log(log, level, message)


def emit_log(level, message, log_q=None, log_entries=None, logger=None):
    if log_q is not None:
        log_q.put((level, message))
    elif log_entries is not None:
        log_entries.append((level, message))
    elif logger:
        write_log(logger, level, message)


def logs_to_queue(log_list, queue):
    for msg in log_list:
        queue.put(msg)


def filename_of_handler():
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.FileHandler):
            return Path(handler.baseFilename)
    return None
    # root = logging.getLogger()
    # for h in root.handlers:
    #     if isinstance(h, logging.FileHandler):
    #         log_path = Path(h.baseFilename)
    #         return log_path


def set_logger(root, process_label="MAIN", level=None):
    fmt = logging.Formatter(f'%(asctime)s [%(name)s] [{process_label}] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')  # [%(levelname)s]
    # if level is not None:
    #     root.setLevel(level)
    for handler in root.handlers:
        handler.setFormatter(fmt)
        if level is not None:
            handler.setLevel(level)


def set_log_level(log_file, level):

    log_level = LEVEL_MAP.get(level, logging.ERROR)
    return log_level


def set_format(log_file, level, process_label):
    log_level = set_log_level(log_file, level.upper())

    logging.basicConfig(
        filename=log_file,
        level=log_level,
        format=f'%(asctime)s [%(name)s] [{process_label}] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # notes: other options: [%(levelname)s]


def setup_logger(log_file, level="ERROR", process_label="MAIN"):
    """  set log level by handler for script or script area  """
    root = logging.getLogger()

    if not root.hasHandlers():
        set_format(log_file, level, process_label)
    else:
        set_logger(root, process_label, level.upper())

    return root


def change_logger(log_file, level, process_label):

    root = logging.getLogger()

    log_level = LEVEL_MAP.get(str(level).upper(), logging.ERROR)

    fmt = logging.Formatter(
        f"%(asctime)s [%(name)s] [{process_label}] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    for h in root.handlers[:]:
        if isinstance(h, logging.FileHandler):
            root.removeHandler(h)
            h.close()

    fh = logging.FileHandler(Path(log_file))
    fh.setLevel(log_level)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    root.setLevel(log_level)

    return root, log_file


def check_log_perms(log_path):
    try:
        if log_path.exists():
            if log_path.stat().st_uid == 0:
                log_path.unlink()
        else:
            with open(log_path, 'a'):
                os.utime(log_path, None)
    except PermissionError:
        pass
