# Get metadata hash of files and return array                       07/19/2026
import os
from datetime import datetime
from . import logs
from .logs import emit_log
from .fileops import calculate_checksum
from .fileops import find_link_target
from .fileops import set_stat
from .fsearchfunctions import default_mode
from .fsearchfunctions import get_file_id
from .fsearchfunctions import get_cached
from .fsearchfunctions import file_owner
from .fsearchfunctions import normalize_timestamp
from .pyfunctions import epoch_to_date

# Find Parallel SORTCOMPLETE search and  ctime hashing


def process_scan(line, checksum, file_type, search_start_dt, cache_f, algo="md5", logger=None):

    label = "Sortcomplete"
    fmt = "%Y-%m-%d %H:%M:%S"
    CSZE = 1048576

    log_entries = []

    checks = entropy = mime = cam = lastmodified = None
    target = None
    cached = status = None
    file_st = None

    if len(line) < 11:
        emit_log("DEBUG", f"process_scan record length less than required 2. skipping: {line}", logs.WORKER_LOG_Q, logger=logger)
        return None, log_entries

    mod_time, access_time, c_time, inode, sym, hardlink, size, owner, domain, mode, file_path = line

    if not os.path.exists(file_path):
        return None, log_entries
    mtime = epoch_to_date(mod_time)
    if not os.path.isfile(file_path):
        if not mtime:
            mt = datetime.now().strftime(fmt)
        else:
            mt = mtime.replace(microsecond=0)
        return ("Nosuchfile", mt, mt, file_path), log_entries
    if mtime is None:
        return None, log_entries

    # if python os.scandir files_search
    if inode is None:
        # get inode as st.st_ino unreliable. use py32win.
        inode, sym, hardlink, size, c_time, mode, status = get_file_id(file_path)
        if status == "Nosuchfile":
            mt = mtime.replace(microsecond=0)
            return ("Deleted", mt, mt, file_path), log_entries
        elif status == "Error":
            return None, log_entries

        if not c_time:
            emit_log("DEBUG", f"process_scan file no creation time from py32win file: {file_path} line: {line}", logs.WORKER_LOG_Q, logger=logger)

    # if xRC access_time isnt stored so do a small initial stat before checksum
    if not access_time:
        try:
            st = os.lstat(file_path)
            access_time = st.st_atime
        except PermissionError:
            emit_log("DEBUG", f"Permission denied could not get access time entry : {line}", logs.WORKER_LOG_Q, logger=logger)
            return None, log_entries
        except OSError as e:
            emit_log("DEBUG", f"Error stating access time Skipping entry : {line} {type(e).__name__} err: {e}", logs.WORKER_LOG_Q, logger=logger)
            return None, log_entries

    try:
        size = int(size)
    except (TypeError, ValueError) as e:
        emit_log("ERROR", f"process_scan from find  {e} {type(e).__name__} size: {size} line:{line}", logs.WORKER_LOG_Q, logger=logger)
        return None, log_entries

    mtime_us = normalize_timestamp(mod_time)
    if sym != "y" and size and checksum:

        if size > CSZE:
            cached = get_cached(cache_f, size, mtime_us, file_path)
            if cached is None:
                checks, entropy, mime, file_dt, file_us, file_st, status = calculate_checksum(file_path, mtime, mtime_us, inode, size, algo=algo, retry=1, cacheable=True, log_q=logs.WORKER_LOG_Q, logger=logger)
                if checks is not None:
                    if status == "Retried":
                        checks, mtime, st, mtime_us, c_time, inode, size = set_stat(line, checks, file_dt, file_st, file_us, inode, logs.WORKER_LOG_Q, logger=logger)

                    if checks:
                        label = "Cwrite"

                else:
                    if status == "Nosuchfile":
                        mt = mtime.replace(microsecond=0)
                        return ("Deleted", mt, mt, file_path), log_entries
            else:
                checks = cached.get("checksum")

        else:
            checks, entropy, mime, file_dt, file_us, file_st, status = calculate_checksum(file_path, mtime, mtime_us, inode, size, algo=algo, retry=1, cacheable=False, log_q=logs.WORKER_LOG_Q, logger=logger)
            if checks is not None:
                if status == "Retried":
                    checks, mtime, st, mtime_us, c_time, inode, size = set_stat(line, checks, file_dt, file_st, file_us, inode, logs.WORKER_LOG_Q, logger=logger)

            else:
                if status == "Nosuchfile":
                    mt = mtime.replace(microsecond=0)
                    return ("Deleted", mt, mt, file_path), log_entries

    elif sym == "y":
        target = find_link_target(file_path, logs.WORKER_LOG_Q, logger=logger)

    resolve_owner = file_owner(file_path, logs.WORKER_LOG_Q, logger=logger)

    if resolve_owner in (None, "Nosuchfile"):
        mt = mtime.replace(microsecond=0)
        return ("Deleted", mt, mt, file_path), log_entries

    owner, domain = resolve_owner if resolve_owner else (None, None)

    if not mode:
        mode = default_mode(sym)
        emit_log("DEBUG", f"missing mode setting default line: {line}", logs.WORKER_LOG_Q, logger=logger)
    if mtime is None:
        emit_log("DEBUG", f"process line no mtime from calculate checksum: {file_path} mtime={mtime}", logs.WORKER_LOG_Q, logger=logger)
        return None, log_entries

    if c_time and c_time > mtime:
        lastmodified = mtime
        mtime = c_time
        cam = "y"
    elif not c_time:
        emit_log("DEBUG", f"creation time was None at casmod check: {file_path} : {line}", logs.WORKER_LOG_Q, logger=logger)
    if mtime < search_start_dt:
        emit_log("DEBUG", f"Warning system cache conflict: {file_path} mtime={mtime} < cutoff={search_start_dt}", logs.WORKER_LOG_Q, logger=logger)
        return None, log_entries

    atime = epoch_to_date(access_time)

    # tuple
    return (
        label,
        mtime.replace(microsecond=0),
        file_path,
        c_time.strftime(fmt) if c_time is not None else None,
        inode,
        atime.strftime(fmt) if atime is not None else None,
        checks,
        entropy,
        mime,
        size,
        sym,
        owner,
        domain,
        mode,
        cam,
        target,
        lastmodified.strftime(fmt) if lastmodified is not None else None,
        hardlink,
        mtime_us
    ), log_entries
