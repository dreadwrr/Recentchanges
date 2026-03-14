# Get metadata hash of files and return array                       03/11/2025
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


def process_line(line, checksum, file_type, search_start_dt, CACHE_F):

    label = "Sortcomplete"
    fmt = "%Y-%m-%d %H:%M:%S"
    CSZE = 1048576

    log_entries = []

    checks = cam = lastmodified = None
    sym = target = None
    owner = domain = mode = None
    cached = status = None
    file_st = None

    if len(line) < 11:
        emit_log("DEBUG", f"process_line record length less than required 11. skipping: {line}", logs.WORKER_LOG_Q)
        return None, log_entries

    mod_time, access_time, _, ino, symlink, hardlink, size, _, _, _, file_path = line  # changetime, owner, domain, mode

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

    # py32win
    inode, sym, hardlink, c_time, mode = get_file_id(file_path)
    if inode == "not_found":
        mt = mtime.replace(microsecond=0)
        return ("Deleted", mt, mt, file_path), log_entries
    if not c_time:
        emit_log("DEBUG", f"process_line file no creation time from py32win file: {file_path} line: {line}", logs.WORKER_LOG_Q)
        if file_type == "ctime":
            return None, log_entries

    if not (file_type == "ctime" and c_time > mtime) and file_type != "main":
        return None, log_entries

    try:
        size = int(size)
    except (TypeError, ValueError) as e:
        emit_log("ERROR", f"process_line from find  {e} {type(e).__name__} size: {size} line:{line}", logs.WORKER_LOG_Q)
        return None, log_entries

    mtime_us = normalize_timestamp(mod_time)
    if sym != "y" and checksum:
        if size > CSZE:
            cached = get_cached(CACHE_F, size, mtime_us, file_path)
            if cached is None:
                checks, file_dt, file_us, file_st, status = calculate_checksum(file_path, mtime, mtime_us, inode, size, retry=1, max_retry=1, cacheable=True, log_q=logs.WORKER_LOG_Q)
                if checks is not None:
                    if status == "Retried":
                        checks, mtime, st, mtime_us, c_time, inode, size = set_stat(line, checks, file_dt, file_st, file_us, inode, logs.WORKER_LOG_Q)

                    if checks:
                        label = "Cwrite"

                else:
                    if status == "Nosuchfile":
                        mt = mtime.replace(microsecond=0)
                        return ("Deleted", mt, mt, file_path), log_entries
            else:
                checks = cached.get("checksum")

        else:
            checks, file_dt, file_us, file_st, status = calculate_checksum(file_path, mtime, mtime_us, inode, size, retry=1, max_retry=1, cacheable=False, log_q=logs.WORKER_LOG_Q)
            if checks is not None:
                if status == "Retried":
                    checks, mtime, st, mtime_us, c_time, inode, size = set_stat(line, checks, file_dt, file_st, file_us, inode, logs.WORKER_LOG_Q)

            else:
                if status == "Nosuchfile":
                    mt = mtime.replace(microsecond=0)
                    return ("Deleted", mt, mt, file_path), log_entries

    elif sym == "y":
        target = find_link_target(file_path, logs.WORKER_LOG_Q)

    resolve_owner = file_owner(file_path, logs.WORKER_LOG_Q)
    owner, domain = resolve_owner if resolve_owner else (None, None)

    if not mode:
        mode = default_mode(sym)
        emit_log("DEBUG", f"missing mode setting default line: {line}", logs.WORKER_LOG_Q)
    if mtime is None:
        emit_log("DEBUG", f"process line no mtime from calculate checksum: {file_path} mtime={mtime}", logs.WORKER_LOG_Q)
        return None, log_entries

    if file_type == "ctime":
        if c_time is None or c_time <= mtime:
            return None, log_entries
        lastmodified = mtime
        mtime = c_time
        cam = "y"
    if mtime < search_start_dt:
        emit_log("DEBUG", f"Warning system cache conflict: {file_path} mtime={mtime} < cutoff={search_start_dt}", logs.WORKER_LOG_Q)
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
