# Get metadata hash of files and return array                       03/11/2026
import calendar
import os
from datetime import datetime
from .fileops import calculate_checksum
from .fileops import find_link_target
from .fileops import set_stat
from .fsearchfunctions import get_file_id
from .fsearchfunctions import get_cached
from .fsearchfunctions import file_owner
from .fsearchfunctions import parse_iso
from . import logs
from .logs import emit_log
# Powershell Parallel SORTCOMPLETE search and ctime hashing


def process_ps1(line, checksum, filetype, search_start_dt, CACHE_F, logger=None):

    fmt = "%Y-%m-%d %H:%M:%S"
    label = "Sortcomplete"
    CSZE = 1024 * 1024

    log_entries = []

    checks = cam = last_modified = target = None

    if len(line) < 11:
        emit_log("DEBUG", f"process_ps1 record length less than required 11. skipping: {line}", logs.WORKER_LOG_Q, logger=logger)
        return None, log_entries

    mod_time = line[0]
    file_path = line[1]
    c_time = line[2]
    # inode = line[3]
    access_time = line[4]
    # checksum = line[5]
    size = line[6]
    sym = line[7]
    owner = line[8]
    domain = line[9]
    mode = line[10]

    if not os.path.exists(file_path):
        return None, log_entries
    mtime = parse_iso(mod_time, logs.WORKER_LOG_Q, logger=logger)

    if not os.path.isfile(file_path):
        if not mtime:
            mt = datetime.now().strftime(fmt)
        else:
            mt = mtime.replace(microsecond=0)
        return ("Nosuchfile", mt, mt, file_path), log_entries
    if mtime is None:
        return None, log_entries

    c_time = parse_iso(c_time, logs.WORKER_LOG_Q, logger=logger)
    # pywin32
    inode, _, hardlink, _, _, _, status = get_file_id(file_path, logs.WORKER_LOG_Q, logger=logger)  # sym c_time mode
    if status == "Nosuchfile":
        mt = mtime.replace(microsecond=0)
        return ("Deleted", mt, mt, file_path), log_entries
    elif status == "Error":
        return None, log_entries

    sec = calendar.timegm(mtime.utctimetuple())
    us = mtime.microsecond
    mtime_us = sec * 1_000_000 + us

    if sym != "y" and size and checksum:

        if size > CSZE:
            cached = get_cached(CACHE_F, size, mtime_us, file_path)
            if cached is None:
                checks, file_dt, file_us, file_st, status = calculate_checksum(file_path, mtime, mtime_us, inode, size, retry=1, max_retry=1, cacheable=True, log_q=logs.WORKER_LOG_Q, logger=logger)
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
            checks, file_dt, file_us, file_st, status = calculate_checksum(file_path, mtime, mtime_us, inode, size, retry=1, max_retry=1, cacheable=False, log_q=logs.WORKER_LOG_Q, logger=logger)
            if checks is not None:
                if status == "Retried":
                    checks, mtime, st, mtime_us, c_time, inode, size = set_stat(line, checks, file_dt, file_st, file_us, inode, logs.WORKER_LOG_Q, logger=logger)
            else:
                if status == "Nosuchfile":
                    mt = mtime.replace(microsecond=0)
                    return ("Deleted", mt, mt, file_path), log_entries

    elif sym == "y":
        target = find_link_target(file_path, logs.WORKER_LOG_Q, logger=logger)

    if not owner:
        owner_domain = file_owner(file_path, logs.WORKER_LOG_Q, logger=logger)
        if owner_domain in (None, "Nosuchfile"):
            mt = mtime.replace(microsecond=0)
            return ("Deleted", mt, mt, file_path), log_entries
        owner, domain = owner_domain if owner_domain else (None, None)

    if mtime is None:
        emit_log("DEBUG", f"no mtime from calculate checksum: {file_path} mtime={mtime}", logs.WORKER_LOG_Q, logger=logger)
        return None, log_entries

    if c_time and c_time > mtime:
        last_modified = mtime
        mtime = c_time
        cam = "y"
    elif not c_time:
        emit_log("DEBUG", f"creation time was None at casmod check: {file_path} : {line}", logs.WORKER_LOG_Q, logger=logger)

    if mtime < search_start_dt:
        emit_log("DEBUG", f"Warning system cache conflict: {file_path} mtime={mtime} < cutoff={search_start_dt}", logs.WORKER_LOG_Q, logger=logger)
        return None, log_entries

    atime = parse_iso(access_time, logs.WORKER_LOG_Q, logger=logger)

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
        last_modified.strftime(fmt) if last_modified is not None else None,
        hardlink,
        mtime_us
    ), log_entries
