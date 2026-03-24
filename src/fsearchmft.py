
#  Mft parallel                03/16/2026
#  Certain fields are different from fsearch and fsearchps1. The MFT doesnt have to walk the filesystem.
# C# and rust parsers are efficient. This can be invaluable in locating a file or file(s).
import os
from datetime import datetime
from .fileops import calculate_checksum
from .fileops import find_link_target
from .fileops import is_reparse_point
from .fileops import set_stat
from .fsearchfunctions import default_mode
from .fsearchfunctions import file_owner
from .fsearchfunctions import get_cached
from .fsearchfunctions import get_mode
from .fsearchfunctions import get_mft_mode
from . import logs
from .logs import emit_log


# mftecmd Parallel SORTCOMPLETE search and  ctime hashing


def process_mft(line, checksum, filetype, search_start_dt, CACHE_F, logger=None):

    fmt = "%Y-%m-%d %H:%M:%S"

    label = "Sortcomplete"
    CSZE = 1024 * 1024

    checks = sym = target = mode = None

    log_entries = []

    mtime = line[0]
    mtime_us = line[1]
    c_time = line[2]
    atime = line[3]
    size = line[4]
    last_modified = line[5]
    mode_attribs = line[6]
    hardlink = line[7]
    inode = line[8]
    cam = line[9]
    file_path = line[10]

    if not os.path.exists(file_path):
        return None, log_entries
    if not os.path.isfile(file_path):
        if not mtime:
            mt = datetime.now().strftime(fmt)
        else:
            mt = mtime.replace(microsecond=0)
        return ("Nosuchfile", mt, mt, file_path), log_entries
    if mtime is None:
        return None, log_entries

    if mode_attribs not in ("None", None):
        mode, sym = get_mft_mode(mode_attribs)
    else:
        emit_log("DEBUG", f"Mft missing mode_atribs stating file : {file_path}", logs.WORKER_LOG_Q, logger=logger)
        try:
            st = os.lstat(file_path)
            if is_reparse_point(st):
                sym = "y"
            attrs = getattr(st, "st_file_attributes", 0)
            mode = get_mode(attrs, sym)
        except PermissionError:
            emit_log("DEBUG", f"Permission denied could not stat mft entry : {line}", logs.WORKER_LOG_Q, logger=logger)
            return None, log_entries
        except OSError as e:
            emit_log("DEBUG", f"Error stating mft Skipping entry : {line} {type(e).__name__} err: {e}", logs.WORKER_LOG_Q, logger=logger)
            return None, log_entries

    # mtime_us = int(mtime.timestamp() * 1_000_000) # passed in from pandas
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

    if not mode:
        mode = default_mode(sym)
        emit_log("DEBUG", f"missing mode_attribs line: {line}", logs.WORKER_LOG_Q, logger=logger)
    if mtime is None:
        emit_log("DEBUG", f"no mtime from calculate checksum: {file_path} mtime={mtime}", logs.WORKER_LOG_Q, logger=logger)
        return None, log_entries
    if not c_time:
        emit_log("DEBUG", f"creation time was None at casmod check: {file_path} : {line}", logs.WORKER_LOG_Q, logger=logger)
    if mtime < search_start_dt:
        emit_log("DEBUG", f"Warning system cache conflict: {file_path} mtime={mtime} < cutoff={search_start_dt}", logs.WORKER_LOG_Q, logger=logger)
        return None, log_entries

    owner_domain = file_owner(file_path, logs.WORKER_LOG_Q, logger=logger)
    if owner_domain in (None, "Nosuchfile"):
        mt = mtime.replace(microsecond=0)
        return ("Deleted", mt, mt, file_path), log_entries
    owner, domain = owner_domain if owner_domain else (None, None)

    return (
        label,
        mtime.replace(microsecond=0),
        file_path,
        c_time,
        inode,
        atime,
        checks,
        size,
        sym,
        owner,
        domain,
        mode,
        cam,
        target,
        last_modified,
        hardlink,
        mtime_us
    ), log_entries

#
# End parallel #
