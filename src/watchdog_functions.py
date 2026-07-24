import os
import psutil
from pathlib import Path
from src.dirwalkerfunctions import get_stat
from src.logs import emit_log
from src.logs import write_log
from src.fileops import calculate_checksum
from src.fileops import set_stat
from src.fsearchfunctions import file_owner
from src.fsearchfunctions import get_file_id
from src.fsearchfunctions import normalize_timestamp
from src.inotifyfunctions import drop_pid
from src.inotifyfunctions import process_kill
from src.pyfunctions import epoch_to_date
from src.pyfunctions import epoch_to_str
SENTINEL = None
DEBUG = False  # switch to serial so more rudimentary operation and added verbosity to print out in key areas so can debug the core for stable operation
CSZE = 1024 * 1024  # when to cache created files


def emit_write(output_file, CACHE_F, checks, size, out_data, cache_data, lockfile, log_q, logger):
    payload = (output_file, CACHE_F, checks, size, out_data, cache_data)
    if log_q is not None:
        log_q.put(("write", payload))
    else:
        try:
            file_lineout(payload, lockfile, logger)
        except Exception as e:
            emit_log("ERROR", f"write failed: {e}", logger=logger)


def file_lineout(payload, lockfile, logger):
    output_file, CACHE_F, checks, size, out_data, cache_data = payload
    # lock_file = open(lockfile, "w")
    # lock_ = False
    # try:
    # non blocking
    # msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)

    # block until lock
    # msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
    # lock_ = True
    if checks and size and size > CSZE:
        with open(CACHE_F, 'a') as f:
            f.write(cache_data + '\n')

    with open(output_file, 'a') as f:
        f.write(" ".join(map(str, out_data)) + '\n')

    # except OSError as e:
    #     logs.emit_log("ERROR", f"Failed to acquire lock: {e}", logger=logger)
    # finally:
    #     if lock_:
    #         msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
    #         lock_ = False
    #     lock_file.close()


def logger_process(work_queue, lockfile, logger):

    log = logger

    while True:
        msg = work_queue.get()
        if msg is SENTINEL:
            break
        try:
            level, message = msg
        except Exception:

            emit_log("ERROR", f"Invalid log format detected: {msg}", logger=log)
            continue

        try:

            if level == "write":
                file_lineout(message, lockfile, log)
            else:
                write_log(log, level, message)
        except Exception as e:
            emit_log("ERROR", f"level {level} write failed: {e}", logger=log)


def log_lineout(log_q, logger, path, status, message):
    """ handle errors here rather than cascading them down so caller check if missing result handling to indicate something unusual happened
        if its Nosuchfile return False
        if its Error return None
        return True  """
    is_error = True
    level = "DEBUG"
    if status == "Nosuchfile":
        is_error = False
    elif status == "Error":
        level = "ERROR"
        is_error = None

    emit_log(level, f"{message} {status} file: {path}", log_q, logger=logger)
    return is_error


def get_specs(action, entry, path, output_file, CACHE_F, lockfile, algo, created_seen, log_q, logger):
    fmt = "%Y-%m-%d %H:%M:%S"

    cam = last_modified = None

    stat_info = get_stat(entry, log_q, logger=logger)
    if not stat_info:
        return

    m_epoch = stat_info.st_mtime

    c_epoch = stat_info.st_birthtime

    a_epoch = stat_info.st_atime

    mtime = epoch_to_date(m_epoch)
    ctime = epoch_to_date(c_epoch)
    if mtime is None:
        return log_lineout(log_q, logger, path, "Error", f"Warning mt is null on input to get_specs skipped {path}")

    size = stat_info.st_size

    m_epoch_ns = stat_info.st_mtime_ns
    mtime_us = normalize_timestamp(str(m_epoch))  # mtime_us = m_epoch_ns // 1_000_000 # truncate as opposed to round

    # get inode as st.st_ino unreliable. use py32win.
    inode, sym, hardlink, _, _, mode, status = get_file_id(path)
    if status in ("Nosuchfile", "Error"):
        return log_lineout(log_q, logger, path, status, "get_specs failed to get inode, hardlink and mode")

    if action == "created":
        if path not in created_seen:
            return
        elif not size:
            return
        del created_seen[path]

    if sym != "y":

        resolve_owner_data = file_owner(path, log_q, logger=logger)
        if resolve_owner_data in ("Nosuchfile", "Error"):
            return log_lineout(log_q, logger, path, resolve_owner_data, "get_specs failed to get owner domain")

        owner, domain = resolve_owner_data if resolve_owner_data else (None, None)

        m_time = mtime.strftime(fmt)
        c_time = ctime.strftime(fmt)
        a_time = epoch_to_str(a_epoch)
        is_error = None
        status = is_error

        file_info = (mode, inode, hardlink, owner, domain, mtime, m_epoch_ns, m_time, c_time, a_time, size, status)  # for logging if inode changed in set_stat
        # or can be used for debug output line = ', '.join(map(str, file_info)) and would have to be updated after set_stat

        checks, entropy, mime, file_dt, file_us, file_st, status = calculate_checksum(path, mtime, mtime_us, inode, size, algo=algo, retry=2, cacheable=True, log_q=log_q, logger=logger)

        if checks is not None:  # if status in ("Returned", "Retried"):
            if status == "Retried":
                # returns date time objs to refresh stats. if inode changed checks set to None and rejected
                checks, mtime, st, mtime_us, ctime, inode, size = set_stat(file_info, checks, file_dt, file_st, file_us, inode, log_q, logger=logger)
                if mtime is None:
                    return log_lineout(log_q, logger, path, "Error", "get_specs Retried mtime was None skipping")

                m_time = mtime.strftime(fmt)
                c_time = ctime.strftime(fmt) if ctime else None
                m_epoch = mtime.timestamp()
                c_epoch = ctime.timestamp() if ctime else None

        elif status == "Nosuchfile":
            return log_lineout(log_q, logger, path, status, "get_specs file not found while calculating checksum")

        # status in ("Returned", "Retried", "Changed"):
        if status == "Changed":
            emit_log("DEBUG", f"get_specs was unable to hash in calculate_checksum checksum set to None file {path}", log_q, logger=logger)
        if ctime:
            if ctime < mtime:
                emit_log("DEBUG", f"get_specs itime {c_epoch} and mtime {m_epoch} , ctime was < mtime file has since been modified {path}", log_q, logger=logger)

            elif ctime > mtime:
                cam = "y"
                last_modified = m_time
                m_time = c_time

        if not size:
            checks = None

        # Output results

        emit_log("DEBUG", f"change time: {c_epoch} and mtime: {m_epoch} , get_specs passed processed line", log_q, logger=logger)

        # get proper formatting - printf '%s|%s|%s\t%s\t%s\n' "$inode" "$size" "$mtime" "$checksum" "$path" >> "$cache_file"  # uptcache from pblk on cache
        #
        #                                              timestamp   mtime_us
        # the check in this app uses - checksum|size|modified_time|modified_ep|root
        out_str = f"{inode}|{size}|{mtime_us}\t{checks}\t{entropy}\t{mime}\t{path}"

        # always write to output_file so can diagnose ***
        # mt, ct, ats and lmt is format "date time"
        # get proper formatting - output="$mt \"$y\" $ct $i $ats $adtcmd $cam $lmt $nlinks $mtime_us"  # pblk
        #
        # adtcmd="$check_sum $fs $sl $wnr $grp $pmn"  # pblk

        data = [
            m_time, f'"{path}"', c_time, inode, a_time, checks,
            entropy, mime, size, sym, owner, domain, mode, cam,
            last_modified, hardlink, mtime_us
        ]

        emit_write(output_file, CACHE_F, checks, size, data, out_str, lockfile, log_q, logger)


def is_excl_dir(path: Path, exclusions: list) -> bool:
    path = path.resolve()

    for excluded in exclusions:
        try:
            path.relative_to(excluded)
            return True
        except ValueError:
            pass

    return False


def is_temp_file(path: Path, temp_suffixes) -> bool:
    return path.suffix.lower() in temp_suffixes


def shandle(action, event, entry, path, start_time, created_seen, log_q, logger):
    """ formerly phandle returns false meaning process the event """

    # if using another index could store a key
    # size = stat_info.st_size
    # key = (path, size, mod_time)
    # pending_files[key] = time.time()

    # a moved file wouldnt have a creation event or the creation event could have been missed
    stat_info = get_stat(entry, log_q, logger=logger)
    if not stat_info:
        return True

    mod_time = stat_info.st_mtime

    src = str(Path(event.src_path).resolve())
    # the unusual. maybe some other app? creation event write partial in place -> move event dest but dest isnt in created_seen src is.
    if src in created_seen:

        del created_seen[src]

        emit_log("DEBUG", f"handle_file {action} unusual src was in created_seen on move after created file. src: {src} dest or file: {path}", log_q, logger=logger)

    # the usual. firefox and others normal for downloaded file. creation event makes final file src 0 bytes -> writes a partial -> move event dest atomic with full file
    elif path in created_seen:

        del created_seen[path]

    else:
        # does it look like a creation event?

        emit_log("DEBUG", f"handle_file {action} did not have a creation event checking if ctime >= mtime for file: {path}", log_q, logger=logger)

        creation_time = stat_info.st_ctime
        if creation_time < mod_time and creation_time < start_time:
            emit_log("DEBUG", f"handle_file {action} it was a regular move. skipping.. file: {path}", log_q, logger=logger)
            return True

    return False


def old_pid_check(pid_file, new_pid, logger, platform):
    """ if there is an old pid file try to kill. """
    res = False

    if os.path.isfile(pid_file):
        with open(pid_file) as f:
            parts = f.read().rstrip("\n").split("|", 1)

        if parts and len(parts) == 2:
            stored_pid, stored_start = parts
            stored_pid = int(stored_pid)
            stored_start = float(stored_start)
            differs = new_pid and new_pid != stored_pid

            if differs or not new_pid:
                logger.debug(f"{pid_file} stale pid found attempted cleanup new {new_pid} vs old {stored_pid}")
                try:
                    proc = psutil.Process(stored_pid)
                    current_start = proc.create_time()
                except psutil.NoSuchProcess:
                    current_start = None

                if current_start is not None:
                    normalized = abs(current_start - stored_start) < 1e-3
                    if normalized:
                        if platform == "linux":
                            # process_kill(pid)
                            res = drop_pid(stored_pid, platform, pid_file=pid_file)
                            # if not res:
                            #   kill by pattern
                            #   fk_success = _fk_process(r'inotifywait.*-e create -e moved_to --format %e\|%w%f%0')  # fk_success = _fk_process('inotifywait -m -r -e create -e moved_to --format %e|%w%f%0')  # original
                        elif platform == "windows":
                            res = process_kill(stored_pid, pid_file=pid_file)
                        if res:
                            return True  # In the rare case it wasnt previously shutdown prevented having two before starting this watchdog process
                        # else:
                        # alternative
                        # try:
                        #     os.killpg(int(stored_pid), signal.SIGTERM)
                        #     return True
                        # except OSError:
                        #     print(f"failed to close old pid {stored_pid} {stored_start}")
                    else:
                        logger.debug(f"{pid_file} pid {stored_pid} reused by a different process, removing stale pidfile")
                else:
                    logger.debug("couldnt get process time from psutil oldpid skipping check")
        else:
            logger.error(f"incorrect format in {pid_file} couldnt parse in oldpid")

        os.remove(pid_file)  # normal clear the old pid file

    return res


# original
# def spec_handle(action, event, path, created_seen, log_q, logger):

    # src = str(Path(event.src_path).resolve())
    # # stat_info = get_stat(entry, log_q, logger=self.logger)
    # # if not stat_info:
    # #     return None

    # # mod_time = stat_info.st_mtime

    # # unconventional or maybe some other app? creation event write partial in place -> move event dest but dest isnt in created_seen src is.
    # if src in created_seen:
    #   # log unusual event
    #   emit_log("DEBUG", f"handle_file {action} unusual src was in created_seen on move after created file. src: {src} dest or file: {path}", log_q, logger=logger)
    #   del created_seen[src]

    #   return False

    # else:
    #   # firefox and others normal for downloaded file. creation event makes final file src 0 bytes -> writes a partial -> move event dest atomic with full file
    #   if path in created_seen:
    #       del created_seen[path]

    #       # size = stat_info.st_size
    #       # key = (path, size, mod_time)
    #       # self.pending_files[key] = time.time()
    #   else:

    #       # a moved file wouldnt have a creation event or the creation event could have been missed

    #       emit_log("DEBUG", f"handle_file {action} did not have a creation event checking if ctime >= mtime for file: {path}", log_q, logger=logger)
    #       # change_time = stat_info.stat_info.st_ctime
    #       # gated on regular moves. otherwise ctime >= mtime and since watch start, they are files of interest so process anyway
    #       # on linux this increases noise but is better to include than exclude and can adjust where necessary
    #       # if change_time < mod_time or change_time < self.start_time:
    #       #      emit_log("DEBUG", f"handle_file {action} it was a regular move. skipping.. file: {path}", log_q, logger=self.logger)
    #       #      return

    #   return True


# def get_pid(pid_file):
#     """ not used as could accidently kill the wrong process """
#     pid = None
#     if os.path.isfile(pid_file):
#         try:
#             with open(pid_file, "r") as f:
#                 pid = int(f.read().strip())
#         except (ValueError, OSError):
#             return None
#     return pid


#     pid = get_pid(watchdog_pid_file)
#     if pid:
#         differs = new_pid and new_pid != pid
#         if differs or not new_pid:
#             logger.debug(f"{watchdog_pid_file} stale pid found attempted cleanup new {new_pid} vs old {pid}")
#             if platform == "linux":
#                 # process_kill(pid)
#                 drop_pid(pid, platform, watchdog_pid_file)
#                 # if not res:
#                 #   kill by pattern
#                 #   fk_success = _fk_process(r'inotifywait.*-e create -e moved_to --format %e\|%w%f%0')  # fk_success = _fk_process('inotifywait -m -r -e create -e moved_to --format %e|%w%f%0')  # original
#             else:
#                 process_kill(pid, watchdog_pid_file)
