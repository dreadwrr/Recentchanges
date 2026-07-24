import logging
import os
import psutil
import re
import signal
import subprocess
import sys
import time
try:
    import fcntl  # linux
except ImportError:
    fcntl = None
try:
    import msvcrt  # win
except ImportError:
    msvcrt = None
from datetime import datetime
from pathlib import Path
from .fsearchfunctions import upt_cache
from .pyfunctions import ap_decode
from .pyfunctions import epoch_to_date
from .pyfunctions import escf_py
from .pyfunctions import parse_datetime
from .rntchangesfunctions import removefile
# 07/17/2026

# Globals
QUOTED_RE = re.compile(r'"((?:[^"\\]|\\.)*)"')

# xRC functions


# cross platform
def process_by_target(target):
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmdline = proc.info["cmdline"] or []
            if any(target in arg for arg in cmdline):
                return proc.pid
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return 0


def process_kill(pid, pid_file):
    try:
        proc = psutil.Process(pid)
        proc.terminate()
        proc.wait(timeout=5)
        removefile(pid_file)
        return True
    except psutil.TimeoutExpired:
        proc.kill()
        proc.wait()
        removefile(pid_file)
        return True
    except psutil.NoSuchProcess:
        return False
    except psutil.AccessDenied:
        return False


def drop_pid(pid, platform, pid_file=None):
    try:
        if platform == "linux":
            os.kill(-pid, signal.SIGTERM)
        else:
            os.kill(pid, signal.SIGTERM)
        if pid_file:
            removefile(pid_file)
        return True
    except ProcessLookupError:
        pass  # already gone
    except PermissionError:
        print("shutdown func inotifywait permission error")
    return False
# end cross platform


# linux
def process_status(pattern):
    try:
        result = subprocess.run(
            ["pgrep", "-af", pattern],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        return result.returncode == 0
    except Exception as e:
        logging.error(f"process_status xRC failed to check if process was running: {e} {type(e).__name__}", exc_info=True)
    return False


def _fk_process(pattern):
    try:
        result = subprocess.run(
            ["pkill", "-f", pattern],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        return result.returncode == 0
    except Exception as e:
        logging.error(f"_fk_process xRC failure to close process. err: {e} {type(e).__name__} \n", exc_info=True)
    return False
# end linux


def strup(script_dir, script, appdata_local, home_dir, inotify_creation_file, CACHE_F, cdir, pid_file, lockfile, log_file, ll_level, _time, escaped_user, moduleNAME, usrDIR, temp_dir, gnupg_home, supbrwLIST, debug_mode, algo, logger, platform):

    def build_terminal_cmd(terminal, cmd):
        """ wrapper function for debug below working off of return_terminal
            so linux can have a debug terminal """
        if os.path.basename(terminal) == "gnome-terminal":
            return [terminal, "--"] + cmd
        return [terminal, "-e"] + cmd

    app = str(appdata_local / "src" / "set_recent_helper.py")

    script_path = os.path.join(script_dir, script)

    is_pyinstall = False
    dispatch = sys.executable
    if getattr(sys, "frozen", False) or "__compiled__" in globals():
        is_pyinstall = True
        dispatch = Path(sys.argv[0]).resolve()

    args = [
        script,
        script_path,
        str(appdata_local),
        str(home_dir),
        str(inotify_creation_file),
        str(CACHE_F),
        str(cdir),
        str(pid_file),
        str(lockfile),
        str(log_file),
        ll_level,
        str(_time),
        algo,
        escaped_user,
        moduleNAME,
        str(usrDIR),
        temp_dir,
        str(gnupg_home),
        str(debug_mode).lower(),
        *supbrwLIST
    ]

    if not is_pyinstall:
        args = ["-u", app] + args

    try:
        script_dir = os.path.dirname(script_path)

        kwargs = {"cwd": script_dir}

        cmd = [dispatch] + args

        if platform == "windows":
            kwargs["creationflags"] = (
                subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NEW_CONSOLE
                if debug_mode else
                subprocess.CREATE_NEW_PROCESS_GROUP
            )
        # else:
        #     kwargs["start_new_session"] = True
        #     if debug_mode:
        #         terminal = return_terminal()
        #         if terminal:
        #             cmd = build_terminal_cmd(terminal, cmd)

        subprocess.Popen(cmd, **kwargs)

        logger.debug("strup completed successfully")
    except Exception as e:
        print("xRC unable to start watchdog logged to", log_file)
        logger.error(f"strup General exception unable to start inotify wait: {e} {type(e).__name__}", exc_info=True)


def to_int_or_not(value, field, line):
    try:
        return int(value)
    except (TypeError, ValueError) as e:
        logging.debug(
            "parselog invalid integer %s: %r line: %s err: %s",
            field, value, line, e
        )
        return None


def parse_line(line):
    quoted_match = QUOTED_RE.search(line)
    if not quoted_match:
        return None
    raw_filepath = quoted_match.group(1)

    filepath = raw_filepath  # escaped but decoded in parselog

    line_without_file = line.replace(quoted_match.group(0), '').strip()  # Remove quoted path
    other_fields = line_without_file.split()

    if len(other_fields) < 7:
        return None

    timestamp1_subfld1 = None if other_fields[0] in ("", "None") else other_fields[0]
    timestamp1_subfld2 = None if other_fields[1] in ("", "None") else other_fields[1]
    timestamp1 = None if not timestamp1_subfld1 or not timestamp1_subfld2 else f"{timestamp1_subfld1} {timestamp1_subfld2}"
    if timestamp1:
        timestamp1 = parse_datetime(timestamp1)
    if not timestamp1:
        return None

    timestamp2_subfld1 = None if other_fields[2] in ("", "None") else other_fields[2]
    timestamp2_subfld2 = None if other_fields[3] in ("", "None") else other_fields[3]
    timestamp2 = None if not timestamp2_subfld1 or not timestamp2_subfld2 else f"{timestamp2_subfld1} {timestamp2_subfld2}"

    inode = other_fields[4]

    timestamp3_subfld1 = None if other_fields[5] in ("", "None") else other_fields[5]
    timestamp3_subfld2 = None if other_fields[6] in ("", "None") else other_fields[6]
    timestamp3 = None if not timestamp3_subfld1 or not timestamp3_subfld2 else f"{timestamp3_subfld1} {timestamp3_subfld2}"

    rest = other_fields[7:]

    return [timestamp1, filepath, timestamp2, inode, timestamp3] + rest


def parselog(file, checksum, logger):

    results = []

    for line in file:
        try:
            inputln = parse_line(line)
            if not inputln or not inputln[1].strip():
                logger.debug("parselog missing line or filename from input , line: %s. skipping.. file: %s", line, file)
                continue

            n = len(inputln)

            if checksum:
                if n < 18:
                    print("parselog checksum, input out of boundaries skipping")
                    logger.debug("file: %s record length less than required 18. skipping.. record: %s", file, line)
                    continue
            else:
                if n < 10:
                    print("parselog no checksum, input out of boundaries skipping")
                    logger.debug("file %s record length less than required 10. skipping.. record: %s", file, line)
                    continue

            timestamp = inputln[0]

            filename = ap_decode(inputln[1])
            escf_path = escf_py(filename)

            changetime = inputln[2]
            ino = None if inputln[3] in ("", "None") else inputln[3]
            accesstime = inputln[4]
            checks = None if n > 5 and inputln[5] in ("", "None") else (inputln[5] if n > 5 else None)
            entropy = None if n > 6 and inputln[6] in ("", "None") else (inputln[6] if n > 6 else None)
            mime = None if n > 7 and inputln[7] in ("", "None") else (inputln[7] if n > 7 else None)
            sze = None if n > 8 and inputln[8] in ("", "None") else (inputln[8] if n > 8 else None)
            sym = None if n <= 9 or inputln[9] in ("", "None") else inputln[9]
            onr = None if n <= 10 or inputln[10] in ("", "None") else inputln[10]
            gpp = None if n <= 11 or inputln[11] in ("", "None") else inputln[11]
            pmr = None if n <= 12 or inputln[12] in ("", "None") else inputln[12]
            cam = None if n <= 13 or inputln[13] in ("", "None") else inputln[13]
            timestamp1 = None if n <= 14 or inputln[14] in ("", "None") else inputln[14]
            timestamp2 = None if n <= 15 or inputln[15] in ("", "None") else inputln[15]
            lastmodified = None if not timestamp1 or not timestamp2 else f"{timestamp1} {timestamp2}"
            hardlink = None if n <= 16 or inputln[16] in ("", "None") else inputln[16]
            us = None if n <= 17 or inputln[17] in ("", "None") else inputln[17]

            target = None
            if sym == 'y':
                try:
                    target = os.readlink(filename)
                except OSError:
                    logger.error("skipped error resolving symlink target, file: %s", filename)
                    continue

            if entropy:
                try:
                    entropy = float(entropy)
                except (TypeError, ValueError) as e:
                    entropy = None
                    logging.debug(
                        "parselog not a float %s: %r line: %s err: %s",
                        "entropy", entropy, line, e
                    )

            inode = to_int_or_not(ino, "inode", line)
            filesize = to_int_or_not(sze, "filesize", line) if checksum else sze
            usec = to_int_or_not(us, "usec", line) if checksum else us
            hardlink_count = to_int_or_not(hardlink, "hardlink_count", line) if checksum else hardlink

            # changetime = inputln[2]
            # ino = None if inputln[3] in ("", "None") else inputln[3]
            # accesstime = inputln[4]
            # checks = None if n > 5 and inputln[5] in ("", "None") else (inputln[5] if n > 5 else None)
            # entropy = None if n > 6 and inputln[6] in ("", "None") else (inputln[6] if n > 6 else None)
            # mime = None if n > 7 and inputln[7] in ("", "None") else (inputln[7] if n > 7 else None)
            # sze = None if n > 8 and inputln[8] in ("", "None") else (inputln[8] if n > 8 else None)
            # sym = None if n <= 9 or inputln[9] in ("", "None") else inputln[9]
            # onr = None if n <= 10 or inputln[10] in ("", "None") else inputln[10]
            # gpp = None if n <= 11 or inputln[11] in ("", "None") else inputln[11]
            # pmr = None if n <= 12 or inputln[12] in ("", "None") else inputln[12]
            # cam = None if n <= 13 or inputln[13] in ("", "None") else inputln[13]
            # timestamp1 = None if n <= 14 or inputln[14] in ("", "None") else inputln[14]
            # timestamp2 = None if n <= 15 or inputln[15] in ("", "None") else inputln[15]
            # lastmodified = None if not timestamp1 or not timestamp2 else f"{timestamp1} {timestamp2}"
            # hardlink = None if n <= 16 or inputln[16] in ("", "None") else inputln[16]
            # us = None if n <= 17 or inputln[17] in ("", "None") else inputln[17]

            if not checksum:
                cam = checks
                timestamp1 = entropy
                timestamp2 = mime
                lastmodified = None if not timestamp1 or not timestamp2 else f"{timestamp1} {timestamp2}"
                usec = sze
                hardlink_count = sym
                checks = entropy = mime = filesize = sym = onr = gpp = None

            results.append((timestamp, filename, changetime, inode, accesstime, checks, entropy, mime, filesize, sym, onr, gpp, pmr, cam, target, lastmodified, hardlink_count, usec, escf_path))

        except Exception as e:
            print(f'Problem detected in parser parselog for line {line} err: {type(e).__name__}: {e} \n skipping..')
            logger.error("General error parselog , file %s  line: %s \n error: %s", file, line, type(e).__name__, exc_info=True)

    return results


def rotate_cache(cfr, cache_f, logger):
    created = {}
    if cache_f.is_file():
        rotated = cache_f.with_name(cache_f.name + ".old")
        if rotated.exists():
            logger.debug("init_recentchanges old cachefile already existed %s", rotated)
            removefile(rotated)
        os.rename(cache_f, rotated)
        with rotated.open("r") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    logger.debug("Skipping possibly empty line from cache file: %s", line)
                    continue
                try:
                    metadata, checksum, entropy, mime, filepath = line.split("\t", maxsplit=4)
                    filepath = filepath.strip()
                    if not filepath:
                        logger.debug("Skipping malformed line in cache file with empty filepath: %s", line)
                        continue
                except ValueError:
                    print("Skipping malformed line in cache file")
                    logger.error("Failed to parse delimiter in cache file line: %s", line)
                    continue
                try:
                    _, size, mtime_epoch = metadata.split("|")  # inode not used
                    size = int(size)
                    mtime_epoch = int(mtime_epoch)
                except ValueError:
                    print(f"Skipping malformed metadata in cache file: {metadata}")
                    logger.error("Failed to parse metadata in cache file line: %s", line)
                    continue

                time_stamp_frm = epoch_to_date(mtime_epoch / 1_000_000)
                if time_stamp_frm:
                    time_stamp = time_stamp_frm.replace(microsecond=0)
                    logger.debug("Inserting %s %s %s %s %s", checksum, size, time_stamp, mtime_epoch, filepath)
                    upt_cache(cfr, checksum, entropy, mime, size, time_stamp, mtime_epoch, filepath)

                    cache_data = {
                        'checksum': checksum,
                        'entropy': entropy,
                        'mime': mime
                    }

                    created[filepath] = cache_data
                else:
                    print("xRC invalid time_stamp or format detected in cache file.")
                    logger.debug("xRC Invalid timestamp in cache file line: %s", line)
        removefile(rotated)
    return created


# file_creation_log.txt
def parse_tout(log_file, checksum, logger):
    """ this is unused """
    tout_files = []
    all_files = []

    rotated = log_file.with_name(log_file.name + ".old")
    if os.path.exists(rotated):
        logger.debug("init_recentchanges old tout already existed %s", rotated)
        removefile(rotated)
    os.rename(log_file, rotated)

    with rotated.open('r') as f:
        tout_files = f.readlines()

    if tout_files:
        all_files = parselog(tout_files, checksum, logger)

    removefile(rotated)
    return all_files


def time_extract(line, tout_file, logger):
    parts = line.split(maxsplit=2)
    if len(parts) < 2:
        logger.error("trim_tout time_extract while parsing log impartial line couldnt get mtime. skipping.. record: %s file: %s", line, tout_file)
        return 0
    if parts[0] == "None" or parts[1] == "None":
        logger.error("trim_tout time_extract while parsing log impartial line couldnt get mtime. skipping.. record: %s file: %s", line, tout_file)
        return 0
    dt = parse_datetime(f"{parts[0]} {parts[1]}")
    return dt.timestamp() if dt else 0


def time_extract_str(line, tout_file, logger):
    parts = line.split(maxsplit=2)
    if len(parts) < 2:
        logger.error("trim_tout time_extract while parsing log impartial line couldnt get mtime. skipping.. record: %s file: %s", line, tout_file)
        return ""
    if parts[0] == "None" or parts[1] == "None":
        logger.error("trim_tout time_extract while parsing log impartial line couldnt get mtime. skipping.. record: %s file: %s", line, tout_file)
        return ""
    return f"{parts[0]} {parts[1]}"


def trim_tout(log_file, time_back=6, trim_to=9, min_span_hours=0, logger=logging):
    """ trim created log file.
        by span trim the borderline. if exceeded clear file_creation_log.txt
        or by rolling waterline """

    cutoff_time = time.time()

    if os.path.isfile(log_file):

        try:

            with log_file.open('r') as f:
                tout_files = f.readlines()

            if tout_files:

                first_ts = time_extract(tout_files[0], log_file, logger)

                # by span
                if min_span_hours:
                    # get the last file and get the span
                    last_ts = time_extract(tout_files[-1], log_file, logger)
                    span = (last_ts - first_ts) / 3600  # hours

                    if span > min_span_hours:
                        removefile(log_file)
                        return True

                # by rolling. trim to low water
                elif trim_to:
                    # is it at high water
                    trim = (cutoff_time - first_ts) > (trim_to * 3600)

                    if trim:
                        if trim_to < time_back:
                            print("trim_tout low water was higher than high water defaulting to high water", trim_to)
                            time_back = trim_to
                        cutoff_time = cutoff_time - (time_back * 3600)
                        fmt = "%Y-%m-%d %H:%M:%S"
                        cutoff_str = datetime.fromtimestamp(cutoff_time).strftime(fmt)
                        kept = [line for line in tout_files if time_extract_str(line, log_file, logger) >= cutoff_str]
                        if kept:
                            with open(log_file, 'w') as f:
                                f.writelines(kept)
                        else:
                            removefile(log_file)
                        return True

        except Exception as e:
            print(f'trim_tout problem detected in parser parselog err: {type(e).__name__}: {e} \n skipping..')
            logger.error("trim_tout General error parselog , file %s \n error: %s", log_file, type(e).__name__, exc_info=True)
            return None

    return False


def init_recentchanges(script_dir, appdata_local, usrDIR, home_dir, temp_dir, gnupg_home, cfr, xRC, _time, checksum, user, moduleNAME, log_file, ll_level, supbrwLIST, algo="md5", platform="Windows"):

    debug_mode = True  # open debug console if using qt
    inotify_log_file = "file_creation_log.txt"

    logger = logging.getLogger("INITRECENTCHANGES")
    platform = platform.lower()

    created = {}

    try:

        if platform == "linux":

            temp_base = Path("/tmp")

            # inotify_creation_file main output /tmp/file_creation_log.txt
            # CACHE_F cache output              /tmp/dbctimecache/ctimecache
            # watchdog_pid_file                 /tmp/inotify_watcher.pid
            # lockfile                          /tmp/pblk.lock

            script = search_pattern = "watchdog_linux.py"
            cdir = temp_base / "dbctimecache"
            inotify_creation_file = temp_base / inotify_log_file
            CACHE_F = cdir / "ctimecache"

            watchdog_pid_file = os.path.join(temp_base, 'inotify_watcher.pid')
            lockfile = "/tmp/pblk.lock"

        else:

            # inotify_creation_file main output \\scripts\\file_creation_log.txt
            # CACHE_F cache output              \\scripts\\ctimecache
            # watchdog_pid_file                 \\scripts\\inotify_watcher.pid
            # lockfile                          \\scripts\\ctime.lock

            script = search_pattern = "watchdog_win.py"
            cdir = script_dir
            inotify_creation_file = cdir / inotify_log_file
            CACHE_F = cdir / "ctimecache"

            watchdog_pid_file = os.path.join(cdir, 'inotify_watcher.pid')
            lockfile = cdir / "ctime.lock"

        # lock_ = False
        fk_success = True

        pid = process_by_target(search_pattern)

        if pid:

            if platform == "linux":

                # if multiple processes
                # inotify wait is running wait until it is finished if it is in the middle of a write

                # fd = os.open(lockfile, os.O_WRONLY | os.O_CREAT, 0o644)
                # os.dup2(fd, 200)
                # os.close(fd)

                # lock_fd = 200
                # try:
                # fcntl.flock(lock_fd, fcntl.LOCK_EX)
                # lock_ = True

                # kill inotify wait process results and restart
                if checksum and xRC:

                    os.makedirs(cdir, mode=0o700, exist_ok=True)

                    fk_success = process_kill(pid, watchdog_pid_file)

                    # a partial write could occur but would get parsed out and is insignificant this avoids the use of locks currently

                    created = rotate_cache(cfr, CACHE_F, logger)

                    # if os.path.isfile(inotify_creation_file):
                    #   all_files = parse_tout(inotify_creation_file, checksum, logger)
                    # open(inotify_creation_file, 'w').close()

                    if fk_success and not process_by_target(search_pattern):
                        strup(
                            script_dir, script, appdata_local, home_dir, inotify_creation_file, CACHE_F, cdir, watchdog_pid_file, lockfile,
                            log_file, ll_level, _time, user, moduleNAME, usrDIR, temp_dir, gnupg_home, supbrwLIST, debug_mode, algo,
                            logger, platform
                        )
                    else:
                        if fk_success:
                            logger.debug("init_recentchanges inotifywait was already running continuing")  # log unusual event

                # the setting was turned off kill inotify wait
                else:

                    fk_success = process_kill(pid, watchdog_pid_file)

                if not fk_success:
                    logger.debug("init_recentchanges _fk_process did not report success for inotifywait termination")  # log second unusual event
                # except OSError as e:
                #     logger.error(f"Failed to acquire lock: {e}")
                # finally:
                #     if lock_:
                #         fcntl.flock(lock_fd, fcntl.LOCK_UN)
                #     os.close(lock_fd)

            elif platform == "windows":

                # lock_file = open(lockfile, "w")

                # try:

                # msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
                # lock_ = True
                if checksum and xRC:

                    fk_success = process_kill(pid, watchdog_pid_file)

                    created = rotate_cache(cfr, CACHE_F, logger)

                    if fk_success and not process_by_target(search_pattern):
                        strup(
                            script_dir, script, appdata_local, home_dir, inotify_creation_file, CACHE_F, cdir, watchdog_pid_file, lockfile,
                            log_file, ll_level, _time, user, moduleNAME, usrDIR, temp_dir, gnupg_home, supbrwLIST, debug_mode, algo,
                            logger, platform
                        )
                    else:
                        if fk_success:
                            logger.debug("init_recentchanges inotifywait was already running continuing")

                else:

                    fk_success = process_kill(pid, watchdog_pid_file)

                if not fk_success:
                    logger.debug("init_recentchanges _fk_process did not report success for inotifywait termination")
                # except OSError as e:
                #     logger.error(f"Failed to acquire lock: {e}")
                # finally:
                #     if lock_:
                #         msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                #     lock_file.close()

        # first start
        elif checksum and xRC:
            if platform == "linux":
                os.makedirs(cdir, mode=0o700, exist_ok=True)
            strup(
                script_dir, script, appdata_local, home_dir, inotify_creation_file, CACHE_F, cdir, watchdog_pid_file, lockfile,
                log_file, ll_level, _time, user, moduleNAME, usrDIR, temp_dir, gnupg_home, supbrwLIST, debug_mode, algo,
                logger, platform
            )

    except Exception as e:
        logger.error(f"Error in xRC error: {e} {type(e).__name__}", exc_info=True)

    return created

# end xRC functions
