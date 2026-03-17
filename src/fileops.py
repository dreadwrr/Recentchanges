import hashlib
import logging
import os
import stat
import win32con
import win32file
from .logs import emit_log
from .pyfunctions import epoch_to_date


def find_link_target(file_path, log_q=None, log_entries=None, logger=None):

    target = resolve_target(file_path, log_q, log_entries, logger)
    if target and not os.path.exists(target):
        target = f"broken {target}"
    elif not target:
        emit_log("DEBUG", f"Symlink target was None : {file_path}", log_q, log_entries, logger)

    return target


def resolve_target(file_path, log_q=None, log_entries=None, logger=None):

    try:
        # absolute = os.path.realpath(file_path)  # 2 method
        target = os.readlink(file_path)
        base = os.path.dirname(file_path)
        absolute = os.path.abspath(os.path.join(base, target))
        return absolute
    except (OSError, ValueError) as e:
        emit_log("DEBUG", f"resolve_target checking symlink target file: {file_path} {type(e).__name__} error: {e}", log_q, log_entries, logger)
        return None


def find_dir_link_target(dirpath, log_q=None, log_entries=None, logger=None):
    try:
        target = os.readlink(dirpath)
        if target and target.endswith(os.sep):
            target = f"broken {target}"
            base = os.path.dirname(dirpath)
            return os.path.abspath(os.path.join(base, target))
    except OSError as e:
        emit_log("DEBUG", f"Error checking for broken dir symlinks {dirpath}: {e}", log_q, log_entries, logger)
    return None


def is_reparse_point(st):
    return (
        stat.S_ISLNK(st.st_mode) or
        bool(getattr(st, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT)
    )


def get_reparse(path):
    try:
        attrs = win32file.GetFileAttributes(path)
        if not (attrs & win32con.FILE_ATTRIBUTE_REPARSE_POINT):
            return None
        return "symlink"
    except Exception as e:
        logging.debug(f"Failed to get reparse type for {path}: {type(e).__name__} {e}")
        return None


def set_stat(line, checks, file_dt, file_st, file_us, inode, log_q=None, logger=None):

    mtime = file_dt
    mtime_us = file_us

    creation_time = file_st.st_birthtime
    c_time = epoch_to_date(creation_time)
    size_int = file_st.st_size

    a_ino = file_st.st_ino

    if a_ino != inode:
        checks = None
        if isinstance(line, tuple):
            line = ', '.join(map(str, line))
        emit_log("DEBUG", f"set_stat file inode changed {a_ino} vs {inode} discarding checksum for line {line}", log_q, logger=logger)

    return checks, mtime, file_st, mtime_us, c_time, a_ino, size_int


def normalize_to_us(mod_time):
    """ slice ns to us """
    s = str(mod_time).strip()
    return int(s[:-3] or "0")


def calculate_checksum(file_path, mtime, mod_time, inode, size_int, prev_hash=None, st=None, retry=1, max_retry=1, cacheable=True, log_q=None, logger=None):

    total_size = 0

    try:
        hash_func = hashlib.md5()
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                hash_func.update(chunk)
                total_size += len(chunk)

        checks = hash_func.hexdigest()

        if prev_hash is not None:
            if checks == prev_hash:
                return checks, mtime, mod_time, st, "Retried"

        if retry > 0:

            if not total_size:
                emit_log("INFO", f"calculate_checksum Size was zero: {file_path} checksum {checks} and total_size {total_size}", log_q, logger=logger)

            re_st = goahead(file_path, log_q, logger=logger)
            if re_st == "Nosuchfile":
                emit_log("INFO", f"calculate_checksum file not found: {file_path}", log_q, logger=logger)
                return None, mtime, mod_time, st, "Nosuchfile"

            elif re_st:

                a_mod = re_st.st_mtime_ns // 1000
                a_size = re_st.st_size
                a_ino = re_st.st_ino

                if total_size == size_int and mod_time == a_mod and inode and int(inode) == a_ino:
                    status = "Returned"
                    if prev_hash:
                        mtime = epoch_to_date(re_st.st_mtime)
                        status = "Retried"
                    return checks, mtime, mod_time, st, status
                else:
                    emit_log("INFO", f"File changed from first stat. the file is Cacheable: {cacheable} doesnt match: {file_path} the follow characteristics: ", log_q, logger=logger)
                    emit_log("INFO", f"Retry #{retry}\\{max_retry}. Entry mtime {mod_time} size {size_int} inode {inode}", log_q, logger=logger)
                    emit_log("INFO", f"calculate_checksum checksum size is {total_size} . mtime {a_mod} size {a_size} inode {a_ino}", log_q, logger=logger)

                mtime = epoch_to_date(re_st.st_mtime)
                return calculate_checksum(file_path, mtime, a_mod, a_ino, a_size, checks, re_st, retry - 1, max_retry, cacheable, log_q, logger=logger)

        emit_log("INFO", f"calculate_checksum file changed returning None: {file_path}", log_q, logger=logger)

        return None, mtime, mod_time, st, "Changed"

    except FileNotFoundError:
        emit_log("INFO", f"calculate_checksum file not found while calculating checksum: {file_path}", log_q, logger=logger)
        return None, mtime, mod_time, st, "Nosuchfile"
    except PermissionError as e:
        emit_log("DEBUG", f"calculate_checksum: {file_path} error: {e}", log_q, logger=logger)
    except OSError as e:
        emit_log("DEBUG", f"calculate_checksum: {file_path} {type(e).__name__} error: {e}", log_q, logger=logger)
    except Exception as e:
        emit_log("ERROR", f"Exception calculating checksum for file: {file_path} total_size {total_size} size_int {size_int} error: {e}", log_q, logger=logger)
        raise

    return None, mtime, mod_time, st, "Error"


def sha256_sum(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def get_md5(file_path):
    try:
        hash_func = hashlib.md5()
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                hash_func.update(chunk)
        return hash_func.hexdigest()
    except Exception:
        return None


def truncate_to_6_digits(timestamp):
    """ rounds last digit """
    return float(f"{timestamp:.6f}")


def goahead(file_path, log_q=None, log_entries=None, logger=None):
    try:
        return os.lstat(file_path)
    except FileNotFoundError:
        return "Nosuchfile"
    except OSError as e:
        emit_log("DEBUG", f"goahead Skipping: {file_path} {type(e).__name__} error: {e} \n", log_q, log_entries, logger)
    return None


def hlink_count(st=None, file_path=None, log_q=None, log_entries=None, logger=None):
    try:
        if st is None and file_path is None:
            emit_log("DEBUG", "hlink_count no args given. returning None", log_q, log_entries, logger)
            return None

        if not st and file_path:
            if hasattr(file_path, "lstat"):
                st = file_path.lstat()
            else:
                st = os.stat(file_path, follow_symlinks=False)

        if st:
            return st.st_nlink

    except Exception as e:
        emit_log("DEBUG", f"hlink_count could not get hardlinks {f'file: {file_path}' if file_path else ''} {type(e).__name__} error: {e}", log_q, log_entries, logger)
    return None
