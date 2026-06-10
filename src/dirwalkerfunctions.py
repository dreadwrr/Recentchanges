import csv
import logging
import os
import stat
from datetime import datetime
from io import StringIO
from .config import get_json_settings
from .dirwalkerwin import return_info
from .fileops import calculate_checksum
from .fileops import find_dir_link_target
from .fileops import find_link_target
from .fileops import is_reparse_point
from .fileops import set_stat
from .fsearchfunctions import get_file_id
from .gpgcrypto import decrm
from .logs import emit_log
from .pyfunctions import epoch_to_str
# 04/14/2026

fmt = "%Y-%m-%d %H:%M:%S"
EXEC_EXTN = (".exe", ".msi", ".bat", ".com")

MODE_FILENAME = 1
MODE_EXT = 2
MODE_FILENAME_EXT = 3


# Cache read
def decr_cache(cache_s, user=None):
    if not cache_s or not os.path.isfile(cache_s):
        return None

    csv_path = decrm(cache_s)
    if not csv_path:
        return None

    cfr_src = {}
    reader = csv.DictReader(StringIO(csv_path), delimiter='|')

    for row in reader:
        root = row.get('root')
        if not root:
            print("Warning missing systimeche missing a root key")
            continue

        modified_ep_s = row.get('modified_ep') or ''
        try:
            modified_ep = float(modified_ep_s) if modified_ep_s else None
        except ValueError:
            modified_ep = None
        if modified_ep is None:
            continue

        modified_ep_s = row.get('modified_ep') or ''
        cfr_src[root] = {
            'modified_time': str(row.get('modified_time', '')),
            'modified_ep': modified_ep,
            'file_count': str(row.get('file_count', '0')),
            'idx_count': str(row.get('idx_count', '0')),
            'idx_bytes': str(row.get('idx_bytes', '0')),
            'max_depth': str(row.get('max_depth', '0')),
            'type': str(row.get('type', '')),
            'target': str(row.get('target', ''))
        }

    return cfr_src


def chunk_split(recent_sys, list_length, batch_size=25):  # , max_workers=8

    return [recent_sys[i:i+batch_size] for i in range(0, list_length, batch_size)]

    # round robin batching
    # worker_count = min(max_workers, multiprocessing.cpu_count() or 1)

    # chunks = [[] for _ in range(worker_count)]
    # worker_index = 0
    # for i in range(0, len(recent_sys), batch_size):
    #     batch = recent_sys[i:i + batch_size]
    #     chunks[worker_index].extend(batch)

    #     worker_index = (worker_index + 1) % worker_count

    # chunks = [c for c in chunks if c]
    # return chunks

#
# above uses numpy because pandas uses it. if not numpy
# num_chunks = min(8, multiprocessing.cpu_count() or 1)
# total_items = len(recent_sys)
# chunk_size = math.ceil(total_items / num_chunks)
# chunks = [
#     recent_sys[i:i + chunk_size]
#     for i in range(0, total_items, chunk_size)
# ]


def flatten_dict(dir_data):
    # dict of dicts to flat tuples
    parsedidx = []
    for fldr, key_meta in dir_data.items():
        parsedidx.append((
            none_if_empty(key_meta.get('modified_time')),
            fldr,
            key_meta.get('file_count'),
            key_meta.get('idx_count'),
            key_meta.get('idx_bytes'),
            key_meta.get('max_depth'),
            none_if_empty(key_meta.get('type')),
            none_if_empty(key_meta.get('target'))
        ))
    return parsedidx


def none_if_empty(value):
    return value or None


def get_base_folders(basedir, exclDIRS_fullpath):
    c = 0
    base_folders = []
    if os.path.isdir(basedir):
        c += 1
        base_folders.append(basedir)

    for folder_name in os.listdir(basedir):
        folder_path = os.path.join(basedir, folder_name)
        if folder_path in exclDIRS_fullpath:
            continue
        if os.path.isdir(folder_path):
            c += 1
            base_folders.append(folder_path)
    return base_folders, c


def get_drive_type(basedir, driveTYPE, cache_s, json_file):
    """ from config return the drive type or default passed default driveTYPE """
    # _, suffix = parse_systimeche(basedir, cache_s)
    di = get_json_settings(None, basedir, json_file) or {}
    dtype = di.get("drive_type")
    if dtype in ("HDD", "SSD"):
        return dtype
    else:
        print("Warning entry for", basedir, "is malformed in json file:", json_file, "using default", driveTYPE)
    return driveTYPE


def create_profile_baseline(EXEC_EXTN):
    """ build list format so can differentiate between psEXTN """
    # template
    #     "exec exe msi bat com",

    extn = []

    exec_str = ' '.join(EXEC_EXTN)
    extn.append("exec " + exec_str)
    return extn


class ErrorHandler:
    def __init__(self, logger=None):
        self.logger = logger if logger else logging

    def __call__(self, list_error):
        if isinstance(list_error, PermissionError):
            self.logger.debug(
                "os.walk Permission denied: %s, skipping...",
                getattr(list_error, "filename", None)
            )
        elif isinstance(list_error, OSError):
            self.logger.debug("os.walk Error accessing in a root folder: %s", list_error)
        else:
            self.logger.debug("os.walk Unexpected error: %s", list_error)
            raise list_error


# os.scandir find
def files_search(base_dir, search_start_dt, feedback, exclDIRS, logger, filename=None, extension=None, search_type=None, iqt=False, strt=0, endp=100):

    if search_start_dt and not isinstance(search_start_dt, datetime):
        print("search_start_dt is not a valid date time object exitting")
        return None, 0

    # primary mode use process scan find created files by time for recentchangessearch
    # modes
    # process search find filename, extension or filename and extension and or by time for findfile

    def match_name(file_lower, filename, extension):
        return file_lower == filename

    def match_extn(file_lower, filename, extension):
        return (file_lower.endswith(extension))

    def match_name_extn(file_lower, filename, extension):
        base, ext = os.path.splitext(file_lower)
        return base.startswith(filename) and ext == extension

    all_entries = []
    buffer = []

    cckSEEN = set()

    max_depth = 0
    BATCH_SIZE = 5

    filename = filename.lower() if filename else None
    extension = extension.lower() if extension else None

    cutoff = None
    if search_start_dt:
        cutoff = search_start_dt.timestamp()

    # set any modes

    matcher = None
    if search_type:
        if search_type == MODE_FILENAME:
            matcher = match_name
        elif search_type == MODE_EXT:
            matcher = match_extn
        elif search_type == MODE_FILENAME_EXT:
            matcher = match_name_extn

    exclDIRS_fullpath = set(os.path.join(base_dir, d.lstrip("\\")) for d in exclDIRS)

    base_folders, root_count = get_base_folders(base_dir, exclDIRS_fullpath)
    if root_count == 0:
        print(f"Unable to read base folders of drive {base_dir} the drive could be empty or check permissions")
        return None, 0

    ino = hardlink = owner = domain = mode = None

    try:

        def process_search(root, matcher, current_depth=0, max_depth=0):

            rtype = None
            try:

                if root in cckSEEN:
                    return max_depth
                cckSEEN.add(root)

                max_depth = max(max_depth, current_depth)

                with os.scandir(root) as entries:
                    for entry in entries:

                        rtype = None
                        symlink = False

                        full_path = entry.path

                        try:

                            if entry.is_symlink():
                                symlink = True

                            if entry.is_dir():

                                if full_path in exclDIRS_fullpath:
                                    continue
                                stat_info = get_stat(entry, logger=logger)
                                if not stat_info:
                                    continue

                                if symlink:
                                    rtype = "symlink"
                                elif entry.is_junction():
                                    rtype = "junction"
                                elif is_reparse_point(stat_info):
                                    rtype = "reparse"

                                if not rtype:
                                    if full_path != base_dir:
                                        max_depth = process_search(full_path, matcher, current_depth + 1, max_depth)

                            elif entry.is_file():

                                # filename = entry.name
                                file_lower = entry.name.lower()

                                if matcher(file_lower, filename, extension):
                                    if cutoff:
                                        stat_info = get_stat(entry, logger=logger)
                                        if not stat_info:
                                            continue
                                        mtime = stat_info.st_mtime
                                        c_time = stat_info.st_birthtime
                                        if (mtime < cutoff and c_time < cutoff):
                                            continue

                                    if len(buffer) >= BATCH_SIZE:
                                        print("\n".join(buffer), flush=True)
                                        buffer.clear()
                                    if feedback:
                                        buffer.append(full_path)

                                    all_entries.append(full_path)

                        except OSError as e:
                            logger.error(f"files search process_search Exception scanning {'symlink' if symlink else ''} {full_path}: {type(e).__name__} {e}", exc_info=True)

            except PermissionError:
                logger.debug(f"files search process_search Permission denied scanning: {root}")
            except OSError as e:
                logger.error(f"files search process_search Exception scanning {root}: {type(e).__name__} {e}", exc_info=True)

            return max_depth

        def process_scan(root, current_depth=0, max_depth=0):

            rtype = None
            try:

                if root in cckSEEN:
                    return max_depth
                cckSEEN.add(root)

                max_depth = max(max_depth, current_depth)

                with os.scandir(root) as entries:
                    for entry in entries:

                        rtype = None
                        symlink = False

                        full_path = entry.path

                        try:

                            if entry.is_symlink():
                                symlink = True

                            if entry.is_dir():

                                if full_path in exclDIRS_fullpath:
                                    continue
                                stat_info = get_stat(entry, logger=logger)
                                if not stat_info:
                                    continue

                                if symlink:
                                    rtype = "symlink"
                                elif entry.is_junction():
                                    rtype = "junction"
                                elif is_reparse_point(stat_info):
                                    rtype = "reparse"

                                if not rtype:
                                    if full_path != base_dir:
                                        max_depth = process_scan(full_path, current_depth + 1, max_depth)

                            elif entry.is_file():

                                # filename = entry.name
                                stat_info = get_stat(entry, logger=logger)
                                if not stat_info:
                                    continue

                                mtime = stat_info.st_mtime
                                c_time = stat_info.st_birthtime

                                atime = stat_info.st_atime

                                size = stat_info.st_size

                                if (mtime >= cutoff or c_time >= cutoff):
                                    if len(buffer) >= BATCH_SIZE:
                                        print("\n".join(buffer), flush=True)
                                        buffer.clear()
                                    if feedback:
                                        buffer.append(full_path)

                                    all_entries.append((str(mtime), str(atime), c_time, ino, symlink, hardlink, size, owner, domain, mode, full_path))

                        except OSError as e:
                            logger.error(f"files search process_scan Exception scanning {'symlink' if symlink else ''} {full_path}: {type(e).__name__} {e}", exc_info=True)

            except PermissionError:
                logger.debug(f"files search process_scan Permission denied scanning: {root}")
            except OSError as e:
                logger.error(f"files search process_scan Exception scanning {root}: {type(e).__name__} {e}", exc_info=True)

            return max_depth

        f = 0
        prog_v = 0
        scale = current_step = 0
        steps = step_len = 0

        if iqt:
            scale = (endp - strt) / root_count
            n = min(10, root_count)
            steps = sorted(set(int(i * root_count / n) for i in range(n + 1)))
            step_len = len(steps)

        max_depth = 0
        for dir_path in base_folders:
            f += 1
            try:

                if not search_type:
                    d = process_scan(dir_path)

                else:
                    d = process_search(dir_path, matcher)

                if d > max_depth:
                    max_depth = d

                if iqt:
                    if current_step < step_len and f >= steps[current_step]:
                        prog_v = strt + (f * scale)
                        print(f"Progress: {prog_v:.2f}%", flush=True)

                        current_step += 1
            except OSError as e:
                emsg = f"Couldnt stat path {dir_path}: {type(e).__name__} err: {e}"
                print(emsg)
                logger.debug(emsg)
                continue
        if buffer:
            print("\n".join(buffer))
        if iqt and current_step <= len(steps) - 1:
            print(f"Progress: {endp:.2f}%", flush=True)

        return all_entries, max_depth

    except Exception as e:
        print(f"files_search Exception: {type(e).__name__} {e}")
        emit_log("ERROR", f"files_search file loop error {f}\\{root_count}, detected files_search line {f} of {root_count} : dir: {dir_path} {type(e).__name__} {e}", logger=logger)
        raise


# create a directory cache of the system
# return files by specification for multliprocessing hashing
def collect_files(basedir, exclDIRS_fullpath, filter_tup, exec_tup, extn_tup, paths_tup, is_noextension, is_exec, is_sym, logger):
    ''' proteusEXTN shield os.scandir '''
    all_entries = []
    log_entries = []
    dir_data = {}
    cckSEEN, idx_bytes = set(), set()

    try:

        def collect_scan(root, root_modified_dt=None, root_modified_ep=None, current_depth=0, max_depth=0, r=0, j=0):

            x = 0
            ix = 0
            idx_files = 0
            rtype = None
            try:

                if root in cckSEEN:
                    return max_depth, r, j
                cckSEEN.add(root)

                max_depth = max(max_depth, current_depth)

                with os.scandir(root) as entries:
                    for entry in entries:

                        rtype = target = None
                        symlink = False
                        found = False

                        path = entry.path

                        inode = None
                        modified_dt = None
                        modified_ep = None

                        try:

                            if entry.is_symlink():
                                symlink = True

                            if entry.is_dir():

                                if path in exclDIRS_fullpath:
                                    continue
                                stat_info = get_stat(entry, logger=logger)
                                if not stat_info:
                                    continue

                                if symlink:
                                    rtype = "symlink"
                                elif entry.is_junction():
                                    rtype = "junction"
                                elif is_reparse_point(stat_info):
                                    rtype = "reparse"

                                modified_ep = stat_info.st_mtime
                                modified_dt = epoch_to_str(modified_ep)

                                if not rtype:
                                    if path != basedir:
                                        max_depth, r, j = collect_scan(path, modified_dt, modified_ep, current_depth + 1, max_depth, r, j)
                                else:
                                    target = find_link_target(path, logger=logger)

                            elif entry.is_file():

                                if not (symlink and not is_sym):
                                    filename = entry.name
                                    x += 1
                                    j += 1

                                    if path.lower().startswith(filter_tup):
                                        continue

                                    if path.startswith(paths_tup):
                                        found = True
                                    else:
                                        if is_exec:
                                            filename_lower = filename.lower()
                                            if filename_lower.endswith(exec_tup):
                                                found = True
                                        else:
                                            if is_noextension:
                                                if "." not in filename or (filename.startswith(".") and filename.count(".") == 1):
                                                    found = True
                                            if not found:
                                                filename_lower = filename.lower()
                                                if filename_lower.endswith(extn_tup):
                                                    found = True

                                    if found:
                                        stat_info = get_stat(entry, logger=logger)

                                        if not stat_info:
                                            continue

                                        if symlink:
                                            target = find_link_target(path, logger=logger)

                                        else:
                                            if is_reparse_point(stat_info):
                                                symlink = True
                                                if symlink and not is_sym:
                                                    continue

                                        sze = stat_info.st_size
                                        dev = stat_info.st_dev
                                        if stat_info.st_nlink > 1:
                                            inode, _, _, _, _, _, status = get_file_id(path, logger=logger)
                                            if status in ("Nosuchfile", "Error"):
                                                continue
                                            elif inode:
                                                key = (dev, inode)
                                                if key not in idx_bytes:
                                                    idx_bytes.add(key)
                                                    ix += sze
                                            else:
                                                ix += sze
                                        else:
                                            ix += sze

                                        idx_files += 1
                                        r += 1

                                        all_entries.append((path, stat_info, symlink, target, found))
                            else:
                                if symlink:
                                    target = find_dir_link_target(path, logger=logger)
                                    if target:
                                        rtype = "symlink"
                                        stat_info = get_stat(entry, logger=logger)
                                        if not stat_info:
                                            logger.debug(f"could not stat broken dir symlink {path}")
                                            continue
                                        modified_ep = stat_info.st_mtime
                                        modified_dt = epoch_to_str(modified_ep)
                            if rtype:

                                entry_data = {
                                    'modified_time': modified_dt if modified_dt else '',
                                    'modified_ep': modified_ep,
                                    'file_count': 0,
                                    'idx_count': 0,
                                    'idx_bytes': 0,
                                    'max_depth': path.count(os.sep),
                                    'type': rtype,
                                    'target': target
                                }
                                dir_data[path] = entry_data

                        except OSError as e:
                            logger.error(f"collect_scan Exception scanning {'symlink' if symlink else ''} {path}: {type(e).__name__} {e}", exc_info=True)

                    entry_data = {
                        'modified_time': root_modified_dt if root_modified_dt else '',
                        'modified_ep': root_modified_ep,
                        'file_count': x,
                        'idx_count': idx_files,
                        'idx_bytes': ix,
                        'max_depth': root.count(os.sep),
                        'type': '',
                        'target': ''
                    }
                    dir_data[root] = entry_data

            except PermissionError:
                logger.debug(f"collect_scan Permission denied scanning: {root}")
            except OSError as e:
                logger.error(f"collect_scan Exception scanning {root}: {type(e).__name__} {e}", exc_info=True)

            return max_depth, r, j

        stat_info = os.stat(basedir)
        modified_ep = stat_info.st_mtime
        modified_dt = epoch_to_str(modified_ep)

        max_depth, r, j = collect_scan(basedir, modified_dt, modified_ep)

    except OSError as e:
        print(f"Couldnt stat unable to access drive {basedir}: {e}")
        return None, None, None, 0, 0, 0
    except Exception as e:
        emsg = f"collect_files Exception: {type(e).__name__} {e}"
        print(emsg)
        logger.error(f"{emsg}", exc_info=True)
        return None, None, None, 0, 0, 0

    return all_entries, dir_data, log_entries, max_depth, r, j


# os.scandir meta DirEntry object formerly walk_meta
# for Build IDX meta - either to specifications or XzmProfile template
# take initial stat. run the checksum then stat again to confirm hash.

def scandir_meta(file_path, st, symlink, link_target, found, sys_data, log_q=None):

    count = 1  # init version #
    status = None
    checks = size = cam = lastmodified = None

    try:

        file_info = return_info(file_path, st, symlink, link_target, log_q)

        sym, target, mode, inode, hardlink, owner, domain, m_dt, m_epoch_ns, m_time, c_time, a_time, size, status = file_info

        if status == "Nosuchfile":
            return False, status
        elif status == "Error":
            return None, status

        mtime_us = m_epoch_ns // 1_000

        if found and sym != "y":

            checks, file_dt, file_us, file_st, status = calculate_checksum(file_path, m_dt, mtime_us, inode, size, retry=2, max_retry=2, cacheable=False, log_q=log_q)

            if checks is not None:  # if status in ("Returned", "Retried"):
                if status == "Retried":
                    checks, mtime, st, mtime_us, c_time, inode, size = set_stat(file_info, checks, file_dt, file_st, file_us, inode, log_q)
                    if mtime is None:
                        emit_log("ERROR", f"scandir_meta Retried mtime was None skipping file {file_path}", log_q)
                        return None, status

                    m_time = mtime.strftime(fmt)
                    c_time = c_time.strftime(fmt) if c_time else None

            else:
                if status == "Nosuchfile":
                    return False, status

        # status in ("Returned", "Retried", "Changed"):
        sys_data.append((m_time, file_path, c_time, inode, a_time, checks, size, sym, owner, domain, mode, cam, target, lastmodified, hardlink, count, mtime_us))
        return True, status

    except PermissionError as e:
        emit_log("ERROR", f"scandir_meta Permission error on: {file_path} {e}", log_q)
        return None, status
    except FileNotFoundError:
        return False, "Nosuchfile"
    except Exception as e:
        emit_log("ERROR", f"scandir_meta Problem getting metadata skipped: {file_path} err:{type(e).__name__}: {e}", log_q)
        raise


# For Scan IDX meta
# same as above but have previous checksum of file. stat and hash each profile item and check to original to find any
# changes including modifications without a new modified time or faked modified time.
#
# a file could change to a symlink and vice versa. which wouldnt effect anything but is info that can be output for symmetric
# differences
# previous_symlink before
# and symlink\\sym after
#
def meta_sys(file_path, previous_md5, previous_symlink, previous_target, previous_count, is_sym, sys_data, link_data, log_q=None):

    status = None
    checks = size = hardlink = None

    target = None

    cam = None  # record[9]
    lastmodified = None  # record[11]
    count = previous_count + 1

    try:

        st = os.lstat(file_path)

        symlink = False
        if is_reparse_point(st):
            symlink = True
            if stat.S_ISLNK(st.st_mode):
                target = find_link_target(file_path, log_q)

        file_info = return_info(file_path, st, symlink, target, log_q)

        sym, target, mode, inode, hardlink, owner, domain, m_dt, m_epoch_ns, m_time, c_time, a_time, size, status = file_info

        if status == "Nosuchfile":
            return False, status
        elif status == "Error":
            return None, status

        if previous_symlink == "y" and sym != "y":
            emit_log("ERROR", f"meta_sys Warning symlink changed to file: {file_path}", log_q)
        mtime_us = m_epoch_ns // 1_000

        if sym != "y":

            checks, file_dt, file_us, file_st, status = calculate_checksum(file_path, m_dt, mtime_us, inode, size, retry=2, max_retry=2, cacheable=False, log_q=log_q)
            if checks is not None:  # if status in ("Returned", "Retried"):
                if status == "Retried":
                    checks, mtime, st, mtime_us, c_time, inode, size = set_stat(file_info, checks, file_dt, file_st, file_us, inode, log_q)
                    if mtime is None:
                        emit_log("ERROR", f"meta_sys Retried mtime was None skipping file {file_path}", log_q)
                        return None, status

                    m_time = mtime.strftime(fmt)
                    c_time = c_time.strftime(fmt) if c_time else None

                # status in ("Returned", "Retried"):
                if checks != previous_md5:
                    sys_data.append((m_time, file_path, c_time, inode, a_time, checks, size, sym, owner, domain, mode, cam, target, lastmodified, hardlink, count, mtime_us))

            else:  # status == "Nosuchfile" or status == "Changed"
                return False, status

        else:
            if is_sym and previous_symlink == "y":
                if target != previous_target:
                    link_data.append((m_time, file_path, c_time, inode, a_time, checks, size, sym, owner, domain, mode, cam, target, lastmodified, hardlink, count, mtime_us))
                    link_data.append((previous_target, target))
            elif not previous_symlink:
                emit_log("ERROR", f"meta_sys Warning file changed to symlink: {file_path}", log_q)

        return True, status

    except PermissionError as e:
        emit_log("ERROR", f"meta_sys Permission error on: {file_path} err: {e}", log_q)
        return None, status
    except FileNotFoundError:
        emit_log("DEBUG", f"file not found while scanning. file: {file_path}", log_q)
        return False, "Nosuchfile"
    except Exception as e:
        emit_log("ERROR", f"meta_sys Problem getting metadata skipped: {file_path} err:{type(e).__name__}: {e}", log_q)
        raise


def get_stat(entry, log_q=None, log_entries=None, logger=None):
    try:
        return entry.stat(follow_symlinks=False)
    except OSError as e:
        emit_log("DEBUG", f"OSError cannot stat  {type(e).__name__} {e} : {entry}", log_q, log_entries, logger)
        return None


def get_filter_tup(suppress_list):
    sup_set = set()
    for s in suppress_list:
        if s:
            sup_set.add(s.lower())
    return tuple(sup_set)


def check_specified_paths(basedir, configured_paths, list_name, suppress=False):
    paths = set()
    exists = []  # valid system paths
    missing = []  # inform

    for p in configured_paths:
        full = os.path.join(basedir, p)
        if os.path.isdir(full):
            paths.add(full)
            exists.append(p)
        else:
            missing.append(full)

    if not suppress and missing:
        # missing = [p[len(basedir):].lstrip(os.sep) for p in missing]  # absolute
        print(
            f"\nWarning: The following {list_name} do not exist, removed and continuing: "
            f'{", ".join(missing)}'
        )
    return tuple(paths), exists
