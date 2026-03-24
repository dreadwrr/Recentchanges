import csv
import os
import stat
from io import StringIO
from .dirwalkerwin import return_info
from .fileops import calculate_checksum
from .fileops import find_link_target
from .fileops import is_reparse_point
from .fileops import set_stat
from .gpgcrypto import decrm
from .logs import emit_log
# 03/16/2026

fmt = "%Y-%m-%d %H:%M:%S"
execEXTN = (".exe", ".msi", ".bat", ".com")


# Cache read
def decr_cache(CACHE_S, user=None):
    if not CACHE_S or not os.path.isfile(CACHE_S):
        return None

    csv_path = decrm(CACHE_S)
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


def get_base_folders(basedir, EXCLDIRS_FULLPATH):
    c = 0
    base_folders = []
    if os.path.isdir(basedir):
        c += 1
        base_folders.append(basedir)

    for folder_name in os.listdir(basedir):
        folder_path = os.path.join(basedir, folder_name)
        if folder_path in EXCLDIRS_FULLPATH:
            continue
        if os.path.isdir(folder_path):
            c += 1
            base_folders.append(folder_path)
    return base_folders, c


def create_profile_baseline(execEXTN):
    """ build list format so can differentiate between psEXTN """
    # template
    #     "exec exe msi bat com",

    extn = []

    exec_str = ' '.join(execEXTN)
    extn.append("exec " + exec_str)
    return extn


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
