import csv
import logging
import multiprocessing
import os
import sqlite3
import traceback
from io import StringIO
from .fsearchfnts import calculate_checksum
from .fsearchfnts import defaultm
from .fsearchfnts import get_file_id
from .fsearchfnts import get_mode
from .fsearchfnts import get_onr
from .fsearchfnts import issym
from .pyfunctions import dict_string
from .pyfunctions import dict_to_list_sys
from .pyfunctions import epoch_to_str
from .rntchangesfunctions import decrm
from .rntchangesfunctions import encrm
from .pyfunctions import clear_conn
from .query import table_has_data


# Cache read
def decr_cache(CACHE_S):
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
            continue

        cfr_src[root] = {
            'modified_time': str(row.get('modified_time', '')),
            'modified_ep': float(row.get('modified_ep', 0.0)),
            'file_count': str(row.get('file_count', '0')),
            'idx_count': str(row.get('idx_count', '0')),
            'max_depth': str(row.get('max_depth', '0')),
            'type': str(row.get('type', '')),
            'target': str(row.get('target', ''))
        }

    return cfr_src

# Cache write
#


def encr_cache(dir_data, CACHE_S, email):
    data_to_write = dict_to_list_sys(dir_data)
    ctarget = dict_string(data_to_write)

    if encrm(ctarget, CACHE_S, email, False, False):
        return True
    return False


def chunk_split(recent_sys, batch_size=25, max_workers=8):

    worker_count = min(max_workers, multiprocessing.cpu_count() or 1)

    chunks = [[] for _ in range(worker_count)]
    worker_index = 0
    for i in range(0, len(recent_sys), batch_size):
        batch = recent_sys[i:i + batch_size]
        chunks[worker_index].extend(batch)

        worker_index = (worker_index + 1) % worker_count

    chunks = [c for c in chunks if c]  # remove empty chunks
    return chunks
# num_chunks = min(8, multiprocessing.cpu_count() or 1)
# total_items = len(recent_sys)
# chunk_size = math.ceil(total_items / num_chunks)

# chunks = [
#     recent_sys[i:i + chunk_size]
#     for i in range(0, total_items, chunk_size)
# ]


def none_if_empty(value):
    if value == "":
        return None
    else:
        return value


def get_dir_mtime(dirpath, locale):
    try:
        modified_time = os.stat(dirpath).st_mtime
        modified_opt = epoch_to_str(modified_time)
        return modified_opt, modified_time
    except Exception as e:
        logging.debug(f"get_dir_mtime from {locale} access denied indexing directory on {dirpath}: {e}")
    return None, None


# verification logging os.walk err handler
def os_walk_error(list_error):
    if isinstance(list_error, PermissionError):
        logging.debug("os.walk Permission denied: %s, skipping...", list_error.filename)
    elif isinstance(list_error, OSError):
        logging.debug("os.walk Error accessing in a root folder: %s", list_error)
    else:
        logging.debug("os.walk Unexpected error: %s", list_error)
        raise


def get_base_folders(base_dir, EXCLDIRS_FULLPATH):
    c = 0
    base_folders = []
    if os.path.isdir(base_dir):
        c += 1
        base_folders.append(base_dir)
    for folder_name in os.listdir(base_dir):
        folder_path = os.path.join(base_dir, folder_name)
        if folder_path in EXCLDIRS_FULLPATH:
            continue
        if os.path.isdir(folder_path):
            c += 1
            base_folders.append(folder_path)
    return base_folders, c


# os.walk
# For Scan IDX meta
# same as above but pass checksum. And fewer fields since they are not needed for index scan.
def meta_sys(file_path, file_name, md5, updatehlinks, sys_data, record):
    f_n = "meta_sys"
    size = None
    hardlink = None
    cas = None  # record[9]
    lastmodified = None  # record[11]
    count = record[14] + 1
    try:

        st = file_path.stat()

        checks = md5
        sym = "y" if issym(file_path) else None
        mode = get_mode(file_name, st, sym)
        inode, hardlink, _ = get_file_id(file_name, updatehlinks)
        result = get_onr(file_name)
        owner, domain = result if result else (None, None)
        m_epoch = st.st_mtime
        c_epoch = st.st_ctime
        a_epoch = st.st_atime
        m_time = epoch_to_str(m_epoch)
        c_time = epoch_to_str(c_epoch)
        a_time = epoch_to_str(a_epoch)

        if not mode:
            mode = defaultm(sym)
        size = st.st_size
        sys_data.append((m_time, file_name, c_time, inode, a_time, checks, size, sym, owner, domain, mode, cas, lastmodified, hardlink, count))
    except PermissionError as e:
        logging.debug(f"{f_n} Permission error on: {file_path} err: {e}")
    except FileNotFoundError as e:
        logging.debug(f"{f_n} File not found: {file_name} err: {e}")
    except Exception as e:
        logging.error(f"{f_n} Problem getting metadata: {e}", exc_info=True)

    return size


# os.walkdir meta stat creating path object
# for Build IDX meta
def walk_meta(file_path, file_name, updatehlinks, sys_data):
    f_n = "walk_meta"
    count = 1  # init version #
    size = None
    cam = None
    lastmodified = None
    try:

        st = file_path.stat()

        checks = calculate_checksum(file_path)
        sym = "y" if issym(file_path) else None
        mode = get_mode(file_name, st, sym)
        inode, hardlink, _ = get_file_id(file_name, updatehlinks)
        # hardlink = str(st.st_nlink) linux
        result = get_onr(file_name)
        owner, domain = result if result else (None, None)
        m_epoch = st.st_mtime
        c_epoch = st.st_ctime
        a_epoch = st.st_atime
        m_time = epoch_to_str(m_epoch)
        c_time = epoch_to_str(c_epoch)
        a_time = epoch_to_str(a_epoch)

        if not mode:
            mode = defaultm(sym)

        size = st.st_size
        sys_data.append((m_time, file_name, c_time, inode, a_time, checks, size, sym, owner, domain, mode, cam, lastmodified, hardlink, count))
    except PermissionError as e:
        logging.debug(f"{f_n} Permission error on: {file_name} {e}")
    except FileNotFoundError as e:
        logging.debug(f"{f_n} File not found: {file_name}: {e}")
    except Exception as e:
        logging.error(f"{f_n} Problem getting metadata skipped: {type(e).__name__}: {e}", exc_info=True)
    return size


def find_symmetrics(dbopt, cache_table, systimeche):
    cache_records = []
    has_systime = False
    conn = None
    cur = None
    try:
        conn = sqlite3.connect(dbopt)
        cur = conn.cursor()
        if table_has_data(conn, systimeche):
            has_systime = True
            query = f"""
                SELECT s.modified_time,
                    s.filename,
                    s.file_count,
                    s.max_depth
                FROM {systimeche} AS s
                WHERE s.file_count > 0
                AND s.type IS NOT NULL
                AND EXISTS (
                        SELECT 1
                        FROM {cache_table} AS c
                        WHERE c.filename = s.filename
                        AND c.file_count = 0
                        AND c.type IS NULL
                )
            """
            cur.execute(query)
            cache_records = cur.fetchall()
        else:
            query = f'''
                SELECT modified_time, filename, file_count, max_depth
                FROM {cache_table}
                WHERE file_count = 0 AND type is NULL
            '''
            cur.execute(query)
            records = cur.fetchall()
            if records:
                for record in records:
                    dirname = record[1]
                    if os.path.isdir(dirname):
                        try:
                            if any(entry.is_file() for entry in os.scandir(dirname)):
                                cache_records.append(record)
                        except (FileNotFoundError, PermissionError):
                            pass

        sql = f"""
        SELECT DISTINCT s.filename
        FROM {systimeche} s
        LEFT JOIN {cache_table} c ON s.filename = c.filename
        WHERE c.filename IS NULL
        """
        cur.execute(sql)
        new_records = [row[0] for row in cur.fetchall()]

        return cache_records, new_records
    except sqlite3.Error as e:
        errmsg = f"table {cache_table}" if not has_systime else f"tables {cache_table} {systimeche}"
        print(f"dirwalker.py problem retrieving data in find_symmetrics. database {dbopt} {errmsg} {type(e).__name__} error: {e}")
        return None, None
    except Exception as e:
        print(f"General error in find_symmetrics {type(e).__name__} error: {e} \n{traceback.format_exc()}")
        logging.error(f'find_symmetrics profile cache:{cache_table} cache table: {systimeche}  {type(e).__name__} error: {e}\n', exc_info=True)
        return None, None
    finally:
        clear_conn(conn, cur)
