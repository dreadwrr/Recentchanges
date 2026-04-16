import csv
import io
import os
import re
import sqlite3
import struct
import subprocess
from datetime import datetime, timezone
from typing import Iterator, Dict, Any
from .fsearchfunctions import get_mft_mode
from .fsearchfunctions import get_mode
from .pyfunctions import epoch_to_date
# 04/14/2026

MAX_NAME = 1024

# Little-endian, standard sizes, no alignment in Python format string.
# handle the C padding explicitly.
#
# 7x uint64_t  = 56
# 2x uint32_t  = 8   -> 64
# 3x uint16_t  = 6   -> 70
# 3x uint8_t   = 3   -> 73
# char[1024]   = 1024
# total        = 1097
#
# name starts immediately after is_ads and there is no extra internal padding there.
FIXED_PREFIX_FORMAT = "<QQQQQQQIIHHHBBB"
FIXED_PREFIX_SIZE = struct.calcsize(FIXED_PREFIX_FORMAT)
STRUCT_SIZE = FIXED_PREFIX_SIZE + MAX_NAME

if FIXED_PREFIX_SIZE != 73:
    raise RuntimeError(f"Unexpected prefix size: {FIXED_PREFIX_SIZE}")

FILE_CREATE = 0x00000100
FILE_DELETE = 0x00000200
RENAME_OLD = 0x00001000
RENAME_NEW = 0x00002000
CLOSE = 0x80000000
DIR_ATTR = 0x00000010
FILE_ATTR = 0x00002020
FILE_ATTR_REPARSE_POINT = 0x400
DATA_OVERWRITE = 0x00000001
DATA_EXTEND = 0x00000002
DATA_TRUNCATION = 0x00000004


FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",
    "%m/%d/%Y %H:%M:%S"
]


class UsnReadError(Exception):
    def __init__(self, message, code=None):
        super().__init__(message)
        self.code = code


def return_journal_id():
    """ on startup read id to see if changed. also the next usn for next search """
    cmd = ['fsutil', 'usn', 'queryjournal', 'c:']
    # cmd = ['fsutil', 'usn', 'readjournal', 'c:', 'csv']  # | findstr /i /C:"`"File create`"" > “log.log”  # similar to grep
    res = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='ignore'
    )

    jid = None
    first_usn = next_usn = None
    ret = res.returncode
    if ret == 0 and res.stdout:
        for line in res.stdout.splitlines():
            if "Usn Journal ID" in line:
                jid = int(line.split(":")[1].strip(), 16)
            elif "First Usn" in line:
                first_usn = int(line.split(":")[1].strip(), 16)
            elif "Next Usn" in line:
                next_usn = int(line.split(":")[1].strip(), 16)
    elif ret != 0:
        stderr = res.stderr
        print("error in return_journal error_code:", ret)
        if stderr:
            print(stderr)
        return None, None, None
    return jid, first_usn, next_usn


def read_usn(start_usn: int, logger):
    cmd = ['fsutil', 'usn', 'readjournal', 'c:', f'startusn={start_usn}', 'csv']

    started = False

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8',
        errors='ignore'
    )

    if proc.stdout is None:
        return

    fieldnames = None
    for line in proc.stdout:
        m = re.match(r"Error (\d+): (.+)", line)
        if m:
            code = int(m.group(1))
            msg = m.group(2)
            raise UsnReadError(msg, code=code)

        if line.startswith("Usn,"):
            fieldnames = next(csv.reader([line]))
            break
    else:
        proc.wait()
        return

    reader = csv.DictReader(proc.stdout,  fieldnames=fieldnames)

    for row in reader:
        if not row:
            continue
        if not row.get('Usn') or not row.get('File ID') or not row.get('Parent file ID'):
            logger.debug("Skipping partial USN row: %r", row)
            continue
        reason_num_str = row.get('Reason #')
        reason_str = row.get('Reason')
        attrs_str = row.get('File attributes #')
        if not reason_num_str or not attrs_str:
            logger.debug("Skipping partial flags row: %r", row)
            continue
        try:
            reason_num = int(reason_num_str, 16)
            file_attrs = int(attrs_str, 16)
            usn = int(row['Usn'])
        except (KeyError, ValueError) as e:
            logger.error("read_usn Bad row getting reason_num file_attrs row: %s Error: %s: %s", row, type(e).__name__, e)
            continue

        started = True

        yield {
            'usn': usn,
            'name': row['File name'],
            'name_len': int(row['File name length']),
            'reason_num': reason_num,
            'reason': reason_str,
            'timestamp': row['Time stamp'],
            'file_attrs': file_attrs,
            'frn': row['File ID'],
            'parent_frn': row['Parent file ID'],
            'is_directory': bool(file_attrs & DIR_ATTR),
            'is_reparse': bool(file_attrs & FILE_ATTR_REPARSE_POINT),
            'is_create': bool(
                (reason_num & FILE_CREATE) and
                not (reason_num & ~(FILE_CREATE | CLOSE))),
            'is_delete': bool(reason_num & FILE_DELETE),
            'is_rename_old': bool(reason_num & RENAME_OLD),
            'is_rename_new': bool(reason_num & RENAME_NEW),
            'is_close': bool(reason_num & CLOSE),
            'is_extend': bool(reason_num & DATA_EXTEND),
            'is_overwrite': bool(reason_num & DATA_OVERWRITE),
            'is_truncate': bool(reason_num & DATA_TRUNCATION)
        }

    proc.stdout.close()
    rlt = proc.wait()
    if rlt != 0:
        err = ""
        if proc.stderr is not None:
            err = proc.stderr.read().strip()
        em = "fsutil readjournal failed" + err
        print(em)
        logger.error(em)
    if not started:
        logger.debug("No USN rows returned after header")


def parse_datetime(value):
    if not value:
        return None

    s = str(value).strip()

    for fmt in FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1_000_000)
        except ValueError:
            print("failed to convert with first format", fmt, "for", s, "using alternative")
            continue

    return None


def parse_entry(chunk: bytes) -> Dict[str, Any]:
    if len(chunk) != STRUCT_SIZE:
        raise ValueError(f"Expected {STRUCT_SIZE} bytes, got {len(chunk)}")

    values = struct.unpack(FIXED_PREFIX_FORMAT, chunk[:FIXED_PREFIX_SIZE])
    name_buf = chunk[FIXED_PREFIX_SIZE:FIXED_PREFIX_SIZE + MAX_NAME]

    (
        frn,
        parent_frn,
        size,
        creation_time,
        modification_time,
        mft_modification_time,
        access_time,
        record_number,
        file_attribs,
        sequence_number,
        hard_link_count,
        name_len,
        in_use,
        is_dir,
        has_ads,
    ) = values

    if name_len > MAX_NAME:
        raise ValueError(f"Invalid name_len {name_len} > MAX_NAME {MAX_NAME}")

    name = name_buf[:name_len].decode("utf-8", errors="replace")

    return {
        "frn": frn,
        "parent_frn": parent_frn,
        "size": size,
        "creation_time": creation_time,
        "modification_time": modification_time,
        "mft_modification_time": mft_modification_time,
        "access_time": access_time,
        "record_number": record_number,
        "file_attribs": file_attribs,
        "sequence_number": sequence_number,
        "hard_link_count": hard_link_count,
        "name_len": name_len,
        "in_use": bool(in_use),
        "is_dir": bool(is_dir),
        "has_ads": bool(has_ads),
        "name": name,
    }


def process_entry(pipe) -> Iterator[Dict[str, Any]]:
    while True:
        chunk = pipe.read(STRUCT_SIZE)
        if chunk and len(chunk) != STRUCT_SIZE:
            print("SHORT CHUNK:", len(chunk), repr(chunk[:100]))
            raise RuntimeError(f"Expected {STRUCT_SIZE} bytes, got {len(chunk)}")
        if not chunk:
            break

        if len(chunk) != STRUCT_SIZE:
            raise RuntimeError(
                f"Truncated stream: expected {STRUCT_SIZE} bytes, got {len(chunk)}"
            )

        yield parse_entry(chunk)


def build_tuple(proc):

    dirs = {}
    fc = []

    for entry in process_entry(proc.stdout):

        frn = entry["frn"]
        parent_frn = entry["parent_frn"]
        name = entry["name"]

        if entry["is_dir"]:

            dirs[frn] = {
                "parent": parent_frn,
                "name":  name
            }

        # file
        else:

            timestamp = entry["modification_time"]
            ts = (timestamp / 10) - 11644473600000000

            creation_time = entry["creation_time"]
            c_time = (creation_time / 10) - 11644473600000000

            size = entry["size"]

            attrs = entry["file_attribs"]
            mode, symlink = get_mode(attrs)

            hardlinks = entry["hard_link_count"]

            fc.append((frn, parent_frn, ts, c_time, name, size, symlink, mode, hardlinks))
        proc.stdout.read(7)  # 1097 bytes read but its 1104 byte reads add padding of 7
    return dirs, fc


def build_dict(proc):

    dirs = {}
    fc = {}

    for entry in process_entry(proc.stdout):

        frn = entry["frn"]
        parent_frn = entry["parent_frn"]
        name = entry["name"]

        if entry["is_dir"]:

            dirs[frn] = {
                "parent": parent_frn,
                "name":  name
            }

        # file
        else:

            fc[frn] = entry
        proc.stdout.read(7)  # 1097 bytes read but its 1104 byte reads add padding of 7
    return dirs, fc


def output_mft(exe_path: str, target: str, dict_output=False) -> Iterator[Dict[str, Any]]:

    proc = subprocess.Popen(
        [exe_path, target, "--parse"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert proc.stdout is not None
    assert proc.stderr is not None

    # fail here if wrong format or misalign
    header = proc.stdout.read(8)

    if len(header) != 8:
        proc.stdout.close()
        print("Failed to read record count from stream")
        return False

    # for progress indicating
    (record_count,) = struct.unpack("<Q", header)

    dirs = fc = None

    try:
        if not dict_output:
            dirs, fc = build_tuple(proc)
        else:
            dirs, fc = build_dict(proc)

    finally:
        proc.stdout.close()

    stderr_data = proc.stderr.read()
    proc.stderr.close()
    rc = proc.wait()

    if rc != 0:
        err = stderr_data.decode("utf-8", errors="replace")
        raise RuntimeError(f"Parser exited with code {rc}: {err}")

    return dirs, fc


def output_mftec():
    """ to build the directories on the system to be able to read usn journal. also secondary ctime of all files
        to use for ctime search """
    mft = 'C:\\$MFT'
    # cutoff = (datetime.now(timezone.utc) - timedelta(seconds=tmn))
    # df = cutoff.isoformat().replace("+00:00", "Z")  # for command str output
    cutoff = "2003-03-19T11:13:18Z"
    df = cutoff
    cmd = [
        '.\\bin\\MFTECmd.exe', '-f', mft, '--dt', 'yyyy-MM-dd HH:mm:ss.ffffff',
        '--cutoff', df, '--csv', 'C:\\', '--csvf', 'myfile2.csv'
    ]

    csv_started = False
    dirs, fc = [], []

    try:
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except FileNotFoundError:
            print(f"Error: Could not find command {cmd[0]}.")
            return None, None
        except Exception as e:
            print(f"Failed to start process: {type(e).__name__} : {e}")
            return None, None

        assert proc.stdout is not None

        for line in iter(proc.stdout.readline, ''):
            if not line.strip():
                continue

            if not csv_started:
                if "EntryNumber,SequenceNumber,InUse" in line:
                    csv_started = True
                continue

            if ',' not in line:
                continue

            try:
                record = next(csv.reader(io.StringIO(line)))
            except Exception:
                continue

            if len(record) >= 24:

                # if record[5].startswith('.\\$'):
                #    continue
                # record[17] Si Flags
                # if record[17] in ('None', 'Hidden|System', None):
                #     continue
                if record[9] != "1":
                    continue

                # record[2] is_dir, record[13] is_ads
                if record[2] == 'True' and record[13] == 'False':

                    # in use and not alternate datastream

                    entry = int(record[0])
                    seq = int(record[1])
                    frn = (seq << 48) | entry
                    parent = int(record[3])
                    parent_seq = int(record[4])
                    parent_frn = (parent_seq << 48) | parent

                    # get back from frn
                    # entry = frn & 0xFFFFFFFFFFFF
                    # seq   = (frn >> 48) & 0xFFFF

                    # def frn_to_entry(frn_str):
                    #     entry_id = frn_str[-16:]
                    #     entry_hex = entry_id[4:]
                    #     return int(entry_hex, 16)

                    name = record[6]
                    if not name:
                        continue

                    # if unknown input but input is clean <
                    # if name:
                    #     name = name.lstrip('\\') <
                    # else:
                    #     continue <

                    # directory
                    if record[11] == 'True':
                        path = record[5]
                        if path == ".":
                            path = "C:\\"
                        elif path.startswith("."):
                            path = "C:\\" + path.lstrip(".\\")
                            # path = re.sub(r'^\.(\\)?', r'C:\\', path)
                            # path = p_frm.rstrip('\\') <

                        dirs.append((frn, parent_frn, path, name))

                    # file
                    else:

                        timestamp = record[21]
                        ts = parse_datetime(timestamp)

                        creation_time = record[19]
                        c_time = parse_datetime(creation_time)

                        size = record[8]

                        mode_attribs = record[17]
                        mode, symlink = get_mft_mode(mode_attribs)

                        hardlinks = record[9]

                        fc.append((frn, parent_frn, ts, c_time, name, size, symlink, mode, hardlinks))

        proc.stdout.close()
        res = proc.wait()

        if res == 0:
            return dirs, fc
        else:
            if proc.stderr is not None:
                err_output = proc.stderr.read()
                if err_output:
                    print(f"Failed. Unable to output csv with mftecmd.exe: {err_output}")

    except Exception as e:
        print(f'Unexpected err in outputmft func: {type(e).__name__} : {e}')
    return None, None


def load_dirs(c):
    dirs = {}

    for frn, parent_frn, name, path in c.execute(
        "SELECT frn, parent_frn, name, path FROM dir_"
    ):
        dirs[frn] = {
            "parent": parent_frn,
            "name": name,
            "path": path
        }

    return dirs


def resolve_frn(frn, dir_info, dirs, walked):
    parent_frn = dir_info["parent"]

    if frn == parent_frn:
        return "C:"

    path_parts = [dir_info["name"]]
    seen = {frn}

    while parent_frn in dirs and parent_frn not in seen:
        if parent_frn in walked:
            suffix = "\\".join(reversed(path_parts))
            new_path = walked[parent_frn]
            if suffix:
                new_path = new_path.rstrip("\\") + "\\" + suffix
            return new_path

        seen.add(parent_frn)
        parent = dirs[parent_frn]

        if parent_frn == parent["parent"]:
            path_parts.append("C:")
            break

        path_parts.append(parent["name"].rstrip("\\"))
        parent_frn = parent["parent"]

    return "\\".join(reversed(path_parts))


def build_dirs_tup(dirs):
    all_results = []
    walked = {}

    for frn, dir_info in dirs.items():

        parent_frn = dir_info.get("parent")
        name = dir_info.get("name")

        if frn not in walked:
            walked[frn] = resolve_frn(frn, dir_info, dirs, walked)

        path = walked[frn]

        all_results.append((frn, parent_frn, path, name))

    return all_results


def build_paths(dirs):
    """ takes a dict and returns a dict of paths """
    all_results = {}
    walked = {}

    for frn, dir_info in dirs.items():

        parent_frn = dir_info.get("parent")
        name = dir_info.get("name")

        if frn not in walked:
            walked[frn] = resolve_frn(frn, dir_info, dirs, walked)

        path = walked[frn]

        all_results[frn] = {
            "parent": parent_frn,
            "name": name,
            "path": path
        }

    return all_results


def rebuild_paths(dirs):
    """ takes a dict, rebuilds paths then returns the difference between the two for db upsert """
    walked = {}
    updates = []

    for frn, dir_info in dirs.items():

        old_path = dir_info.get("path")

        if frn not in walked:
            walked[frn] = resolve_frn(frn, dir_info, dirs, walked)

        new_path = walked[frn]

        if old_path != new_path:
            dir_info["path"] = new_path
            updates.append((new_path, frn))

    return updates


# parse mft, retrieve cache, mark usn and commit
def build_up(appdata_local, exe_path, basedir, conn, cur, logger):

    dirs = baseline(appdata_local, exe_path, basedir, conn, cur)

    if not dirs:
        print("no directory entries in dir_ table couldnt load cache. quitting")
        # print("starting fault failed initial parse of mft")
        return None

    # sample database specifically for entry # 5 or root

    # dirs = load_dirs(cur)
    # if not dirs:
    #     print("no directory entries in dir_ table couldnt load cache. quitting")
    #     return None
    # root_frn = None
    # root_path = None
    # for frn, d in dirs.items():
    #     if d["parent"] not in dirs:
    #         root_frn = frn
    #         root_path = d["path"]
    #         break
    #     if d["parent"] == frn:
    #         root_path = d["path"]
    #         break
    # if root_frn is not None:
    #     logger.debug("Root directory present %s %s", root_frn, root_path)
    # elif root_path is not None:
    #     logger.debug("Root directory present %s", root_path)

    jid, _, next_usn = return_journal_id()
    if not jid or not next_usn:
        em = "problem getting journal id and or next_usn from fsutil"
        print(em)
        logger.error(f"{em} jid {jid} next_usn {next_usn}")
        return None

    last_processed = history_start = None
    close_commit(conn, cur, jid, next_usn, last_processed, history_start)

    return True


def reload_(appdata_local, exe_path, basedir, conn, cur, logger):
    if clear_all(conn, cur, logger):
        create_table_usn_state(cur)
        print("building up")
        if build_up(appdata_local, exe_path, basedir, conn, cur, logger):
            print("success")
            return "reset"
        return None
    return "db_error"


def handle_rename(old_parent, old_name, new_parent, new_name):
    """ print for history\\debug """
    if old_parent == new_parent and old_name != new_name:
        return "RENAME"
    if old_parent != new_parent and old_name == new_name:
        return "MOVE"
    if old_parent != new_parent and old_name != new_name:
        return "MOVE_RENAME"
    return "UNKNOWN"


def get_created(cur, base_dir, search_start_dt, FEEDBACK, EXCLDIRS, logger):
    file_records = []
    buffer = []
    BATCH_SIZE = 5
    EXCLDIRS_FULLPATH = set(os.path.join(base_dir, d.lstrip("\\")) for d in EXCLDIRS)

    # initial concept and start point
    # if table_has_data(conn, "usn_state"):  # state is built
    # can check if the search time is too far back but this would be for different model. system state is built
    # cur.execute('''
    #     SELECT 1
    #     FROM files_current
    #     WHERE timestamp < ?
    #     LIMIT 1
    # ''', (compt,))
    # has_prior_data = cur.fetchone()

    #  find created files

    # current_time = datetime.now()
    # search_start_dt = (current_time - timedelta(minutes=search_time))
    compt = int(search_start_dt.timestamp() * 1_000_000)

    cur.execute("""
    SELECT d.path || '\\' || f.name, timestamp, creationtime, f.frn, size, symlink, mode, hardlinks
    FROM files_current f
    JOIN dir_ d ON f.parent_frn = d.frn
    WHERE f.creationtime >= ?
    """, (compt,))
    records = cur.fetchall()

    if records:

        for entry in records:

            atime = owner = domain = None

            file_path = entry[0]
            if file_path in EXCLDIRS_FULLPATH:
                continue

            mod_time = entry[1]
            change_time = entry[2]
            ino = entry[3]
            size = entry[4]
            symlink = entry[5]
            mode = entry[6]
            hardlinks = entry[7]

            try:
                if not mod_time:
                    logger.debug("ctime.py mtime was null: %s entry : %s", mod_time, entry)
                    continue
                mod_time = str(mod_time / 1_000_000)

                c_time = None
                if change_time:
                    c_time = str(change_time / 1_000_000)
                    c_time = epoch_to_date(c_time)

                # can do any other stats ect here

                if len(buffer) >= BATCH_SIZE:
                    print("\n".join(buffer), flush=True)
                    buffer.clear()
                if FEEDBACK:
                    buffer.append(file_path)
                file_records.append((mod_time, atime, c_time, ino, symlink, hardlinks, size, owner, domain, mode, file_path))
            except (ValueError, TypeError) as e:
                logger.debug("ctime.py invalid timestamp mod_time %s file: %s entry : %s err %s", mod_time, file_path, entry, e)
                continue
            except PermissionError:
                logger.debug("ctime.py Permission denied could not stat file: %s entry : %s", file_path, entry)
                continue
            except OSError as e:
                logger.debug("ctime.py Error stating mft Skipping file: %x entry : %s \n %s err: %s", file_path, entry, type(e).__name__, e)
                continue
        if buffer:
            print("\n".join(buffer))
    return file_records


def baseline(appdata_local, exe_path, basedir, conn, cur):

    target = basedir.rstrip("\\")

    create_table_dir_(cur)
    create_fc_table(cur)
    create_table_usn_history(cur)

    dir_, fc = output_mft(exe_path, target)  # parser.exe
    # dir_, fc = output_mftec()  # MFTECmd.exe

    if not dir_:
        print("starting fault failed initial parse of mft")
        return None

    # find any duplicates in the mft
    # hardlinks or reparses share same entry # as well as ADS or IsAds and cause sql unique constraint error

    # seen, dups = set(), set()
    # for row in fc:
    #     frn = row[0]
    #     if frn in seen:
    #         dups.add(frn)
    #     else:
    #         seen.add(frn)
    # print("duplicate file FRNs:", len(dups))
    # print(list(dups)[:20])
    # for d in dups:
    #     entry = d & 0xFFFFFFFFFFFF
    #     seq = (d >> 48) & 0xFFFF
    #     print("entry", entry)
    #     print("sequence", seq)

    dirs = build_dirs_tup(dir_)

    # ctime sql

    cur.executemany("""
        INSERT INTO dir_ (frn, parent_frn, path, name)
        VALUES (?, ?, ?, ?)
    """, dirs)

    cur.executemany("""
        INSERT INTO files_current (frn, parent_frn, timestamp, creationtime, name, size, symlink, mode, hardlinks)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, fc)

    return dirs


def close_commit(conn, cur, jid, next_usn, last_processed, history_start):

    now_utc = datetime.now(timezone.utc)
    now_ts = int(now_utc.timestamp() * 1_000_000)
    if not history_start:
        now_local = now_utc.astimezone()
        history_start = now_local.strftime("%Y-%m-%d %H:%M:%S")

    cur.execute("""
        INSERT OR REPLACE INTO usn_state (journal_id, next_usn, last_usn, last_updated, history_start)
        VALUES (?, ?, ?, ?, ?)
        """, (jid, next_usn, last_processed, now_ts, history_start))
    conn.commit()


def clear_all(conn, cur, logger):
    cur_tbl = ""
    try:
        for tbl in ('usn_state', 'usn_history', 'dir_', 'files_current'):
            cur_tbl = tbl
            cur.execute(f"DROP TABLE IF EXISTS {tbl}")
        conn.commit()
        return True
    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Failed clearing {cur_tbl} table {type(e).__name__}: {e}", exc_info=True)
        return False


def create_table_dir_(c):
    c.execute("""
    CREATE TABLE IF NOT EXISTS dir_ (
        frn INTEGER PRIMARY KEY,
        parent_frn INTEGER,
        path TEXT,
        name TEXT NOT NULL
    )
    """)

    c.execute("""
    CREATE INDEX IF NOT EXISTS idx_dir_parent
    ON dir_ (parent_frn)
    """)


def create_fc_table(c):
    """ files current """
    c.execute("""
    CREATE TABLE IF NOT EXISTS files_current (
        frn INTEGER PRIMARY KEY,
        parent_frn INTEGER,
        timestamp INTEGER,
        creationtime INTEGER,
        name TEXT NOT NULL,
        size INTEGER,
        symlink TEXT,
        mode TEXT,
        hardlinks INTEGER
    )
    """)

    c.execute("""
    CREATE INDEX IF NOT EXISTS idx_dir_created_parent
    ON files_current (parent_frn)
    """)


# id INTEGER PRIMARY KEY AUTOINCREMENT,
# journal_id INTEGER NOT NULL UNIQUE,
def create_table_usn_state(c):
    c.execute("""
    CREATE TABLE IF NOT EXISTS usn_state (
        journal_id INTEGER PRIMARY KEY,
        next_usn INTEGER NOT NULL,
        last_usn INTEGER,
        last_updated INTEGER,
        history_start TEXT
    )
    """)


# for debugger and no need for log file
def create_table_usn_history(c):
    c.execute("""
    CREATE TABLE IF NOT EXISTS usn_history (
        usn INTEGER PRIMARY KEY,
        frn INTEGER,
        parent_frn INTEGER,
        name TEXT,
        reason_num INTEGER,
        reason TEXT,
        timestamp TEXT,
        event TEXT,
        action_taken TEXT
    )
    """)

    c.execute("""
    CREATE INDEX IF NOT EXISTS idx_usn_history_frn
    ON usn_history(frn)
    """)
    c.execute("""
    CREATE INDEX IF NOT EXISTS idx_usn_history_timestamp
    ON usn_history(timestamp)
    """)


def last_entry(cur):
    cur.execute("""
    SELECT journal_id, next_usn, last_usn, history_start
    FROM usn_state
    ORDER BY last_updated DESC
    LIMIT 1
    """)
    return cur.fetchone()

# end ctime sql
