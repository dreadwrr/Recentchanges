import csv
import io
import re
import sqlite3
import subprocess
from datetime import datetime, timezone
# 03/21/2026

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
        reason_str = row.get('Reason #')
        attrs_str = row.get('File attributes #')
        if not reason_str or not attrs_str:
            logger.debug("Skipping partial flags row: %r", row)
            continue
        try:
            reason_num = int(reason_str, 16)
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


def output_mft():
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
                # if record[17] in ('None', 'Hidden|System', None):
                #     continue
                if record[9] != "1":
                    continue
                # record[17] Si Flags
                # record[2] is_dir, record[13] is_ads

                if record[2] == 'True' and record[13] == 'False':

                    # in use and not alternate datastream

                    entry = int(record[0])
                    seq = int(record[1])
                    frn = (seq << 48) | entry
                    parent = int(record[3])
                    parent_seq = int(record[4])
                    parent_frn = (parent_seq << 48) | parent

                    # get back from frn # entry = frn & 0xFFFFFFFFFFFF  # seq   = (frn >> 48) & 0xFFFF
                    # def frn_to_entry(frn_str):
                    #     entry_id = frn_str[-16:]
                    #     entry_hex = entry_id[4:]
                    #     return int(entry_hex, 16)

                    name = record[6]
                    if not name:
                        continue
                    # f unknonw input but input is clean <
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
                    else:
                        # or file
                        timestamp = record[21]
                        ts = parse_datetime(timestamp)

                        creation_time = record[19]
                        c_time = parse_datetime(creation_time)

                        fc.append((frn, parent_frn, ts, c_time, name))

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


def rebuild_paths(dirs):
    updates = []
    walked = {}

    for frn, dir_info in dirs.items():
        old_path = dir_info.get("path")

        if frn in walked:
            new_path = walked[frn]

        else:
            parent_frn = dir_info["parent"]

            if frn == parent_frn:
                new_path = dir_info["path"]
                walked[frn] = new_path
            else:
                path_parts = [dir_info["name"]]
                new_path = None
                seen = {frn}

                while parent_frn in dirs and parent_frn not in seen:
                    if parent_frn in walked:
                        suffix = "\\".join(reversed(path_parts))
                        new_path = walked[parent_frn]
                        if suffix:
                            new_path = new_path.rstrip("\\") + "\\" + suffix
                        walked[frn] = new_path
                        break

                    seen.add(parent_frn)
                    parent = dirs[parent_frn]

                    if parent_frn == parent["parent"]:
                        walked[parent_frn] = parent["path"]
                        path_parts.append(parent["path"])
                        break

                    path_parts.append(parent["name"].rstrip("\\"))
                    parent_frn = parent["parent"]

                if new_path is None:
                    new_path = "\\".join(reversed(path_parts))
                    walked[frn] = new_path

        if old_path != new_path:
            dir_info["path"] = new_path
            updates.append((new_path, frn))

    return updates


# parse mft, retrieve cache, mark usn and commit
def build_up(conn, cur, last_processed, logger):
    if not baseline(conn, cur):
        print("starting fault failed initial parse of mft")
        return None

    dirs = load_dirs(cur)
    if not dirs:
        print("no directory entries in dir_ table couldnt load cache. quitting")
        return None
    root_frn = None
    root_path = None
    for frn, d in dirs.items():
        if d["parent"] not in dirs:
            root_frn = frn
            root_path = d["path"]
            break
        if d["parent"] == frn:
            root_path = d["path"]
            break
    if root_frn is not None:
        logger.debug("Root directory present %s %s", root_frn, root_path)
    elif root_path is not None:
        logger.debug("Root directory present %s", root_path)

    jid, _, next_usn = return_journal_id()
    if not jid or not next_usn:
        print("problem getting journal id and or next_usn from fsutil")
        return None
    close_commit(conn, cur, jid, next_usn, last_processed)
    return True


def reload_(conn, cur, last_processed, logger):
    if clear_all(conn, cur, logger):
        create_table_usn_state(cur)
        print("building up")
        if build_up(conn, cur, last_processed, logger):
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

# ctime sql

# you can store a history of usn snapshots to piece together. only useful for detailed implementation
# where debugging is critical

# for debugger
# usn_history table
# usn #
# frn
# parent frn
# name
# reason #
# event
# timestamp


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
        name TEXT NOT NULL
    )
    """)

    c.execute("""
    CREATE INDEX IF NOT EXISTS idx_dir_created_parent
    ON files_current (parent_frn)
    """)


def create_table_usn_state(c):
    c.execute("""
    CREATE TABLE IF NOT EXISTS usn_state (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        journal_id INTEGER NOT NULL UNIQUE,
        next_usn INTEGER NOT NULL,
        last_usn INTEGER,
        last_updated INTEGER
    )
    """)


def last_entry(cur):
    cur.execute("""
    SELECT journal_id, next_usn, last_usn
    FROM usn_state
    ORDER BY last_updated DESC
    LIMIT 1
    """)
    return cur.fetchone()


def get_created(cur, search_start_dt, logger):
    file_records = []

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
    SELECT d.path || '\\' || f.name, timestamp
    FROM files_current f
    JOIN dir_ d ON f.parent_frn = d.frn
    WHERE f.creationtime >= ?
    """, (compt,))
    records = cur.fetchall()

    if records:

        for entry in records:

            file_path = entry[0]
            mod_time = entry[1]
            # creationtime = entry[2]

            try:
                if not mod_time:
                    logger.debug("ctime.py mtime was null: %s entry : %s", mod_time, entry)
                    continue
                mod_time = str(mod_time / 1_000_000)
                # if creationtime:
                #     creationtime = str(creationtime / 1_000_000)
                # st = os.lstat(file_path)
                # if is_reparse_point(st):
                #     sym = "y"
                # attrs = getattr(st, "st_file_attributes", 0)
                # mode = get_mode(attrs, sym)
                # atime = st.st_atime
                # ino = None
                # sym = None
                # hardlink = None
                # size = st.st_size
                atime = None
                c_time = None
                ino = None
                sym = None
                hardlink = None
                size = None
                owner = None
                domain = None
                mode = None
                file_records.append((mod_time, atime, c_time, ino, sym, hardlink, size, owner, domain, mode, file_path))
            except (ValueError, TypeError) as e:
                logger.debug("ctime.py invalid timestamp mod_time %s file: %s entry : %s err %s", mod_time, file_path, entry, e)
                continue
            # except PermissionError:
            #     logger.debug("ctime.py Permission denied could not stat file: %s entry : %s", file_path, entry)
            #     continue
            # except OSError as e:
            #     logger.debug("ctime.py Error stating mft Skipping file: %x entry : %s \n %s err: %s", file_path, entry, type(e).__name__, e)
            #     continue
    return file_records


def baseline(conn, cur):
    create_table_dir_(cur)
    create_fc_table(cur)

    dir_, fc = output_mft()
    if not dir_:
        return None
    # hardlinks reparses share same entry # and cause sql unique constraint error
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

    cur.executemany("""
        INSERT INTO dir_ (frn, parent_frn, path, name)
        VALUES (?, ?, ?, ?)
    """, dir_)
    cur.executemany("""
        INSERT INTO files_current (frn, parent_frn, timestamp, creationtime, name)
        VALUES (?, ?, ?, ?, ?)
    """, fc)
    return True


def close_commit(conn, cur, jid, next_usn, last_processed):
    now_ts = int(datetime.now(timezone.utc).timestamp() * 1_000_000)
    cur.execute("""
        INSERT OR REPLACE INTO usn_state (journal_id, next_usn, last_usn, last_updated)
        VALUES (?, ?, ?, ?)
        """, (jid, next_usn, last_processed, now_ts))
    conn.commit()


def clear_all(conn, cur, logger):
    cur_tbl = ""
    try:
        for tbl in ('usn_state', 'dir_', 'files_current'):
            cur_tbl = tbl
            cur.execute(f"DROP TABLE IF EXISTS {tbl}")
        conn.commit()
        return True
    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Failed clearing {cur_tbl} table {type(e).__name__}: {e}", exc_info=True)
        return False
# end ctime sql
