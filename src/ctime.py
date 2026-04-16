#  tout implementation for Windows recentchanges. bypass 1 of 2 loop in main app. tout is the create files within the search
#  time. the reason for this is that downloaded files or copies of files can have preserved attributes and not show up in regular
#  searches. tout is merged with SORTCOMPLETE in the searches
#  04/14/2026
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from .ctimefunctions import build_up
from .ctimefunctions import close_commit
from .ctimefunctions import create_table_usn_state
from .ctimefunctions import get_created
from .ctimefunctions import handle_rename
from .ctimefunctions import last_entry
from .ctimefunctions import load_dirs
from .ctimefunctions import read_usn
from .ctimefunctions import rebuild_paths
from .ctimefunctions import reload_
from .ctimefunctions import return_journal_id
from .ctimefunctions import UsnReadError
from .fsearchfunctions import get_mode
from .pysql import clear_conn


def init_recentchanges(search_time, search_start_dt, FEEDBACK, EXCLDIRS, logging_values, search=False):

    # jrnl error code 1181 journal wrap

    # function returns:
    # reset new_build returned   on success
    # db_error if jam up on table drop
    # usn_error if unknown error code reading usn journal
    # None on hard failure

    appdata_local = logging_values[2]
    exe_path = logging_values[3]
    # exe_path = appdata_local / "bin" / "parser.exe"
    dbopt = str(appdata_local / "ctime.db")

    basedir = "C:\\"  # basedir = Path(appdata_local).anchor

    logger = logging.getLogger("CTIME")

    # logging.basicConfig(level=logging.DEBUG)  # print to terminal only

    # print to file
    # logger.setLevel(logging.DEBUG)
    # file_handler = logging.FileHandler("ctime.log")
    # formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    # file_handler.setFormatter(formatter)
    # console_handler = logging.StreamHandler()
    # console_handler.setFormatter(formatter)
    # logger.addHandler(file_handler)
    # logger.addHandler(console_handler)

    dirs = {}
    pending_files = {}
    pending_old = {}
    markups = {}
    set_seen = set()
    cur_jid = last_processed = first_ts = history_start = None
    jrnl_change = False
    started = False
    was_usn = False
    update_path = False

    conn = cur = None
    if not os.path.isfile(dbopt):
        print("Inititiazing ctime database...")

    try:
        conn = sqlite3.connect(dbopt)
        cur = conn.cursor()

        create_table_usn_state(cur)

        # query the usn_state
        recent = last_entry(cur)

        if not recent:
            print("building up")
            if not build_up(appdata_local, exe_path, basedir, conn, cur, logger):
                return None

            else:
                print("success")
                if search:

                    return get_created(cur, basedir, search_start_dt, FEEDBACK, EXCLDIRS, logger)
                return "new_build"

        else:
            cur_jid, last_usn, last_processed, history_start = recent  # retrieve from query. the last jid and last_usn. last_processed where we left off.

            jid, first_usn, next_usn = return_journal_id()  # read first_usn and compare to last_usn

            # unlikely but could happen?
            if next_usn == last_usn:
                print("same state nothing to do")
                if search:
                    return get_created(cur, basedir, search_start_dt, FEEDBACK, EXCLDIRS, logger)
                return "returned"

            # how long ago was it run just rebuild
            if last_processed and last_processed < first_usn:
                print(f"last entry {last_processed} expired was less than {first_usn}. building up")
                jrnl_change = True
            if cur_jid and cur_jid != jid:
                print("usn journal id has changed")
                jrnl_change = True
            if jrnl_change:
                # clear build reset
                result = reload_(appdata_local, exe_path, basedir, conn, cur, logger)  # returns "reset" or "db_error" or None
                if result == "reset" and search:
                    return get_created(cur, basedir, search_start_dt, FEEDBACK, EXCLDIRS, logger)
                return result

            print("loading dir_ cache.")

            dirs = load_dirs(cur)
            if not dirs:
                print("no directory entries in dir_ table")
                return None

            usn_index = last_usn
            if last_processed:
                usn_index = last_processed

            try:
                for rec in read_usn(usn_index, logger):
                    started = True
                    usn = int(rec['usn'])
                    if not was_usn and (last_processed and usn <= last_processed):
                        continue

                    last_processed = usn

                    # debug section
                    # print(last_processed)
                    # logger.debug(
                    #     "USN=%s FRN=%s PFRN=%s REASON=0x%08x NAME=%s",
                    #     rec["usn"], rec["frn"], rec["parent_frn"], rec["reason_num"], rec["name"]
                    # )

                    reason_num = rec['reason_num']  # for usn_history tbl
                    reason = rec['reason']
                    timestamp = rec['timestamp']  # .
                    # attrs = int(rec['file_attrs'], 16)  # .

                    frn = int(rec['frn'], 16)
                    parent_frn = int(rec['parent_frn'], 16)
                    name = rec['name']
                    is_dir = rec['is_directory']
                    reparse = rec['is_reparse']
                    #

                    event = ""
                    action = ""
                    action_taken = []

                    if rec['is_create']:
                        event = "Create"
                        if frn not in set_seen:
                            set_seen.add(frn)

                            if is_dir:
                                event = "Dir create"
                                cur.execute("""
                                    INSERT OR REPLACE INTO dir_ (frn, parent_frn, name, path)
                                    VALUES (?, ?, ?, ?)
                                """, (frn, parent_frn, name, None))
                                action_taken.append("Upsert dir_")

                                entry = dirs.get(frn)
                                if entry:
                                    entry["parent"] = parent_frn
                                    entry["name"] = name
                                    action = "dirs Updated"
                                else:
                                    dirs[frn] = {
                                        "parent": parent_frn,
                                        "name": name,
                                        "path": None,
                                    }
                                    action = "dirs Add"
                                update_path = True

                            else:
                                event = "File create"
                                pending_files[frn] = {
                                    "parent_frn": parent_frn,
                                    "name": name,
                                    "reparse": reparse
                                }
                                action = "pending files Updated"

                            action_taken.append(action)

                    if rec['is_rename_old']:
                        event = "Rename"
                        if frn not in pending_old:
                            pending_old[frn] = (parent_frn, name)
                            action_taken.append("pending old Add")

                    if rec['is_rename_new'] and not rec['is_close']:

                        old = pending_old.pop(frn, None)

                        if old:

                            old_parent_frn, old_name = old
                            event = handle_rename(old_parent_frn, old_name, parent_frn, name)

                        else:
                            event = "No rename old on resume"

                        if is_dir:

                            entry = dirs.get(frn)
                            if entry:
                                entry["parent"] = parent_frn
                                entry["name"] = name
                                action_taken.append("dirs Updated")
                            else:
                                dirs[frn] = {
                                    "parent": parent_frn,
                                    "name": name,
                                    "path": None
                                }
                                action_taken.append("dirs Add")
                            update_path = True

                            cur.execute("""
                                UPDATE dir_
                                SET parent_frn = ?, name = ?
                                WHERE frn = ?
                            """, (parent_frn, name, frn))
                            action = "Updated dir_"
                            if cur.rowcount == 0:
                                cur.execute("""
                                    INSERT INTO dir_ (frn, parent_frn, name, path)
                                    VALUES (?, ?, ?, ?)
                                """, (frn, parent_frn, name, None))
                                action = "Insert into dir_"
                            action_taken.append(action)
                        else:

                            if frn in pending_files:
                                pending_files[frn]["parent_frn"] = parent_frn
                                pending_files[frn]["name"] = name
                                action_taken.append("pending files Updated")

                            if frn in markups:
                                markups[frn]["parent_frn"] = parent_frn
                                markups[frn]["name"] = name
                                action_taken.append("markups Updated")
                            cur.execute("""
                                INSERT INTO files_current (frn, parent_frn, name)
                                VALUES (?, ?, ?)
                                ON CONFLICT(frn) DO UPDATE SET
                                    parent_frn = excluded.parent_frn,
                                    name = excluded.name
                            """, (frn, parent_frn, name))
                            action_taken.append("Upsert files_current")

                    if rec['is_delete']:

                        if is_dir:
                            event = "Dir delete"
                            if frn in dirs:
                                del dirs[frn]
                                action_taken.append("dirs Remove")
                            update_path = True

                            cur.execute("DELETE FROM dir_ WHERE frn = ?", (frn,))
                            action_taken.append("Delete from dir_")
                        else:
                            event = "File delete"
                            if frn in markups:
                                del markups[frn]
                                action_taken.append("markups Remove")
                            if frn in pending_files:
                                del pending_files[frn]
                                action_taken.append("pending files Remove")

                            cur.execute("DELETE FROM files_current WHERE frn = ?", (frn,))
                            action_taken.append("Delete from files_current")
                        set_seen.discard(frn)

                    if not rec['is_delete'] and not rec['is_close'] and (
                        rec['is_overwrite'] or
                        rec['is_extend'] or
                        rec['is_truncate']
                    ):
                        event = "Modified"
                        if frn in markups:
                            action_taken.append("markups Update")
                        else:
                            action_taken.append("markups Add")
                        markups[frn] = {
                            "parent_frn": parent_frn,
                            "name": name
                        }

                    dt = datetime.strptime(timestamp, "%m/%d/%Y %H:%M:%S")
                    ts_fixed = dt.strftime("%Y-%m-%d %H:%M:%S")  # to allow lexographic sorting

                    if not was_usn:
                        # first_ts = timestamp
                        first_ts = dt
                    was_usn = True

                    at = "| ".join(action_taken)
                    cur.execute("""
                        INSERT OR IGNORE INTO usn_history (usn, frn, parent_frn, name, reason_num, reason, timestamp, event, action_taken)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (usn, frn, parent_frn, name, reason_num, reason, ts_fixed, event, at))

                # usn_history trim

                if first_ts and history_start:

                    history_start_dt = datetime.strptime(history_start, "%Y-%m-%d %H:%M:%S")

                    delta = timedelta(days=7)

                    if first_ts - history_start_dt >= delta:

                        cutoff_dt = datetime.now()
                        cutoff = cutoff_dt - delta
                        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

                        cur.execute("""
                            DELETE FROM usn_history
                            WHERE timestamp < ?
                        """, (cutoff_str,))

                        history_start = cutoff_str

            except UsnReadError as e:
                print(f"{e} return_code: {e.code}")
                logger.error(e)

                if e.code == 1181:
                    result = reload_(appdata_local, exe_path, basedir, conn, cur, logger)
                    if result == "reset" and search:
                        return get_created(cur, basedir, search_start_dt, FEEDBACK, EXCLDIRS, logger)
                    else:
                        return result
                else:
                    return "usn_error"

            except Exception as e:
                em = f"failure in read_usn ctime.py while reading entries. {type(e).__name__} err: {e}"
                print(em)
                logger.error(em, exc_info=True)
                return None

            if update_path:

                updates = rebuild_paths(dirs)

                cur.executemany("""
                    UPDATE dir_
                    SET path = ?
                    WHERE frn = ?
                """, updates)

            for frn, info in pending_files.items():

                parent_frn = info["parent_frn"]
                name = info["name"]
                reparse = info["reparse"]

                if frn in markups:
                    del markups[frn]

                parent_info = dirs.get(parent_frn)
                if not parent_info:
                    continue
                path = parent_info.get("path")
                if not path:
                    continue

                path = path + "\\" + name

                mod_time = c_time = None
                try:

                    st = os.lstat(path)
                    mod_time = int(st.st_mtime_ns // 1_000)
                    c_time = int(st.st_ctime_ns // 1_000)  # c_time = getattr(st, "st_birthtime", st.st_ctime)

                    size = st.st_size

                    attrs = getattr(st, "st_file_attributes", 0)
                    mode, symlink = get_mode(attrs)

                    hardlinks = st.st_nlink

                    cur.execute("""
                        INSERT OR REPLACE INTO files_current
                        (frn, parent_frn, timestamp, creationtime, name, size, symlink, mode, hardlinks)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (frn, parent_frn, mod_time, c_time, name, size, symlink, mode, hardlinks))

                    # logger.debug(f"pending files Event processed, CREATE for file {path}")

                except (TypeError, ValueError) as e:
                    logging.debug("pending files improper timestamp mod_time %s c_time %s path %s err %s", mod_time, c_time, path, type(e).__name__, exc_info=True)
                except FileNotFoundError:
                    logging.debug("pending files file doesnt exist %s", path)
                except OSError as e:
                    logger.debug("pending files error stating created file: %s \n err: %s %s", path, type(e).__name__, e)

            for frn, info in markups.items():
                parent_frn = info["parent_frn"]
                parent_info = dirs.get(parent_frn)
                if not parent_info:
                    continue
                path = parent_info["path"] + "\\" + info["name"]
                try:
                    st = os.lstat(path)
                    mod_time = int(st.st_mtime_ns // 1_000)

                    cur.execute("""
                        UPDATE files_current
                        SET timestamp = ?
                        WHERE frn = ?
                    """, (mod_time, frn))
                    if cur.rowcount == 0:
                        logging.debug(f"markup logic error file doesnt exist file {path}")
                except (TypeError, ValueError) as e:
                    logging.debug("markup file improper timestamp mod_time %s path %s err %s", mod_time, path, type(e).__name__, exc_info=True)
                except FileNotFoundError:
                    logging.debug(f"markup file doesnt exist {path}")
                except OSError as e:
                    logger.debug("markup file error stating extended truncated file: %s \n err: %s %s", path, type(e).__name__, e)

            markups.clear()

            if not started:
                print("no return from usn jnrl read")
            elif not was_usn:
                print("all records already processed")
            else:
                print("processed journal entries.")

            close_commit(conn, cur, jid, next_usn, last_processed, history_start)
            print("\ndirs set")

        if search:

            return get_created(cur, basedir, search_start_dt, FEEDBACK, EXCLDIRS, logger)

        return True

    except Exception as e:
        msg = f"An error occured in ctime.py. {type(e).__name__} err: {e}"
        print(msg)
        logger.error(msg, exc_info=True)
        return None
    finally:
        clear_conn(conn, cur)
