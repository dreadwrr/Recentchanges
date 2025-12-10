import logging
import sqlite3
import traceback
from .dirwalkerfnts import encr_cache
from .pyfunctions import increment_f
from .query import clear_conn
from .query import clear_table
from .query import create_sys_table
from .query import create_table_cache
from .query import get_sys_changes
from .query import insert_cache
from .query import table_exists
from .query import table_has_data
from .query import update_cache
from .rntchangesfunctions import encr
from .rntchangesfunctions import getnm
from .rntchangesfunctions import get_idx_tables
from .rntchangesfunctions import intst


# insert changes into sys2 or sys2_n table. sys or sys_n table have originals.
# ie for C:\\ sys2, sys for S:\\ sys2_s, sys_s,  sys_n for n drive
def syncdb(dbopt, basedir, CACHE_S, parsedsys, parsedidx, sys_records, keys=None, from_idx=False):

    systimeche = getnm(CACHE_S)
    sys_tables, cache_table = get_idx_tables(basedir)
    res = False
    conn = cur = None

    try:
        conn = sqlite3.connect(dbopt)
        cur = conn.cursor()

        # scan IDX
        if sys_records:
            res = increment_f(conn, cur, sys_tables, sys_records)

        # build IDX
        elif parsedsys:

            drive_sys_table = sys_tables[0]

            if table_exists(conn, drive_sys_table):
                clear_table(drive_sys_table, conn, cur, True)

            create_sys_table(conn, sys_tables)
            create_table_cache(conn, cache_table, ('filename',))
            create_table_cache(conn, systimeche, ('filename',))

            if table_has_data(conn, systimeche):
                clear_table(systimeche, conn, cur, True)
            if table_has_data(conn, cache_table):
                clear_table(cache_table, conn, cur, True)

            with conn:

                cur.executemany(f"""
                    INSERT OR IGNORE INTO {drive_sys_table} (
                        timestamp, filename, creationtime, inode, accesstime,
                        checksum, filesize, symlink, owner, domain, mode,
                        casmod, lastmodified, hardlinks, count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, parsedsys)

                if parsedidx:
                    cur.executemany(f"""
                        INSERT OR IGNORE INTO {cache_table} (
                            modified_time, filename, file_count, idx_count,
                            max_depth, type, target
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, parsedidx)

                    cur.execute(f"""
                        INSERT INTO {systimeche} (
                            modified_time, filename, file_count, idx_count,
                            max_depth, type, target
                        )
                        SELECT modified_time, filename, file_count, idx_count,
                            max_depth, type, target
                        FROM {cache_table}
                    """)
                res = True

        # Find downloads add index
        elif from_idx and parsedidx:

            if table_has_data(conn, systimeche):
                clear_table(systimeche, conn, cur, True)
            create_table_cache(conn, systimeche, ('filename',))

            res = insert_cache(parsedidx, systimeche, conn)
            if not res:
                print(f"Failed to insert parsedidx for table {systimeche} drive {basedir} re syncdb")

        # Find download update index
        elif from_idx and keys:

            res = update_cache(keys, conn, systimeche)
            if not res:
                print(f"failed to update {systimeche} table for drive index for drive {basedir} in syncdb. dirwalkersrg.py")

            # if maintaining a full index can add remove but chance of desync
            # cur.executemany("DELETE FROM sys WHERE filepath = ?", del_keys)
            # conn.commit()
        else:
            print("Incorrect parameters for syncdb function dirwalkersrg.py. returning False")

        return res

    except sqlite3.Error as e:
        emsg = f"Database error syncdb in dirwalkersrg: {type(e).__name__} {e}"
        logging.error(emsg, exc_info=True)
        return False
    except Exception as e:
        emsg = f"Unexpected error in syncdb: {type(e).__name__} {e}"
        print(f"{emsg}  \n{traceback.format_exc()}")
        logging.error(emsg, exc_info=True)
        return False
    finally:
        clear_conn(conn, cur)


def save_db(dbopt, dbtarget, basedir, CACHE_S, email, parsedsys, parsedidx, sys_records, keys=None, idx_drive=False, compLVL=200, dcr=False):
    if syncdb(dbopt, basedir, CACHE_S, parsedsys, parsedidx, sys_records, keys, idx_drive):

        nc = intst(dbopt, compLVL)
        if encr(dbopt, dbtarget, email, nc, dcr):
            return True
        else:
            print("Reencryption of database failed.")
    return False


def index_drive(dbopt, dbtarget, basedir, parsedsys, parsedidx, dir_data, CACHE_S, error_message, email, idx_drive=False, compLVL=200, dcr=False):

    if save_db(dbopt, dbtarget, basedir, CACHE_S, email, parsedsys, parsedidx, None, None, idx_drive, compLVL, dcr):
        if dir_data:
            if encr_cache(dir_data, CACHE_S, email):
                return 0
            else:
                print(error_message)
        else:
            return 0
    else:
        print("Failed to sync db. index_system from dirwalkersrg")
    return 4


def db_sys_changes(dbopt, sys_tables):
    conn = None
    cur = None
    try:
        conn = sqlite3.connect(dbopt)
        cur = conn.cursor()
        sys_a, sys_b = sys_tables

        if not table_has_data(conn, sys_a):
            return False

        recent_sys = get_sys_changes(cur, sys_a, sys_b)
        return recent_sys

    except (sqlite3.Error, Exception) as e:
        print(f"Problem retrieving profile data for system index in db_sys_changes dirwalkersrg. database {dbopt} {type(e).__name__} error: {e}")
    finally:
        clear_conn(conn, cur)
    return None
