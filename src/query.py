# 12/02/2025                               Developer buddy stats
import os
import sqlite3
import sys
import tempfile
from collections import Counter
from datetime import datetime
from .pyfunctions import clear_conn
from .pyfunctions import get_delete_patterns
from .pyfunctions import is_integer
from .rntchangesfunctions import decr
from .rntchangesfunctions import getnm
from .rntchangesfunctions import cprint


def clear_cache(conn, cur, usr):

    files_d = get_delete_patterns(usr)

    try:
        for filename_pattern in files_d:
            cur.execute("DELETE FROM logs WHERE filename LIKE ?", (filename_pattern,))
            cur.execute("DELETE FROM stats WHERE filename LIKE ?", (filename_pattern,))
            conn.commit()
        print("Cache files cleared.")
        return True
    except sqlite3.Error as e:
        conn.rollback()
        print(f"cache_clear query.py failed to write to db. {e}")
    except Exception as e:
        conn.rollback()
        print(f'General failure in query.py clear_cache: {e}')
    return False


def execute_query(dbopt, sql, params=None, iqt=False):

    conn = None
    cur = None
    try:
        conn = sqlite3.connect(dbopt)
        cur = conn.cursor()

        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)

        return cur.fetchall()

    except (sqlite3.Error, Exception) as e:
        print(f"Problem retrieving data for dirwalker.py in execute_query. database {dbopt} {type(e).__name__} error: {e}")
        return None
    finally:
        clear_conn(conn, cur)


def insert(log, conn, c, table, last_column, add_column=None):  # Log, sys/sys_n

    columns = [
        'timestamp', 'filename', 'creationtime', 'inode', 'accesstime',
        'checksum', 'filesize', 'symlink', 'owner', 'domain', 'mode',
        'casmod', 'lastmodified', last_column
    ]

    if add_column:
        columns.append(add_column)

    placeholders = ', '.join(['TRIM(?)'] * len(columns))
    col_str = ', '.join(columns)
    c.executemany(
        f'INSERT OR IGNORE INTO {table} ({col_str}) VALUES ({placeholders})',
        log
    )

    if table == 'logs':
        blank_row = tuple([None] * len(columns))
        c.execute(
                f'INSERT INTO {table} ({col_str}) VALUES ({", ".join(["?"]*len(columns))})',
                blank_row
        )

    conn.commit()


def insert_if_not_exists(action, timestamp, filename, creationtime, conn, c):  # Stats
    timestamp = timestamp or None
    c.execute('''
    INSERT OR IGNORE INTO stats (action, timestamp, filename, creationtime)
    VALUES (?, ?, ?, ?)
    ''', (action, timestamp, filename, creationtime))
    conn.commit()


def insert_cache(log, table, conn):

    columns = [
            'modified_time', 'filename', 'file_count', 'idx_count', 'max_depth', 'type', 'target'
    ]
    placeholders = ', '.join(['?'] * len(columns))
    col_str = ', '.join(columns)
    sql = f'INSERT OR IGNORE INTO {table} ({col_str}) VALUES ({placeholders})'
    try:
        with conn:
            conn.executemany(sql, log)
        return True
    except sqlite3.Error as e:
        print(f"insert failed for table {table} in insert_cache dirwalker: {e}")
    return False


def update_cache(keys, conn, table):
    try:
        with conn:
            c = conn.cursor()
            columns = ['modified_time', 'filename', 'file_count', 'max_depth']
            placeholders = ', '.join(['?'] * len(columns))
            col_str = ', '.join(columns)

            sql = f'''
            INSERT INTO {table} ({col_str})
            VALUES ({placeholders})
            ON CONFLICT(filename) DO UPDATE SET
                modified_time = excluded.modified_time,
                file_count = excluded.file_count

            '''
            c.executemany(sql, keys)
            return True
    except sqlite3.Error as e:
        print(f"Error updating {table} table: {e} {type(e).__name__}")
    return False


def get_sys_changes(cursor, sys_a, sys_b):

    query = f"""
    SELECT
        timestamp,
        filename,
        creationtime,
        inode,
        accesstime,
        checksum,
        filesize,
        symlink,
        owner,
        domain,
        mode,
        casmod,
        lastmodified,
        hardlinks,
        count
    FROM {sys_b} AS b
    WHERE b.timestamp = (
        SELECT MAX(timestamp)
        FROM {sys_b}
        WHERE filename = b.filename
    )
    UNION ALL
    SELECT
        a.timestamp,
        a.filename,
        a.creationtime,
        a.inode,
        a.accesstime,
        a.checksum,
        a.filesize,
        a.symlink,
        a.owner,
        a.domain,
        a.mode,
        a.casmod,
        a.lastmodified,
        a.hardlinks,
        a.count
    FROM {sys_a} AS a
    WHERE NOT EXISTS (
        SELECT 1
        FROM {sys_b} AS b
        WHERE b.filename = a.filename
    )
    """

    cursor.execute(query)
    combined_rows = cursor.fetchall()
    return combined_rows


def table_has_data(conn, table_name):
    c = conn.cursor()
    c.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name=?
    """, (table_name,))
    if not c.fetchone():
        c.close()
        return False
    c.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
    res = c.fetchone() is not None
    c.close()
    return res


def dbtable_has_data(dbopt, table_name):
    conn = None
    cur = None
    try:
        conn = sqlite3.connect(dbopt)
        return table_has_data(conn, table_name)

    except sqlite3.OperationalError:
        return False
    except (sqlite3.Error, Exception) as e:
        print(f"Problem with {dbopt}:", e)
        return False
    finally:
        clear_conn(conn, cur)


def create_logs_table(c, unique_columns, add_column=None):
    columns = [
        'id INTEGER PRIMARY KEY AUTOINCREMENT',
        'timestamp TEXT',
        'filename TEXT',
        'creationtime TEXT',
        'inode INTEGER',
        'accesstime TEXT',
        'checksum TEXT',
        'filesize INTEGER',
        'symlink TEXT',
        'owner TEXT',
        'domain TEXT',
        'mode TEXT',
        'casmod TEXT',
        'lastmodified TEXT',
        'hardlinks TEXT'
    ]
    if add_column:
        columns.append(f'{add_column} INTEGER')

    col_str = ',\n      '.join(columns)
    unique_str = ', '.join(unique_columns)
    sql = f'''
    CREATE TABLE IF NOT EXISTS logs (
    {col_str},
    UNIQUE({unique_str})
    )
    '''
    c.execute(sql)

    sql = 'CREATE INDEX IF NOT EXISTS'

    c.execute(f'{sql} idx_logs_checksum ON logs (checksum)')
    c.execute(f'{sql} idx_logs_filename ON logs (filename)')
    c.execute(f'{sql} idx_logs_checksum_filename ON logs (checksum, filename)')  # Composite


def create_sys_variant(c, table_name, columns, unique_columns):
    col_str = ',\n      '.join(columns)
    unique_str = ', '.join(unique_columns)
    sql = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
      {col_str},
      UNIQUE({unique_str})
    )
    '''
    c.execute(sql)

    c.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_filename ON {table_name} (filename)')
    if table_name.startswith('sys2'):
        c.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_checksum ON {table_name} (checksum)')
        c.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_checksum_filename ON {table_name} (checksum, filename)')  # Composite


def create_sys_table(c, sys_tables):
    sys_a, sys_b = sys_tables
    columns = [
        'id INTEGER PRIMARY KEY AUTOINCREMENT',
        'timestamp TEXT',
        'filename TEXT',
        'creationtime TEXT',
        'inode INTEGER',
        'accesstime TEXT',
        'checksum TEXT',
        'filesize INTEGER',
        'symlink TEXT',
        'owner TEXT',
        'domain TEXT',
        'mode TEXT',
        'casmod TEXT',
        'lastmodified TEXT',
        'hardlinks INTEGER',
        'count INTEGER'
    ]

    # columns.append('count INTEGER')
    create_sys_variant(c, sys_a, columns, ('filename',))
    create_sys_variant(c, sys_b, columns, ('timestamp', 'filename', 'creationtime'))


def create_table_cache(c, table, unique_columns):
    columns = [
        'id INTEGER PRIMARY KEY AUTOINCREMENT',
        'modified_time TEXT',
        'filename TEXT',
        'file_count INTEGER',
        'idx_count INTEGER',
        'max_depth INTEGER',
        'type TEXT',
        'target TEXT'
    ]

    col_str = ',\n      '.join(columns)
    unique_str = ', '.join(unique_columns)

    sql = f'''
    CREATE TABLE IF NOT EXISTS {table} (
      {col_str},
      UNIQUE({unique_str})
    )
    '''
    c.execute(sql)

    c.execute(f'CREATE INDEX IF NOT EXISTS idx_cache_idx_count ON {table} (idx_count)')
    c.execute(f'CREATE INDEX IF NOT EXISTS idx_cache_modified_time ON {table} (modified_time)')
    c.execute(f'CREATE INDEX IF NOT EXISTS idx_cache_idx_count_modified_time ON {table} (idx_count, modified_time)')  # composite


def clear_sys_profile(conn, cur, sys_tables, cache_table, systimeche):

    del_tables = sys_tables + (cache_table,) + (systimeche,)

    cur_tbl = ""
    try:
        for tbl in del_tables:
            cur_tbl = tbl
            cur.execute(f"DROP TABLE IF EXISTS {tbl}")
        conn.commit()
        # for tbl in (del_tables):
        #     cur.execute("""
        #         SELECT name FROM sqlite_master
        #         WHERE type='table' AND name=?
        #     """, (tbl,))
        #     if cur.fetchone():
        #         cur_tbl = tbl
        #         cur.execute(f"DELETE FROM {tbl}")
        #       try:
        #         cur.execute("DELETE FROM sqlite_sequence WHERE name=?", (tbl,))
        #       except sqlite3.OperationalError:
        #         pass
        #
        # conn.commit()
        return True
    except Exception as e:
        if conn:
            conn.rollback()

        print(f"Failed clearing {cur_tbl} table {type(e).__name__}: {e}")
        return False


def dbclear_sys_profile(dbopt, sys_tables, cache_table, systimeche):
    # Drop system time table
    f_f = "dbclear_sys_profile"
    del_tables = sys_tables + (cache_table,) + (systimeche,)

    cur_tbl = ""
    try:
        conn = sqlite3.connect(dbopt)
        cur = conn.cursor()

        for tbl in del_tables:
            cur_tbl = tbl
            cur.execute(f"DROP TABLE IF EXISTS {tbl}")
        conn.commit()

        return True
    except sqlite3.OperationalError as e:
        print(f"OperationalError {dbopt} connection problem {f_f}: {e}")
    except (sqlite3.Error, Exception) as e:
        if conn:
            conn.rollback()
        print(f"Failed clearing {cur_tbl} table {f_f} {type(e).__name__}: {e}")
    finally:
        clear_conn(conn, cur)
    return False


def dbtable_exists(dbopt, table_name):
    f_f = "dbtable_exists"
    conn = None
    cur = None
    try:
        conn = sqlite3.connect(dbopt)
        return table_exists(conn, table_name)
    except sqlite3.OperationalError as e:
        print(f"OperationalError {dbopt} connection problem {f_f}:", e)
        return False
    except (sqlite3.Error, Exception) as e:
        print(f"Problem with {dbopt} general error {f_f}:", e)
        return False
    finally:
        clear_conn(conn, cur)


def dbclear_table(dbopt, table_name):
    f_f = "dbclear_table"
    conn = None
    cur = None
    try:
        conn = sqlite3.connect(dbopt)
        cur = conn.cursor()
        if table_has_data(conn, table_name):
            if not clear_table(table_name, conn, cur, True):
                return False
        return True
    except sqlite3.OperationalError as e:
        print(f"OperationalError {dbopt} connection problem {f_f}:", e)
        return False
    except (sqlite3.Error, Exception) as e:
        print(f"Problem with {dbopt} general error {f_f}:", e)
        return False
    finally:
        clear_conn(conn, cur)


def table_exists(conn, table_name):
    c = conn.cursor()
    c.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name=?
    """, (table_name,))
    if not c.fetchone():
        c.close()
        return False
    return True


def clear_table(table, conn, cur, quiet=False):
    try:
        cur.execute(f"DELETE FROM {table}")
        try:
            cur.execute("DELETE FROM sqlite_sequence WHERE name=?", (table,))
        except sqlite3.OperationalError:
            pass
        conn.commit()
        if not quiet:
            print(f"{table} table cleared.")
        return True
    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        print(f"clear_table problem while clearing table {table} {type(e).__name__}: {e}")
    return False


def clear_extn_tbl(dbopt, quiet):
    conn = None
    cur = None
    try:
        conn = sqlite3.connect(dbopt)
        cur = conn.cursor()
        cur.execute("DELETE FROM extn WHERE ID != 1")
        conn.commit()

        if not quiet:
            print("extn table cleared.")
        return True
    except Exception as e:
        print("Reencryption failed extension table clear")
        if conn:
            conn.rollback()
        print(f"failure clear_extn_tbl func {type(e).__name__}: {e}")
        return False
    finally:
        if conn or cur:
            clear_conn(conn, cur)


def rmv_table(table, conn, cur, quiet=False):
    try:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.commit()
        if not quiet:
            print(f"{table} table cleared.")
        return True
    except sqlite3.Error as e:
        conn.rollback()
        print(f"problem while removing table {table}", e)
    return False
# end Sql


# query main functions


def getcount(curs):
    curs.execute('''
        SELECT COUNT(*)
        FROM logs
        WHERE (timestamp IS NULL OR timestamp = '')
        AND (filename IS NULL OR filename = '')
        AND (inode IS NULL OR inode = '')
        AND (accesstime IS NULL OR accesstime = '')
        AND (checksum IS NULL OR checksum = '')
        AND (filesize IS NULL OR filesize = '')
    ''')
    count = curs.fetchone()
    return count[0]


def dexec(cur, actname, limit):
    query = '''
    SELECT *
    FROM stats
    WHERE action = ?
    ORDER BY timestamp DESC
    LIMIT ?
    '''
    cur.execute(query, (actname, limit))
    return cur.fetchall()


def averagetm(conn, cur):
    cur.execute('''
    SELECT timestamp
    FROM logs
    ORDER BY timestamp ASC
    ''')
    timestamps = cur.fetchall()
    total_minutes = 0
    valid_timestamps = 0
    for timestamp in timestamps:
        if timestamp and timestamp[0]:
            current_time = datetime.strptime(timestamp[0], "%Y-%m-%d %H:%M:%S")
            total_minutes += current_time.hour * 60 + current_time.minute
            valid_timestamps += 1
    if valid_timestamps > 0:
        avg_minutes = total_minutes / valid_timestamps
        avg_hours = int(avg_minutes // 60)
        avg_minutes = int(avg_minutes % 60)
        avg_time = f"{avg_hours:02d}:{avg_minutes:02d}"
        return avg_time
    return "N/A"


def main(dbtarget, email, usr, flth, database=None):

    output = getnm(dbtarget, '.db')
    TEMPD = tempfile.gettempdir()
    try:
        with tempfile.TemporaryDirectory(dir=TEMPD) as tempdir:

            if database:
                dbopt = database
            else:
                dbopt = os.path.join(tempdir, output)
                if not decr(dbtarget, dbopt):
                    return 1

            if os.path.isfile(dbopt):
                conn = sqlite3.connect(dbopt)
                cur = conn.cursor()
                # cur.execute("DELETE FROM logs WHERE filename = ?", ('/home/guest/Downloads/Untitled' ,))
                # conn.commit()
                atime = averagetm(conn, cur)
                cprint.cyan("Search breakdown")
                cur.execute("""
                    SELECT
                    datetime(AVG(strftime('%s', accesstime)), 'unixepoch') AS average_accesstime
                    FROM logs
                    WHERE accesstime IS NOT NULL;
                """)
                result = cur.fetchone()
                average_accesstime = result[0] if result and result[0] is not None else None
                if average_accesstime:
                    print(f'Average access time: {average_accesstime}')
                print(f'Avg hour of activity: {atime}')
                cnt = getcount(cur)
                cur.execute('''
                SELECT filesize
                FROM logs
                ''')
                filesizes = cur.fetchall()
                total_filesize = 0
                valid_entries = 0

                for filesize in filesizes:
                    if filesize and is_integer(filesize[0]):  # Check if filesize is valid (not None or blank)
                        total_filesize += int(filesize[0])
                        valid_entries += 1
                if valid_entries > 0:
                    avg_filesize = total_filesize / valid_entries
                    avg_filesize_kb = int(avg_filesize / 1024)
                    print(f'Average filesize: {avg_filesize_kb} KB')
                    cprint.cyan("")
                print(f'Searches {cnt}')  # count
                cprint.cyan("")
                cur.execute('''
                SELECT filename
                FROM logs
                WHERE TRIM(filename) != ''
                ''')  # Ext
                filenames = cur.fetchall()
                extensions = []
                for entry in filenames:
                    filepath = entry[0]
                    if '.' in filepath:
                        ext = '.' + filepath.split('.')[-1] if '.' in filepath else ''
                    else:
                        ext = '[no extension]'
                    extensions.append(ext)
                if extensions:
                    counter = Counter(extensions)
                    top_3 = counter.most_common(3)
                    cprint.cyan("Top extensions")
                    for ext, count in top_3:
                        print(f"{ext}")
                cprint.cyan("")
                directories = [os.path.dirname(filename[0]) for filename in filenames]  # top directories
                directory_counts = Counter(directories)
                top_3_directories = directory_counts.most_common(3)
                cprint.cyan("Top 3 directories")
                for directory, count in top_3_directories:
                    print(f'{count}: {directory}')
                cprint.cyan("")
                cur.execute("SELECT filename FROM logs WHERE TRIM(filename) != ''")  # common file 5
                filenames = [row[0] for row in cur.fetchall()]  # end='' prevents extra newlines
                filename_counts = Counter(filenames)
                top_5_filenames = filename_counts.most_common(5)
                cprint.cyan("Top 5 created")
                for file, count in top_5_filenames:
                    print(f'{count} {file}')
                cprint.cyan("")
                top_5_modified = dexec(cur, 'Modified', 5)
                filenames = [row[3] for row in top_5_modified]
                filename_counts = Counter(filenames)
                top_5_filenames = filename_counts.most_common(5)
                cprint.cyan("Top 5 modified")
                for filename, count in top_5_filenames:
                    filename = filename.strip()
                    print(f'{count} {filename}')
                cprint.cyan("")
                top_7_deleted = dexec(cur, 'Deleted', 7)
                filenames = [row[3] for row in top_7_deleted]
                filename_counts = Counter(filenames)
                top_7_filenames = filename_counts.most_common(7)
                cprint.cyan("Top 7 deleted")
                for filename, count in top_7_filenames:
                    filename = filename.strip()
                    print(f'{count} {filename}')
                cprint.cyan("")
                top_7_writen = dexec(cur, 'Overwrite', 7)
                filenames = [row[3] for row in top_7_writen]
                filename_counts = Counter(filenames)
                if filename_counts:
                    top_7_filenames = filename_counts.most_common(7)
                    cprint.cyan("Top 7 overwritten")
                    for filename, count in top_7_filenames:
                        filename = filename.strip()
                        print(f'{count} {filename}')
                    cprint.cyan("")
                top_5_nsf = dexec(cur, 'Nosuchfile', 5)
                filenames = [row[3] for row in top_5_nsf]
                filename_counts = Counter(filenames)
                if filename_counts:
                    top_5_filenames = filename_counts.most_common(5)
                    cprint.cyan("Not actually a file")
                    for filename, count in top_5_filenames:
                        print(f'{count} {filename}')
                    cprint.cyan("")
                if os.path.isfile(flth):
                    cprint.green("Filter hits")
                    with open(flth, 'r') as file:
                        for line in file:
                            print(line, end='')
                cur.close()
                conn.close()
                return 0
            else:
                print(f"The database {dbopt} was not found")
    except Exception as e:
        print(f"Exception while running query {type(e).__name__}: {e}", flush=True)
    return 1

# if showdb("display database?"):


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: query.py <arg1> <arg2> <arg3> <arg4>")  # <arg2>
        sys.exit(0)
    dbtarget, email, usr, flth = sys.argv[1:5]
    sys.exit(main(dbtarget, email, usr, flth))
