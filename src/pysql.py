import logging
import os
import sqlite3
import traceback


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
        'target TEXT',
        'lastmodified TEXT'
    ]
    if add_column:
        if isinstance(add_column, (tuple, list)):
            columns.extend(add_column)
        elif isinstance(add_column, str):
            e_cols = [col.strip() for col in add_column.split(',') if col.strip()]
            columns += e_cols
            # columns.append(add_column)
        else:
            raise TypeError("add_column must be str, tuple, or list")

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
    # c.execute(f'{sql} idx_logs_checksum_filename ON logs (checksum, filename)')
    c.execute(f'{sql} idx_logs_collision ON logs (checksum, filesize, filename)')


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

    if table_name.startswith('sys2'):
        c.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_filename_ts ON {table_name} (filename, timestamp DESC)")
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
        "checksum TEXT",  # NOT NULL DEFAULT ''
        'filesize INTEGER',
        'symlink TEXT',
        'owner TEXT',
        'domain TEXT',
        'mode TEXT',
        'casmod TEXT',
        'target TEXT',
        'lastmodified TEXT',
        'hardlinks INTEGER',
        'count INTEGER',
        'mtime_us INTEGER'
    ]

    # columns.append('count INTEGER')
    create_sys_variant(c, sys_a, columns, ('filename',))
    create_sys_variant(c, sys_b, columns, ('timestamp', 'filename', 'creationtime', 'checksum'))  # , 'checksum'


def create_table_cache(c, table, unique_columns):
    columns = [
        'id INTEGER PRIMARY KEY AUTOINCREMENT',
        'modified_time TEXT',
        'filename TEXT',
        'file_count INTEGER',
        'idx_count INTEGER DEFAULT 0',
        'idx_bytes INTEGER',
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


def create_db(database, sys_tables, action=None):

    print('Initializing database...')

    conn = sqlite3.connect(database)
    c = conn.cursor()

    create_logs_table(c, ('timestamp', 'filename', 'creationtime', 'checksum'), ['hardlinks INTEGER', 'mtime_us INTEGER'])

    create_sys_table(c, sys_tables)  # sys and sys2

    tables = [
        '''
        CREATE TABLE IF NOT EXISTS stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT,
            timestamp TEXT,
            filename TEXT,
            creationtime TEXT,
            UNIQUE(timestamp, filename, creationtime)
        )
        ''',
        '''
        CREATE TABLE IF NOT EXISTS extn (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            extension TEXT,
            timestamp TEXT,
            notes TEXT,
            UNIQUE(extension)
        )
        '''
    ]
    # sys and sys2 table
    for sql in tables:
        c.execute(sql)

    # used to store settings/note
    c.execute('''
    INSERT OR IGNORE INTO extn (id, extension, timestamp, notes)
    VALUES (1, '', '', '')
    ''')

    conn.commit()
    if action:
        return (conn)
    else:
        conn.close()


def query_database(dbopt, sql, params=None, iqt=False):

    conn = cur = None
    try:
        conn = sqlite3.connect(dbopt)
        cur = conn.cursor()

        if params is not None:
            cur.execute(sql, params)
        else:
            cur.execute(sql)

        return cur.fetchall()

    except (sqlite3.Error, Exception) as e:
        print(f"Problem retrieving data for dirwalker.py in query_database. database {dbopt} {type(e).__name__} error: {e}")
        return None
    finally:
        clear_conn(conn, cur)


def execute_query(dbopt, sql, params=None, iqt=False):

    conn = cur = None
    try:
        conn = sqlite3.connect(dbopt)
        cur = conn.cursor()

        if params is not None:
            cur.execute(sql, params)
        else:
            cur.execute(sql)

        conn.commit()

    except (sqlite3.Error, Exception) as e:
        print(f"Problem retrieving data for dirwalker.py in execute_query. database {dbopt} {type(e).__name__} error: {e}")
        return None
    finally:
        clear_conn(conn, cur)


def insert(log, conn, c, table, last_column, add_column=None):  # Log

    columns = [
        'timestamp', 'filename', 'creationtime', 'inode', 'accesstime',
        'checksum', 'filesize', 'symlink', 'owner', 'domain', 'mode',
        'casmod', 'target', 'lastmodified', 'hardlinks', last_column
    ]
    if add_column:
        columns.append(add_column)

    placeholders = ', '.join(['?'] * len(columns))
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


def insert_if_not_exists(action, timestamp, filename, creationtime, conn, c):  # Stats
    timestamp = timestamp or None
    c.execute('''
    INSERT OR IGNORE INTO stats (action, timestamp, filename, creationtime)
    VALUES (?, ?, ?, ?)
    ''', (action, timestamp, filename, creationtime))


def insert_cache(log, table, conn):

    columns = [
        'modified_time', 'filename', 'file_count', 'idx_count', 'idx_bytes', 'max_depth', 'type', 'target'
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
            columns = ['modified_time', 'filename', 'file_count', 'idx_bytes', 'max_depth', 'type', 'target']
            placeholders = ', '.join(['?'] * len(columns))
            col_str = ', '.join(columns)

            sql = f'''
            INSERT INTO {table} ({col_str})
            VALUES ({placeholders})
            ON CONFLICT(filename) DO UPDATE SET
                modified_time = excluded.modified_time,
                file_count = excluded.file_count,
                type = excluded.type,
                target = excluded.target
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
        target,
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
        a.target,
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


def clear_sys_profile(conn, cur, sys_tables, cache_table, systimeche, log_fn=print):

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

        log_fn(f"Failed clearing {cur_tbl} table {type(e).__name__}: {e}")
        return False


def dbclear_sys_profile(dbopt, sys_tables, cache_table, systimeche):
    # Drop system time table
    fn = "dbclear_sys_profile"
    del_tables = sys_tables + (cache_table,) + (systimeche,)

    cur_tbl = ""
    conn = cur = None
    try:
        conn = sqlite3.connect(dbopt)
        cur = conn.cursor()

        for tbl in del_tables:
            cur_tbl = tbl
            cur.execute(f"DROP TABLE IF EXISTS {tbl}")
        conn.commit()

        return True
    except sqlite3.OperationalError as e:
        print(f"OperationalError {dbopt} connection problem {fn}: {e}")
    except (sqlite3.Error, Exception) as e:
        if conn:
            conn.rollback()
        print(f"Failed clearing {cur_tbl} table {fn} {type(e).__name__}: {e}")
    finally:
        clear_conn(conn, cur)
    return False


def dbtable_exists(dbopt, table_name):
    fn = "dbtable_exists"
    conn = None
    cur = None
    try:
        conn = sqlite3.connect(dbopt)
        return table_exists(conn, table_name)
    except sqlite3.OperationalError as e:
        print(f"OperationalError {dbopt} connection problem {fn}:", e)
        return False
    except (sqlite3.Error, Exception) as e:
        print(f"Problem with {dbopt} general error {fn}:", e)
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


def dbclear_table(dbopt, table_name):
    fn = "dbclear_table"
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
        print(f"OperationalError {dbopt} connection problem {fn}:", e)
        return False
    except (sqlite3.Error, Exception) as e:
        print(f"Problem with {dbopt} general error {fn}:", e)
        return False
    finally:
        clear_conn(conn, cur)


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


def collision_check(xdata, cerr, sys_tables, c, ps):
    reported = set()
    csum = False
    if not xdata:
        return False

    current_rows = set()
    for record in xdata:
        if not record or len(record) < 7:
            continue
        filename = record[1]
        file_hash = record[5]
        file_size = record[6]
        if not (filename and file_hash and file_size):
            continue

        current_rows.add((filename, file_hash, file_size))

    if not current_rows:
        return False

    try:
        c.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS current_search_collisions (
                filename TEXT NOT NULL,
                checksum TEXT NOT NULL,
                filesize INTEGER NOT NULL
            )
            """
        )
        c.execute("DELETE FROM current_search_collisions")
        c.executemany(
            "INSERT INTO current_search_collisions (filename, checksum, filesize) VALUES (?, ?, ?)",
            list(current_rows),
        )

        if ps:
            tables = ['logs'] + list(sys_tables or [])

            db_rows_sql = " UNION ALL ".join([
                f"SELECT filename, checksum, filesize FROM {t} WHERE checksum IS NOT NULL" for t in tables
            ])

        else:
            db_rows_sql = "SELECT filename, checksum, filesize FROM logs WHERE checksum IS NOT NULL"

        c.execute(
            f"""
            WITH db_rows AS (
                {db_rows_sql}
            )
            SELECT DISTINCT
                cur.filename,
                db.filename,
                cur.checksum,
                cur.filesize,
                db.filesize
            FROM current_search_collisions cur
            JOIN db_rows db
                ON db.checksum = cur.checksum
            WHERE db.filename != cur.filename
              AND db.filesize != cur.filesize
            """
        )
        colcheck = c.fetchall()
    except sqlite3.DatabaseError as e:
        print(f"Database error in collision detection: {type(e).__name__} : {e}")
        return csum

    if colcheck:
        try:
            with open(cerr, "a", encoding="utf-8") as f:
                for filename, other_file, file_hash, size1, size2 in colcheck:
                    pair = tuple(sorted([filename, other_file]))
                    if pair not in reported:
                        csum = True
                        print(f"COLLISION: {filename} {size1} vs {other_file} {size2} | Hash: {file_hash}", file=f)
                        reported.add(pair)
        except IOError as e:
            print(f"Failed to write collisions: {e} {type(e).__name__}  \n{traceback.format_exc()}")
    return csum


def collision(cursor, is_sys, sys_tables=None):
    """ used for collision function in pyfunctions ln211 """
    if is_sys:
        tables = ['logs'] + list(sys_tables or [])

        union_sql = " UNION ALL ".join([
            f"SELECT filename, checksum, filesize FROM {t} WHERE checksum IS NOT NULL" for t in tables
        ])

        query = f"""
            WITH combined AS (
                {union_sql}
            )
            SELECT a.filename, b.filename, a.checksum, a.filesize, b.filesize
            FROM combined a
            JOIN combined b
            ON a.checksum = b.checksum
            AND a.filename < b.filename
            AND a.filesize != b.filesize
            ORDER BY a.checksum, a.filename
        """

    else:
        query = """
            SELECT a.filename, b.filename, a.checksum, a.filesize, b.filesize
            FROM logs a
            JOIN logs b
            ON a.checksum = b.checksum
            AND a.filename < b.filename
            AND a.filesize != b.filesize
            WHERE a.checksum IS NOT NULL
            ORDER BY a.checksum, a.filename
        """
    cursor.execute(query)
    return cursor.fetchall()


def detect_copy(filename, inode, checksum, sys_tables, cursor, ps):
    if ps:
        sys_a, sys_b = sys_tables
        query = f"""
            SELECT filename, inode FROM logs WHERE checksum = ?
            UNION ALL
            SELECT filename, inode FROM {sys_a} WHERE checksum = ?
            UNION ALL
            SELECT filename, inode FROM {sys_b} WHERE checksum = ?
        """
        cursor.execute(query, (checksum, checksum, checksum))
    else:
        query = '''
            SELECT filename, inode
            FROM logs
            WHERE checksum = ?
        '''
        cursor.execute(query, (checksum,))

    candidates = cursor.fetchall()

    for row in candidates:

        _, o_inode = row
        # if o_filename != filename or o_inode != inode:
        #    return True
        if int(o_inode) != inode:
            return True
    return False


def get_recent_changes(filename, cursor, table, e_cols=None):
    columns = [
        "timestamp", "filename", "creationtime", "inode",
        "accesstime", "checksum", "filesize", "symlink", "owner",
        "domain", "mode", "casmod", "target"
    ]
    if e_cols:
        if isinstance(e_cols, str):
            e_cols = [col.strip() for col in e_cols.split(',') if col.strip()]
        columns += e_cols

    col_str = ", ".join(columns)

    query = f'''
        SELECT {col_str}
        FROM {table}
        WHERE filename = ?
        ORDER BY timestamp DESC
        LIMIT 1
    '''
    cursor.execute(query, (filename,))
    return cursor.fetchone()


def get_recent_sys(filename, cursor, sys_tables, e_cols=None):
    sys_a, sys_b = sys_tables

    columns = [
        "timestamp", "filename", "creationtime", "inode",
        "accesstime", "checksum", "filesize", "symlink", "owner",
        "domain", "mode", "casmod", "target"
    ]
    if e_cols:
        if isinstance(e_cols, str):
            e_cols = [col.strip() for col in e_cols.split(',') if col.strip()]
        columns += e_cols

    col_str = ", ".join(columns)

    cursor.execute(f'''
        SELECT {col_str}
        FROM {sys_b}
        WHERE filename = ?
        ORDER BY timestamp DESC
        LIMIT 1
    ''', (filename,))
    row = cursor.fetchone()
    if row:
        return row
    cursor.execute(f'''
        SELECT {col_str}
        FROM {sys_a}
        WHERE filename = ?
        LIMIT 1
    ''', (filename,))
    return cursor.fetchone()


def increment_f(conn, c, sys_tables, records, logger=None):

    sys_b = sys_tables[1]

    sql_insert = f"""
        INSERT OR IGNORE INTO {sys_b} (
            timestamp, filename, creationtime, inode, accesstime, checksum,
            filesize, symlink, owner, domain, mode, casmod, target, lastmodified,
            hardlinks, count, mtime_us
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:

        with conn:
            c.executemany(sql_insert, records)
        return True
    except sqlite3.OperationalError as e:
        err = f"increment_f failed to insert changes in sys_b {e}"
        print(err)
        if logger:
            logger.error(err, exc_info=True)
    except Exception as e:
        err = f"Error increment_f table {sys_b}: {type(e).__name__} {e}"  # \n{traceback.format_exc()}
        print(err)
        if logger:
            logger.error(err, exc_info=True)
    return False


def find_symmetrics(dbopt, cache_table, systimeche):

    # dirs that had no files and now do

    cache_records = []
    has_systime = False
    conn = cur = None
    try:
        conn = sqlite3.connect(dbopt)
        cur = conn.cursor()
        if table_has_data(conn, systimeche):
            has_systime = True
            query = f"""
                SELECT s.modified_time,
                    s.filename,
                    s.file_count
                FROM {systimeche} AS s
                WHERE s.file_count > 0
                AND s.type IS NULL
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
                SELECT modified_time,
                    filename,
                    file_count
                FROM {cache_table}
                WHERE file_count = 0
                AND type IS NULL
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

        # new directories

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


def clear_conn(conn, cur):
    # if cur and conn is None:
    #     print("Warning: cursor exists with no connection")
    for obj, name in ((cur, "cursor"), (conn, "connection")):
        try:
            if obj:
                obj.close()
        except Exception as e:
            print(f"Warning: failed to close {name}: {e}")
