import os
import sqlite3
import shutil
import sys
import tempfile
import traceback
from collections import Counter
from datetime import datetime
from pathlib import Path
from .config import load_toml
from .configfunctions import find_install
from .configfunctions import get_config
from .gpgcrypto import decr
from .gpgkeymanagement import check_for_gpg
from .gpgkeymanagement import delete_gpg_keys
from .gpgkeymanagement import set_gpg
from .pyfunctions import is_integer
from .pysql import clear_conn
from .rntchangesfunctions import name_of
# from .rntchangesfunctions import cprint
# 03/11/2026


# see config.toml cache clear patterns for db


def blank_count(curs):
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


def main(appdata_local=None, user=None, email=None, reset=None, database=None, log_fn=print):

    if not database:

        if not appdata_local:
            appdata_local = find_install()

        if shutil.which("gpg") is None:
            set_gpg(appdata_local, "gpg")
        if not check_for_gpg():
            print("Unable to verify gpg in path. Likely path was partially initialized. quitting")
            return 1

        toml_file, json_file, USR = get_config(appdata_local, user, platform="Windows")
        config = load_toml(toml_file)
        if not config:
            return 1
        email = config['backend']['email']

    flth = appdata_local / "flth.csv"
    dbtarget = appdata_local / "recent.gpg"
    CACHE_F = appdata_local / "ctimecache.gpg"
    CACHE_S = appdata_local / "systimeche.gpg"

    output = name_of(dbtarget, '.db')
    flth = str(flth)
    dbtarget = str(dbtarget)

    result = False

    TEMPD = tempfile.gettempdir()

    if reset:

        return delete_gpg_keys(user, email, dbtarget, CACHE_F, CACHE_S, flth)

    try:

        with tempfile.TemporaryDirectory(dir=TEMPD) as tempdir:

            if database:
                dbopt = database
                result = True
            else:

                # there may not be a key for the gpg check that there are no problems
                # if not gpg_can_decrypt(dbtarget):
                #     return 1
                dbopt = os.path.join(tempdir, output)
                result = decr(dbtarget, dbopt)

                # can easily break if trying to automate fixing keys. let the user do it if wanted.
            if result:
                if os.path.isfile(dbopt):

                    try:
                        conn = sqlite3.connect(dbopt)
                        cur = conn.cursor()
                        # optionally run database commands
                        # cur.execute("DELETE FROM logs WHERE filename = ?", ('/home/guest/Downloads/Untitled' ,))
                        # conn.commit()
                        atime = averagetm(conn, cur)
                        ctext = "\033[36mSearch breakdown \033[0m"
                        log_fn(ctext)
                        cur.execute("""
                            SELECT
                            datetime(AVG(strftime('%s', accesstime)), 'unixepoch') AS average_accesstime
                            FROM logs
                            WHERE accesstime IS NOT NULL;
                        """)
                        rows = cur.fetchone()
                        average_accesstime = rows[0] if rows and rows[0] is not None else None
                        if average_accesstime:
                            log_fn(f'Average access time: {average_accesstime}')
                        log_fn(f'Avg hour of activity: {atime}')
                        c = blank_count(cur)
                        cur.execute('''
                        SELECT filesize
                        FROM logs
                        ''')
                        filesizes = cur.fetchall()
                        total_filesize = 0
                        valid_entries = 0
                        for filesize in filesizes:
                            if is_integer(filesize[0]):
                                sze = int(filesize[0])
                                if sze > 0:
                                    total_filesize += sze
                                    valid_entries += 1
                        if valid_entries > 0:
                            avg_filesize = total_filesize / valid_entries
                            avg_filesize_kb = int(avg_filesize / 1024)
                            log_fn(f'Average filesize: {avg_filesize_kb} KB')
                            log_fn("")
                        log_fn(f'Searches {c}')
                        log_fn("")
                        cur.execute('''
                        SELECT filename
                        FROM logs
                        WHERE TRIM(filename) != ''
                        ''')  # Ext
                        filenames = cur.fetchall()
                        filenames = [row[0] for row in filenames]
                        extensions = []
                        directories = []
                        for filename in filenames:
                            if not filename:
                                continue
                            directories.append(os.path.dirname(filename))  # get the top directories as well
                            filepath = Path(filename)
                            filename = filepath.name
                            if filename.startswith('.') or '.' not in filename:
                                ext = '[no extension]'
                            else:
                                ext = '.' + '.'.join(filename.split('.')[1:])
                            extensions.append(ext)
                        if extensions:
                            counter = Counter(extensions)
                            top_3 = counter.most_common(3)
                            ctext = "\033[36mTop extension\033[0m"
                            log_fn(ctext)
                            for ext, count in top_3:
                                log_fn(f"{ext}")
                        log_fn("")
                        directory_counts = Counter(directories)  # top directories ln170
                        top_3_directories = directory_counts.most_common(3)
                        ctext = "\033[36mTop 3 directories\033[0m"
                        log_fn(ctext)
                        for directory, count in top_3_directories:
                            log_fn(f'{count}: {directory}')
                        log_fn("")
                        # cur.execute("SELECT filename FROM logs WHERE TRIM(filename) != ''")  # common file 5 # original
                        # filenames = [row[0] for row in cur.fetchall()]  # end='' prevents extra newlines # original
                        filename_counts = Counter(filenames)
                        top_5_filenames = filename_counts.most_common(5)
                        ctext = "\033[36mTop 5 created\033[0m"
                        log_fn(ctext)
                        for file, count in top_5_filenames:
                            log_fn(f'{count} {file}')
                        top_5_modified = dexec(cur, 'Modified', 5)
                        filenames = [row[3] for row in top_5_modified]
                        filename_counts = Counter(filenames)
                        top_5_filenames = filename_counts.most_common(5)
                        ctext = "\033[36mTop 5 modified\033[0m"
                        log_fn(ctext)
                        for filename, count in top_5_filenames:
                            filename = filename.strip()
                            log_fn(f'{count} {filename}')
                        top_7_deleted = dexec(cur, 'Deleted', 7)
                        filenames = [row[3] for row in top_7_deleted]
                        filename_counts = Counter(filenames)
                        top_7_filenames = filename_counts.most_common(7)
                        ctext = "\033[36mTop 7 deleted\033[0m"
                        log_fn(ctext)
                        for filename, count in top_7_filenames:
                            filename = filename.strip()
                            log_fn(f'{count} {filename}')
                        top_7_writen = dexec(cur, 'Overwrite', 7)
                        filenames = [row[3] for row in top_7_writen]
                        filename_counts = Counter(filenames)
                        top_7_filenames = filename_counts.most_common(7)
                        ctext = "\033[36mTop 7 overwritten\033[0m"
                        log_fn(ctext)
                        for filename, count in top_7_filenames:
                            filename = filename.strip()
                            log_fn(f'{count} {filename}')
                        top_5_nsf = dexec(cur, 'Nosuchfile', 5)
                        filenames = [row[3] for row in top_5_nsf]
                        filename_counts = Counter(filenames)
                        if filename_counts:
                            top_5_filenames = filename_counts.most_common(5)
                            ctext = "\033[36mNot actually a file\033[0m"
                            log_fn(ctext)
                            for filename, count in top_5_filenames:
                                log_fn(f'{count} {filename}')
                        log_fn("")
                        ctext = "\033[1;32mFilter hit\033[0m"
                        log_fn(ctext)
                        if os.path.isfile(flth):
                            with open(flth, 'r') as file:
                                for line in file:
                                    if database:
                                        log_fn(line)
                                    else:
                                        print(line, end='')
                        return 0
                    finally:
                        clear_conn(conn, cur)

                else:
                    # no recent.db file permission error abort so sql doesnt make an empty database
                    log_fn(f"Unable to locate database: {dbopt}")

            # User has no key
            elif not database and result is None:

                ctime_path = CACHE_F.name
                log_fn(f"No key for {dbtarget} or {ctime_path}. if unable to fix delete to reset")

            else:

                if os.path.isfile(dbtarget):
                    log_fn(f'Find out why not decrypting. If unable to fix call: recentchanges reset  . unable to decrypt file: {dbtarget}')

                # else if no recent.gpg there was an exception
                return 1

    except Exception as e:
        log_fn(f"Exception while running query {type(e).__name__}: {e}  \n {traceback.format_exc()}")
    return 1


if __name__ == "__main__":

    sys.exit(main(*sys.argv[1:]))
