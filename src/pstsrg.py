#! python3
# pstsrg.py - Process and store logs in a SQLite database, encrypting the database       11/07/2026
import os
import sqlcipher3
import sys
import traceback
from .configfunctions import find_gnupg_home
from .dirwalker import index_system
from .hanlyparallel import hanly_parallel
from .pyfunctions import convert_mime_to_int
from .pyfunctions import unescf_py
from .pysql import clear_conn
from .pysql import collision_check
from .pysql import get_lifetime_throughput
from .pysql import get_mime_map
from .pysql import get_unique_files
from .pysql import insert
from .pysql import insert_files_time
from .pysql import insert_if_not_exists
from .pysql import insert_mimes
from .pysql import table_has_data
from .query import blank_count


def hsearch(c, oldsort, argone, argf) -> str | None:
    '''
        if no old search file was found get from the database '''
    prev_search = None
    if not oldsort:

        c.execute("""
            SELECT timestamp, filename, casmod
            FROM prev_searches
            WHERE search_id = (
                SELECT id
                FROM searches
                WHERE thetime = ? AND argf = ?
                ORDER BY searchtime DESC
                LIMIT 1
            )
        """, (argone, argf))

        rows = c.fetchall()
        if rows:
            prev_search = "prev"  # mark as not first time search
        for row in rows:
            oldsort.append(f"{row[0]} {row[1]}\n")
    return prev_search


def move_searches(c, logf, oldsort, argone, argf, current_time, flsrh, validrlt, archivesrh, fmt) -> str | None:
    '''
        This saves previous searches into database now that sqlcipher is used. clear_searches used to handle this by moving
        the old search into a directory in appinstall. If there is no oldsort use hsearch to get from database. Then insert
        current search into database and also check if previous searches is over the limit.
        '''

    # if its not a newer than search
    if not flsrh:
        FLBRAND = current_time.strftime(fmt)
        validrlt = hsearch(c, oldsort, argone, argf)

        c.execute("""
            INSERT INTO searches (searchtime, thetime, argf)
            VALUES (?, ?, ?)
        """, (FLBRAND, argone, argf))
        search_id = c.lastrowid

        c.executemany("""
            INSERT INTO prev_searches (search_id, timestamp, filename)
            VALUES (?, ?, ?)
        """, ((search_id, ts_str, filepath) for ts_str, filepath in logf))

        # check the limit.
        if archivesrh % archivesrh * 2:

            c.execute("""
                DELETE FROM searches
                WHERE thetime = ? AND argf = ?
                AND id NOT IN (
                    SELECT id
                    FROM searches
                    WHERE thetime = ? AND argf = ?
                    ORDER BY searchtime DESC
                    LIMIT ?
                )
            """, (argone, argf, argone, argf, archivesrh))

    return validrlt


def main(dbopt, dbtarget, xdata, complete, rout, created, cachermPATTERNS, user_setting, logging_values, total_time, total_files, sys_tables, goahead, conn, k, iqt=False, strt=65, endp=90):

    # tempwork = logging_values[3]  # the script temp directory
    scr = logging_values[4]
    cerr = logging_values[5]
    # cache_f = logging_values[6]
    cache_s = logging_values[7]
    json_file = logging_values[8]
    gnupg_home = logging_values[9]

    user = user_setting['usr']
    basedir = user_setting['basedir']
    email = user_setting['email']
    model_type = user_setting['driveTYPE']
    analytics = user_setting['analytics']
    checksum = user_setting['checksum']
    checkMETHOD = user_setting['checkMETHOD']
    cdiag = user_setting['cdiag']
    ps = user_setting['ps']
    # compLVL = user_setting['compLVL']

    parsed = []

    csum = False
    new_profile = False
    new_database = False
    db_error = False
    goahead = True
    is_ps = False

    res = 0

    ha_total_time = logger_total_time = 0
    unique_files = 0
    lifetime_throughput = 0

    # original with a temp dir cant leave db to reencrypt if everything succeeds but only reencryption fails. so leave in app directory with proper perms
    # tempdir = tempfile.gettempdir()
    # tempdir = tempfile.mkdtemp()
    # os.makedirs(tempdir, exist_ok=True)
    # with tempfile.TemporaryDirectory(dir=tempdir) as tempwork:
    #     dbopt = name_of(dbtarget, 'db')   # generic output database
    # with tempfile.TemporaryDirectory(dir='/tmp') as tempdir:
    #     dbopt = os.path.join(tempdir, dbopt)

    # app_dir = os.path.dirname(dbtarget)
    # dbopt = os.path.join(app_dir, outfile)

    try:

        c = conn.cursor()

        drive_sys_table = sys_tables[0]
        if table_has_data(conn, drive_sys_table):
            is_ps = True
        else:
            # initial Sys profile
            if ps and checksum and not iqt:
                conn.close()
                new_profile = True

                if not gnupg_home:
                    gnupg_home = find_gnupg_home(json_file)

                print('Generating system profile.')

                appdata_local = logging_values[2]

                sts = index_system(appdata_local, dbopt, dbtarget, basedir, user, cache_s, email, gnupg_home, analytics, False, iqt, strt, endp)
                if sts != 0:
                    print("index_system from dirwalker failed to hash in pstsrg")

                conn = sqlcipher3.connect(dbopt)
                conn.set_key(k)
                c = conn.cursor()

            elif ps and not iqt:
                print('Sys profile requires the setting checksum to index')

        count = blank_count(c)
        if count < 1:
            goahead = False
            new_database = True

        # 07/20/2026
        mime_hashmap, id_to_mime = get_mime_map(c)
        # map mime str to an int for database
        parsed, new_mime_rows, _ = convert_mime_to_int(xdata, mime_hashmap, id_to_mime)

        # Log
        if parsed:

            if goahead:  # Hybrid analysis. Skip first pass ect.

                try:
                    if iqt:
                        print(f"Progress: {strt}", flush=True)

                    # get the time for multiprocessing and logger for benchmark. These are not stored and are for per run execution.
                    csum, ha_total_time, logger_total_time = hanly_parallel(model_type, rout, created, scr, cerr, parsed, id_to_mime, cachermPATTERNS,
                                                                            checksum, cdiag, dbopt, dbtarget, is_ps, user, logging_values, sys_tables,
                                                                            k, iqt, strt, endp)
                    k = b'\x00' * len(k)
                except Exception as e:
                    print(f"hanlydb failed to process : {type(e).__name__} : {e} \n{traceback.format_exc().strip()}", file=sys.stderr)

            try:

                insert(parsed, conn, c, "logs", "mtime_us")

                insert_mimes(c, new_mime_rows)

                count = blank_count(c)

                if count % 10 == 0:
                    print(f'{count} searches in gpg database')

                # Analytics - Store the total files and total time for the search. Also get unique files and lifetime throughput.
                if total_files:
                    if total_time > 0:
                        insert_files_time(c, total_files, total_time)  # insert and increment

                        lifetime_throughput = get_lifetime_throughput(c)  # get the total

                    unique_files = get_unique_files(c)

                    if not lifetime_throughput:
                        print("pstsrg couldnt get analytics. skipped")
                    # end Lifetime throughput

            except Exception as e:
                print(f'log db failed insert err: {e} {type(e).__name__}  \n{traceback.format_exc()}')
                db_error = True

        # Stats
        if rout:

            modified = {}

            if complete:  # store no such files
                rout.extend(" ".join(map(str, item)) for item in complete)

            try:
                for record in rout:
                    # parts = record.strip().split(None, 5)  # original
                    parts = record.strip().split(maxsplit=5)
                    if len(parts) < 6:
                        continue
                    # No need to record created not a useful statistic
                    if parts[0] == "Created":
                        continue
                    action = parts[0]
                    timestamp = f'{parts[1]} {parts[2]}'
                    changetime = f'{parts[3]} {parts[4]}'
                    fp_escaped = parts[5]
                    fp = unescf_py(fp_escaped)
                    insert_if_not_exists(action, timestamp, fp, changetime, conn, c)

                    modified[fp] = action

            except Exception as e:
                print(f'stats db failed to insert err: {e}  \n{traceback.format_exc()}')
                db_error = True

            if parsed:
                # Check for hash collisions
                # checkMETHOD != "blake2"
                if (
                    (checksum and cdiag)
                    and "blake" not in checkMETHOD
                ):

                    if collision_check(parsed, modified, cerr, sys_tables, c, ps):
                        csum = True

                if model_type.lower() != 'hdd':
                    x = os.cpu_count()
                    if x:
                        if not csum:
                            print(f'Detected {x} CPU cores.')

        # Encrypt if o.k.
        if not db_error:
            try:
                conn.commit()
                c.close()
                conn.close()
                conn = c = None

            except Exception as e:
                res = 4
                print(f'Failed to commit to database while closing connection in pstsrg.py: {e}')

        else:
            conn.rollback()
            res = 4  # delete any changes made.
            print('There is a problem with the database.')
    finally:
        k = b'\x00' * len(k)
        clear_conn(conn, c)

    data = (csum, unique_files, lifetime_throughput, ha_total_time, logger_total_time)

    if res == 0 and new_profile:
        return "new_profile", data
    elif res == 0 and new_database:
        return "new_database", data
    elif res == 0:
        return dbopt, data  # return 0 success
    # elif res == 2:
    #     return "decr_error", data
    elif res == 3:
        return "encr_error", data
    elif res == 4:
        return "db_error", data
    return None, None
