#!/usr/bin/env python3
# pstsrg.py - Process and store logs in a SQLite database, encrypting the database       03/03/2026
import os
import sqlite3
import sys
import traceback
from .dirwalker import index_system
from .gpgcrypto import encr
from .gpgcrypto import decr
from .gpgkeymanagement import find_gnupg_home
from .hanlyparallel import hanly_parallel
from .pyfunctions import cprint
from .pyfunctions import unescf_py
from .pysql import clear_conn
from .pysql import collision_check
from .pysql import create_db
from .pysql import insert
from .pysql import insert_if_not_exists
from .pysql import table_has_data
from .qtdrivefunctions import get_idx_tables
from .query import blank_count
from .pyfunctions import cnc
from .rntchangesfunctions import removefile


def main(dbopt, dbtarget, basedir, xdata, COMPLETE, rout, scr, cerr, CACHE_S, cachermPATTERNS, json_file, gnupg_home, user_setting, logging_values, dcr=False, iqt=False, strt=65, endp=90):

    user = user_setting['USR']
    email = user_setting['email']
    model_type = user_setting['driveTYPE']
    ANALYTICSECT = user_setting['ANALYTICSECT']
    checksum = user_setting['checksum']
    cdiag = user_setting['cdiag']
    ps = user_setting['ps']
    compLVL = user_setting['compLVL']

    # tempwork = logging_values[3]  # the script temp directory

    sys_tables, _, _ = get_idx_tables(basedir, CACHE_S)

    parsed = []

    csum = False
    new_profile = False
    db_error = False
    goahead = True
    is_ps = False
    conn = None

    res = 0

    # original with a temp dir cant leave db to reencrypt if everything succeeds but only reencryption fails. so leave in app directory with proper perms
    # TEMPDIR = tempfile.gettempdir()
    # TEMPDIR = tempfile.mkdtemp()
    # os.makedirs(TEMPDIR, exist_ok=True)
    # with tempfile.TemporaryDirectory(dir=TEMPDIR) as tempwork:
    #     dbopt = name_of(dbtarget, 'db')   # generic output database
    # with tempfile.TemporaryDirectory(dir='/tmp') as tempdir:
    #     dbopt = os.path.join(tempdir, dbopt)

    # app_dir = os.path.dirname(dbtarget)
    # dbopt = os.path.join(app_dir, outfile)

    if not iqt:
        if os.path.isfile(dbtarget):
            if not decr(dbtarget, dbopt):
                print(f'Find out why db not decrypting or delete: {dbtarget} and make a new one')
                return None, None
        else:
            try:
                conn = create_db(dbopt, sys_tables)
                cprint.green('Persistent database created')
                goahead = False
            except Exception as e:
                print("Failed to create db:", e)
                return None, None
    else:
        if not os.path.isfile(dbtarget):
            goahead = False

    try:
        if not os.path.isfile(dbopt):
            print("pstrg: cant find db unable to continue", dbopt)
            return None, None
        if not conn:
            conn = sqlite3.connect(dbopt)
    except Exception as e:
        print(f'failed with error: {e}')
        print()
        print("Unable to connect to database and do hybrid analysis")
        if not dcr:
            removefile(dbopt)
        return None, None

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
                res = index_system(appdata_local, dbopt, dbtarget, basedir, user, CACHE_S, email, ANALYTICSECT, False, gnupg_home, compLVL, iqt, strt, endp)
                if res != 0:
                    print("index_system from dirwalker failed to hash in pstsrg")
                conn = sqlite3.connect(dbopt)
                c = conn.cursor()
            elif ps and not iqt:
                print('Sys profile requires the setting checksum to index')

        # Log
        if xdata:

            if goahead:  # Hybrid analysis. Skip first pass ect.

                try:
                    if iqt:
                        print(f"Progress: {strt}", flush=True)

                    csum = hanly_parallel(model_type, rout, scr, cerr, xdata, cachermPATTERNS, ANALYTICSECT, checksum, cdiag, dbopt, is_ps, user, logging_values, sys_tables, iqt, strt, endp)

                except Exception as e:
                    print(f"hanlydb failed to process : {type(e).__name__} : {e} \n{traceback.format_exc().strip()}", file=sys.stderr)

            parsed = xdata

        if parsed:
            try:

                insert(parsed, conn, c, "logs", "mtime_us")

                count = blank_count(c)
                if count % 10 == 0:
                    print(f'{count + 1} searches in gpg database')

                if checksum and cdiag:
                    if collision_check(xdata, cerr, sys_tables, c, ps):
                        csum = True

            except Exception as e:
                print(f'log db failed insert err: {e} {type(e).__name__}  \n{traceback.format_exc()}')
                db_error = True

            if model_type.lower() != 'hdd':
                x = os.cpu_count()
                if x:
                    if not csum:
                        print(f'Detected {x} CPU cores.')

        # Stats
        if rout:

            if COMPLETE:  # store no such files
                rout.extend(" ".join(map(str, item)) for item in COMPLETE)

            try:
                for record in rout:
                    # parts = record.strip().split(None, 5)  # original
                    parts = record.strip().split(maxsplit=5)
                    if len(parts) < 6:
                        continue
                    action = parts[0]
                    timestamp = f'{parts[1]} {parts[2]}'
                    changetime = f'{parts[3]} {parts[4]}'
                    fp_escaped = parts[5]
                    fp = unescf_py(fp_escaped)
                    insert_if_not_exists(action, timestamp, fp, changetime, conn, c)

            except Exception as e:
                print(f'stats db failed to insert err: {e}  \n{traceback.format_exc()}')
                db_error = True

        sts = False

        # Encrypt if o.k.
        if not db_error:
            try:
                conn.commit()
                c.close()
                conn.close()
                conn = c = None
                nc = cnc(dbopt, compLVL)
                if new_profile:
                    dcr = False
                sts = encr(dbopt, dbtarget, email, no_compression=nc, dcr=dcr)
                if not sts:
                    res = 3  # & 2 gpg problem
                    print(f'Failed to encrypt database. Run   gpg --yes -e -r {email} -o {dbtarget} {dbopt}  before running again to preserve data.')

            except Exception as e:
                res = 3
                print(f'Encryption failed pstsrg.py: {e}')

        else:
            conn.rollback()
            res = 4  # delete any changes made.
            print('There is a problem with the database.')
    finally:
        clear_conn(conn, c)

    if not dcr and res != 3:
        removefile(dbopt)
    if res == 0 and new_profile:
        return "new_profile", csum
    elif res == 0:
        return dbopt, csum
        # return 0
    elif res == 3:
        return "encr_error", csum
    elif res == 4:
        return "db_error", csum
    return None, csum
