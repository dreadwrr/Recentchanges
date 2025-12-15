#!/usr/bin/env python3
# pstsrg.py - Process and store logs in a SQLite database, encrypting the database       12/02/2025
import os
import sqlite3
import sys
import traceback
from .hanlyparallel import hanly_parallel
from .dirwalker import index_system
from .rntchangesfunctions import encr
from .rntchangesfunctions import decr
from .rntchangesfunctions import getnm
from .rntchangesfunctions import get_idx_tables
from .rntchangesfunctions import intst
from .rntchangesfunctions import removefile
from .pyfunctions import collision
from .pyfunctions import cprint
from .query import create_logs_table
from .query import create_sys_table
from .query import getcount
from .query import table_has_data
from .query import insert
from .query import insert_if_not_exists


def create_db(database, sys_tables):

    print('Initializing database...')

    conn = sqlite3.connect(database)
    c = conn.cursor()
    create_logs_table(c, ('timestamp', 'filename', 'creationtime'))

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
    c.close()
    conn.close()


def main(dbopt, dbtarget, basedir, model_type, xdata, COMPLETE, logging_values, rout, checksum, updatehlinks, cdiag, email, ANALYTICSECT, ps, indexCACHEDIR, CACHE_S, compLVL, mainl, user='guest', dcr=False, iqt=False, strt=65, endp=90):

    outfile = getnm(dbtarget, '.db')
    scr = os.path.join(mainl, 'scr')
    cerr = os.path.join(mainl, "cerr")

    sys_tables, _ = get_idx_tables(basedir)  # default sys and sys2 - ie for r:\\    sys_r and sys2_r

    parsed = []

    goahead = True
    is_ps = False
    db_error = False
    new_profile = False

    res = 0

    # TEMPDIR = tempfile.gettempdir()
    # TEMPDIR = tempfile.mkdtemp()
    # os.makedirs(TEMPDIR, exist_ok=True)
    # with tempfile.TemporaryDirectory(dir=TEMPDIR) as mainl:

    # dbopt=os.path.join(logging_values[0], outfile)

    if not dbopt:
        dbopt = os.path.join(logging_values[0], outfile)
        if os.path.isfile(dbtarget):
            if not decr(dbtarget, dbopt):
                print(f'Find out why db not decrypting or delete: {dbtarget} and make a new one')
                return None
        else:
            try:
                create_db(dbopt, sys_tables)
                cprint.green('Persistent database created')
                goahead = False
            except Exception as e:
                print("Failed to create db:", e)
                return None
    else:
        if not os.path.isfile(dbtarget):
            goahead = False

    try:
        if not os.path.isfile(dbopt):
            print("pstrg: cant find db unable to continue", dbopt)
            return None
        conn = sqlite3.connect(dbopt)
    except Exception as e:
        print(f'failed with error: {e}')
        print()
        print("Unable to connect to database and do hybrid analysis")
        if dcr:
            removefile(dbopt)
        return None

    c = conn.cursor()

    drive_sys_table = sys_tables[0]
    if table_has_data(conn, drive_sys_table):
        is_ps = True
    else:
        # initial Sys profile
        if ps and checksum and not iqt:
            create_sys_table(c, sys_tables)

            print('Generating system profile.')
            new_profile = True

            res = index_system(dbopt, dbtarget, basedir, updatehlinks, CACHE_S, email, ANALYTICSECT, False, indexCACHEDIR, compLVL, iqt, strt, endp)
            if res != 0:
                print("index_system from dirwalker failed to hash in pstsrg")
        elif ps and not iqt:
            print('Sys profile requires the setting checksum to index')

    # Log
    if xdata:

        if goahead:  # Hybrid analysis. Skip first pass ect.

            try:
                if iqt:
                    print(f"Progress: {strt}", flush=True)
                hanly_parallel(model_type, rout, scr, cerr, xdata, checksum, cdiag, dbopt, is_ps, user, logging_values[1], sys_tables, iqt, strt, endp)

                x = os.cpu_count()
                if x:
                    if os.path.isfile(cerr):
                        with open(cerr, 'r') as f:
                            contents = f.read()
                        if not ('Suspect' in contents or 'COLLISION' in contents):
                            print(f'Detected {x} CPU cores.')
                    else:
                        print(f'Detected {x} CPU cores.')
                if ANALYTICSECT:
                    cprint.green('Hybrid analysis on')

            except Exception as e:
                print(f"hanlydb failed to process : {type(e).__name__} : {e} \n{traceback.format_exc().strip()}", file=sys.stderr)

        try:

            for record in xdata:
                parsed.append(record[:14])

            insert(parsed, conn, c, "logs", "hardlinks")

            # Check for hash collisions
            if checksum and cdiag:
                ccheck = collision(c, ps, sys_tables)
                if ccheck:
                    with open(scr, "a", encoding="utf-8") as f:
                        for row in ccheck:
                            a_filename, b_filename, checksum = row
                            print(f"COLLISION: {a_filename} , {b_filename} | Checksum: {checksum} |", file=f)

            count = getcount(c)
            if count % 10 == 0:
                print(f'{count + 1} searches in gpg database')

        except Exception as e:
            print(f'log db failed insert {type(e).__name__} {e}')
            db_error = True

    # Stats
    if rout:

        if COMPLETE:  # store no such files
            rout.extend(" ".join(map(str, item)) for item in COMPLETE)

        try:
            for record in rout:
                parts = record.strip().split(None, 5)
                if len(parts) < 6:
                    continue
                action = parts[0]
                timestamp = f'{parts[1]} {parts[2]}'
                creationtime = f'{parts[3]} {parts[4]}'
                fp = parts[5]
                insert_if_not_exists(action, timestamp, fp, creationtime, conn, c)

        except Exception as e:
            print(f'stats db failed to insert  {type(e).__name__} {e}')
            db_error = True
    c.close()
    conn.close()

    # os.chmod(outf, stat.S_IWRITE | stat.S_IREAD)

    sts = False
    if not db_error:  # Encrypt if o.k.
        try:
            nc_database = intst(dbopt, compLVL)
            sts = encr(dbopt, dbtarget, email, nc_database, dcr)
            if not sts:
                res = 3
                print(f'Failed to encrypt database. Paste   gpg --yes -r {email} -o {dbtarget} {dbopt}  to save before running again.')

        except Exception as e:
            print(f'Encryption failed pstsrg.py: {e}')

    else:
        res = 1
        print('There is a problem with the database.')

    if dcr and res != 3:
        removefile(dbopt)
    if res == 0 and new_profile:
        return "new_profile"
    elif res == 0:
        return dbopt
    elif res == 3:
        return "encr_error"
    return None
