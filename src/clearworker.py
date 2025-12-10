import io
import sqlite3
import sys
import traceback
from PySide6.QtCore import Signal
from .pyfunctions import reset_csvliteral
from .query import clear_cache
from .query import clear_conn
from .query import clear_sys_profile
from .query import main as query_main
from .qtfunctions import Worker
from .rntchangesfunctions import encr
from .rntchangesfunctions import removefile
# 11/28/2025


# QObject
class ClearWorker(Worker):

    status = Signal(str)

    def __init__(self, database, target, nc, email, usr, flth, dcr):
        super().__init__(database)
        self.database = database
        self.target = target
        self.nc = nc
        self.email = email
        self.usr = usr
        self.flth = flth
        self.dcr = dcr

        self.CACHE_S = None  # set_task
        self.sys_tables = None
        self.cache_table = None
        self.systimeche = None

    def set_task(self, CACHE_S, sys_tables, cache_table, systimeche):
        self.CACHE_S = CACHE_S
        self.sys_tables = sys_tables
        self.cache_table = cache_table
        self.systimeche = systimeche

    def run_op(self, action):
        try:
            self.progress.emit(67)
            buf = io.StringIO()
            old_stdout = sys.stdout
            sys.stdout = buf

            rlt = 1

            try:
                if action == "query":
                    rlt = query_main(self.target, self.email, self.usr, self.flth, self.database)
                    if rlt != 0:
                        self.log.emit('query failed with db action: query from run_query')
                    else:
                        self.status.emit("Query completed")
                        self.progress.emit(100)
                else:
                    conn = None
                    cur = None
                    try:
                        conn = sqlite3.connect(self.database)
                        cur = conn.cursor()

                        if action == "cache":

                            if clear_cache(conn, cur, self.usr):
                                rlt = 0
                                try:
                                    reset_csvliteral(self.flth)
                                    self.status.emit("Cache cleared")
                                except Exception as e:
                                    cm = f'Failed to clear csv: {self.flth} {type(e).__name__} {e}'
                                    self.status.emit(cm)
                                    self.log.emit(cm)
                            else:
                                self.status.emit("Cache clear failed")

                        elif action == "sys":

                            if clear_sys_profile(conn, cur, self.sys_tables, self.cache_table, self.systimeche):
                                rlt = 0
                                self.status.emit("System index cleared")

                    except sqlite3.Error as e:
                        self.log.emit(f'problem with database in clear_worker {action}: {e}')
                    except Exception as e:
                        self.log.emit(f'Unexpected error in clear_worker {action}: {type(e).__name__} {e} traceback:\n{traceback.format_exc()}')
                    finally:
                        clear_conn(conn, cur)

                    if rlt == 0:
                        if encr(self.database, self.target, self.email, self.nc, False):
                            self.progress.emit(100)
                            if action == "sys":
                                removefile(self.CACHE_S)
                        else:
                            rlt = 1
                            msg = "could not clear cache files" if action == "cache" else "unable to clear sys profile"
                            errmsg = f"Reencryption failed. {msg}"
                            self.status.emit(errmsg)
                            self.log.emit(errmsg)

            except Exception as e:
                rlt = 1
                self.log.emit(f'Unexpected error in clear worker: {e}')
            finally:
                sys.stdout = old_stdout

            for line in buf.getvalue().splitlines():
                if line.strip():
                    self.log.emit(line)

            return rlt

        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.exception.emit(exc_type, exc_value, exc_traceback)
            return 1

    def run_cacheclr(self):
        rlt = self.run_op('cache')
        self.complete.emit(rlt if rlt is not None else 1)

    def run_sysclear(self):
        rlt = self.run_op('sys')
        self.complete.emit(rlt if rlt is not None else 1)

    def run_query(self):
        rlt = self.run_op('query')
        self.complete.emit(rlt if rlt is not None else 1)
