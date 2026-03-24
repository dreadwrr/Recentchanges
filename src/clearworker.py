import sqlite3
import sys
import threading
import traceback
from PySide6.QtCore import Signal
from .gpgcrypto import encr
from .gpgcrypto import start_gpg_agent
from .pysql import clear_conn
from .pysql import clear_sys_profile
from .query import blank_count
from .query import main as query_main
from .qtclasses import Worker
from .qtfunctions import clear_cache
from .pyfunctions import cnc
from .rntchangesfunctions import removefile
from .rntchangesfunctions import reset_csvliteral
# 03/11/2026


# QObject
class ClearWorker(Worker):

    status = Signal(str)
    no_compression = Signal(bool)

    def __init__(self, lclhome, home_dir, database, target, usr, email, flth, compLVL):
        super().__init__(database)
        self.lclhome = lclhome
        self.home_dir = home_dir
        self.database = database
        self.target = target
        self.usr = usr
        self.email = email
        self.flth = flth
        self.compLVL = compLVL

        self.timer = None

        self.CACHE_S = None  # set_task
        self.sys_tables = None
        self.cache_table = None
        self.systimeche = None

    def set_task(self, CACHE_S, sys_tables, cache_table, systimeche):
        self.CACHE_S = CACHE_S
        self.sys_tables = sys_tables
        self.cache_table = cache_table
        self.systimeche = systimeche

    def set_cache(self, cachermPATTERNS):
        self.cachermPATTERNS = cachermPATTERNS

    def on_timeout(self):
        self.log.emit("Please enter passphrase in terminal")

    def run_op(self, action):
        try:

            self.progress.emit(67)
            # buf = io.StringIO()
            # old_stdout = sys.stdout
            # sys.stdout = buf

            rlt = 1

            try:
                if action == "query":
                    rlt = start_gpg_agent(self.email)
                    if rlt is False:
                        # if using curses or tty the user may not know to enter in terminal set timer to promp after 9s
                        self.timer = threading.Timer(9.0, self.on_timeout)
                        self.timer.start()

                    rlt = query_main(self.lclhome, self.usr, self.email, database=self.database, log_fn=self.log.emit)
                    if self.timer and self.timer.is_alive():
                        self.timer.cancel()

                    if rlt != 0:
                        self.log.emit('query failed with db action: query from run_query')
                    else:
                        self.status.emit("Query completed")
                        self.progress.emit(100)
                else:
                    conn = cur = None

                    try:
                        conn = sqlite3.connect(self.database)
                        cur = conn.cursor()

                        if action == "cache":

                            if clear_cache(conn, cur, self.cachermPATTERNS, log_fn=self.log.emit):
                                rlt = 0
                                try:
                                    reset_csvliteral(self.flth)
                                    self.status.emit("Cache cleared")
                                    self.log.emit("Filter hits cleared.")
                                    x = blank_count(cur)
                                    if x % 5 == 0:
                                        self.log.emit(f"for resetting filter hits see top of {self.flth}")
                                except Exception as e:
                                    cm = f'Failed to clear csv: {self.flth} {type(e).__name__} {e}'
                                    self.status.emit(cm)
                                    self.log.emit(cm)
                            else:
                                self.status.emit("Cache clear failed")

                        elif action == "sys":

                            if clear_sys_profile(conn, cur, self.sys_tables, self.cache_table, self.systimeche, log_fn=self.log.emit):
                                rlt = 0
                                self.status.emit("System index cleared")

                    except sqlite3.Error as e:
                        self.log.emit(f'problem with database in clear_worker {action}: {e}')
                    except Exception as e:
                        self.log.emit(f'Unexpected error in clear_worker {action}: {type(e).__name__} {e} traceback:\n{traceback.format_exc()}')
                    finally:
                        clear_conn(conn, cur)

                    if rlt == 0:
                        nc = cnc(self.database, self.compLVL)

                        if encr(self.database, self.target, self.email, nc, True):
                            self.progress.emit(100)
                            self.no_compression.emit(nc)
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
                self.log.emit(f'Unexpected error in clear worker: {type(e).__name__} {e}')
            # finally:
            #     sys.stdout = old_stdout

            # for line in buf.getvalue().splitlines():
            #     if line.strip():
            #         self.log.emit(line)

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
