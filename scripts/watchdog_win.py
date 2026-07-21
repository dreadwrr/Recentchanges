import logging
import os
import psutil
import re
import sys
import time
import threading
import traceback
from datetime import datetime
from pathlib import Path
# flake8: noqa: E402
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import queue as queue_mod
from src.logs import emit_log
from src.logs import init_process_worker
from src.logs import setup_logger
from concurrent.futures import ThreadPoolExecutor
from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QApplication, QSystemTrayIcon, QMenu
from PySide6.QtGui import QIcon
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from src.dirwalkerwin import get_config_data
from src.pyfunctions import suppress_list
from src.rntchangesfunctions import get_runtime_exclude_list
from src.rntchangesfunctions import is_excluded
from src.rntchangesfunctions import name_of
from src.rntchangesfunctions import to_bool
from src.rntchangesfunctions import removefile
import src.watchdog_functions as wf
# 07/21/2026
# This watchdog script was made from an inotify script that was a result of needing to watch basedir for created files that
# could have preserved metadata and may not show in regular searches. As well as cache files over 1 MB for the 
# ctimecache.gpg.
#
# The results are written to file_creation_log which indicates proper operation and can also be used for convenience to
# recently recreated files
# 
# If a creation event is missing or lost it is logged. The intention is to only capture created files and by including
# too much can be inaccurate. for example a move event is not necessarily a created file and introduces noise.
#
# A debug mode can be set in inotifyfunctions to open debug terminal and wf.DEBUG points to print feedback
#
# debug file  <app_install>\\scripts\\file_creation_log.txt
# and cache_f <app_install>\\scripts\\ctimecache


# Globals
EXCLUSIONS = [
    Path(r"C:\Windows").resolve()
]

TEMP_SUFFIXES = {".tmp", ".temp", ".swp", ".part", ".partial", ".dup", ".crdownload"}


class CreatedHandler(FileSystemEventHandler):

    SLEEP_TIMES = [0.125, 0.25, 0.5, 1, 2, 4]
    SLEEP_LEN = len(SLEEP_TIMES)
    LIMIT = 3
    MAX_JOBS = 8
    IDLE = MAX_JOBS // 2

    def __init__(self, output_file, CACHE_F, cdir, lockfile, algo, moduleNAME, debug_file, inclusions, exclusions, supbrwLIST, logger, parent=None):
        super().__init__()

        localappdata = inclusions[0]
        usrDIR = inclusions[1]
        temp_dir = inclusions[2]
        escaped_user = re.escape(inclusions[3])
        flth = inclusions[4]
        dbtarget = inclusions[5]
        cache_f = inclusions[6]
        cache_s = inclusions[7]
        log_path = inclusions[8]
        gnupg_home = inclusions[9]

        file = 'file_creation_log.txt'
        if not output_file:
            output_file = Path(__file__).resolve().parent / file

        self.output_file = output_file
        self.CACHE_F = CACHE_F
        self.cdir = cdir
        self.lockfile = lockfile
        self.algo = algo
        self.moduleNAME = moduleNAME
        self.log_file = debug_file
        self.logger = logger

        self.pending_files = {}
        self.created_seen = {}

        self.active_jobs = 0
        self.active_lock = threading.Lock()

        self.executor = None
        self.log_queue = None

        self.webb = suppress_list(escaped_user, supbrwLIST)  # regexes

        # eg os.path.join(localappdata, f"{moduleNAME}_MDY_*") folders that get files moved some time after this script is started by qt main app
        # C:\\Users\\csaig\\AppData\\Local\\save-changesnew\\rntfiles_MDY_07-05-26-TIME_20_30_40\\rntfilesxSystemDiffFromLastSearch5.txt

        pattern = rf"{moduleNAME}_MDY_[^\\]*\\{moduleNAME}x.+$"
        self.webb.append(pattern)

        # eg the db is extracted to C:\\users\\{{user}}\\appdata\\local\\temp\\*\\dbopt.db
        base_temp = os.path.dirname(temp_dir)
        dbopt = name_of(dbtarget, '.db')
        pattern = rf"^{re.escape(base_temp)}\\[^\\]+\\{dbopt}$"
        self.webb.append(pattern)

        # add other regexes

        self.inclusion = get_runtime_exclude_list(
            localappdata, usrDIR, moduleNAME, flth, dbtarget, cache_f, cache_s,
            gnupg_home, str(debug_file), str(log_path) 
        )
 
        self.excluded = EXCLUSIONS.copy()
        self.excluded.append(output_file)
        self.excluded.extend(exclusions)

        self.suffixes = TEMP_SUFFIXES.copy()

        if wf.DEBUG: 
            init_process_worker(None)
        else:
            self.log_queue = queue_mod.Queue()
            init_process_worker(self.log_queue)
            self.log_thread = threading.Thread(
                target=wf.logger_process,
                args=(self.log_queue, self.lockfile, self.logger),
                daemon=True,
                name="LogWriter"
            )

            self.log_thread.start()

            self.executor = ThreadPoolExecutor(max_workers=self.MAX_JOBS, thread_name_prefix="CreatedHandler")

        self.start_time = time.time()

        self.pending_prune_timer = QTimer()
        self.pending_prune_timer.timeout.connect(self.pending_prune)

    def handle_file(self, event, entry):
        """ on created or moved to """
        action = event.event_type
        if action not in ("created", "moved"):
            if wf.DEBUG:
                print(f"{action} not moved or created")
            return

        current = self.active_jobs
        log_q = self.log_queue

        emit_log("DEBUG", f"File event: {action} file: {entry}", log_q, logger=self.logger)

        if event.is_directory:
            if action == "created":
                emit_log("DEBUG", f"Directory created: {entry}", log_q, logger=self.logger)

            return

        path = str(entry)

        if entry.is_file():
            
            emit_log("DEBUG", f"handle_file found checking for matches {entry}", log_q, logger=self.logger)

            match_found = False

            if is_excluded(self.webb, str(path)):
                match_found = True
            if not match_found:
                path_lower = path.lower()
                # if wf.DEBUG:
                #     print("match", path_lower)
                #     print("to")
                #     for rel in self.inclusion:
                #         print(rel)
                if any(path_lower.startswith(excl) for excl in self.inclusion):
                    match_found = True

            if wf.is_excl_dir(entry, self.excluded):
                if wf.DEBUG:
                    print(f"{action} Skipped excluded: {path}")
                return

            if action == "created":
                self.created_seen[path] = None
                    
            if wf.is_temp_file(entry, self.suffixes):
                if wf.DEBUG:
                    print(f"{action} Skipped by suffixes: {path}")
                return

            if not match_found:

                emit_log("DEBUG", f"handle_file matched input to pblk waiting for stable size {path}", log_q, logger=self.logger)

                if action == "moved":

                    if wf.shandle(action, event, entry, path, self.start_time, self.created_seen, log_q, self.logger):
                        return

                else:
                    if current > self.IDLE:
                        emit_log("DEBUG", f"system at has idle jobs: {current} skipping stabilization wait {path}", log_q, logger=self.logger)
                    else:
                        i = 0
                        retried = 0
                        last_size = -1
                        stable = False
                        while True:
                            wait = self.SLEEP_TIMES[min(i, self.SLEEP_LEN - 1)]
                            time.sleep(wait)
                            try:
                                size = entry.stat().st_size
                            except FileNotFoundError:
                                return
                            except PermissionError:
                                size = last_size 
                            if size == 0:
                                if retried >= self.LIMIT:
                                    stable = True
                                    break
                                i += 1
                                retried += 1
                            elif size == last_size:
                                if retried >= self.LIMIT:
                                    stable = True
                                    break
                                i = 0
                                retried += 1
                            else:
                                i += 1
                                retried = 0

                            last_size = size

                        if stable:
                            if size == 0:
                                emit_log("DEBUG", f"watchdog size stabilized looks like a download 0 bytes. file: {path}", log_q, logger=self.logger)
                            else:
                                emit_log("DEBUG", f"watchdog size stabilized for handle_file {path}", log_q, logger=self.logger)
                        else:
                            emit_log("DEBUG", f"timed out waiting for stable size, proceeding anyway (checksum will self-guard): {path}", log_q, logger=self.logger)

                res = wf.get_specs(action, entry, path, self.output_file, self.CACHE_F, self.lockfile, self.algo, self.created_seen, log_q, self.logger)
                if res:
                    emit_log("ERROR", f"Unknown status: {res} returned for file: {path}", log_q, logger=self.logger)

            else:
                emit_log("DEBUG", f"handle_file found excluded by matching skipped. for {path}", log_q, logger=self.logger)
        else:
            emit_log("DEBUG", f"handle_file file not found or parsing error. for {path}", log_q, logger=self.logger)

    def submit_counted(self, fn, *args):
        def wrapper():
            with self.active_lock:
                self.active_jobs += 1
            try:
                return fn(*args)
            # this can be commented out but it is better to shutdown to indicate there is an exception somewhere
            except Exception:
                # sys.excepthook(*sys.exc_info())
                file_out = os.path.join(self.cdir, "crash.txt")
                with open(file_out, "a") as f:
                    f.write(traceback.format_exc())
                os._exit(1)
            finally:
                with self.active_lock:
                    self.active_jobs -= 1
        return self.executor.submit(wrapper)

    def on_moved(self, event):
        if self.executor is None:
            self.handle_file(event, Path(event.dest_path).resolve())
        else:
            self.submit_counted(self.handle_file, event, Path(event.dest_path).resolve())

    def on_created(self, event):
        if self.executor is None:
            self.handle_file(event, Path(event.src_path).resolve())
        else:
            self.submit_counted(self.handle_file, event, Path(event.src_path).resolve())

    def pending_prune(self, ttl=30):

        pending_len = len(self.pending_files)
        created_len = len(self.created_seen)

        emit_log("DEBUG", f"pending_files={pending_len} created_seen={created_len}", self.log_queue, logger=self.logger)

        if pending_len > 500:
            now = time.time()
            for key in list(self.pending_files.keys()):
                if now - self.pending_files[key] > ttl:
                    del self.pending_files[key]
                else:
                    break
                   
        if created_len > 2000:
            overflow = created_len // 2
            oldest = list(self.created_seen.keys())[:overflow]
            for path in oldest:
                del self.created_seen[path]

class WatchdogService:
    def __init__(self, base, output_file, CACHE_F, cdir, lockfile, algo, moduleNAME, debug_file, exclDIRS, inclusions, supbrwLIST, logger):
        self.base = base
        self.output_file = output_file
        self.CACHE_F = CACHE_F
        self.logger = logger

        exclusions = []
        for excluded in exclDIRS:
            entry = Path(os.path.join(base, excluded)).resolve()
            if entry not in EXCLUSIONS:
                exclusions.append(str(entry))

        self.observer = None
        self.handler = CreatedHandler(
            output_file,
            CACHE_F,
            cdir,
            lockfile,
            algo,
            moduleNAME,
            debug_file,
            inclusions,
            exclusions,
            supbrwLIST,
            logger
        )

    def start(self):

        if self.CACHE_F:
            open(self.CACHE_F, "w").close()
        self.observer = Observer()
        self.observer.schedule(self.handler, path=self.base, recursive=True)

        self.handler.pending_prune_timer.start(15 * 60 * 1000)
        self.observer.start()

    def stop(self):
        if self.observer is not None:
            self.observer.stop()
            self.observer.join()
            self.observer = None

    def shutdown(self):
        self.stop()
        if self.handler.executor:
            self.handler.executor.shutdown(wait=True)
            self.handler.executor = None
        if self.handler.log_queue is not None:
            self.handler.log_queue.put(wf.SENTINEL)
            self.handler.log_thread.join(timeout=1)
            self.handler.log_queue = None

class TrayApp:
    def __init__(self, service, appdata_local, pid_file, _time, logger):
        self.service = service
        self.pid_file = pid_file
        self._time = _time
        self.logger = logger

        self.pid = os.getpid()

        # can be called in inotifyfunctions but would slow it down
        # old_pid_check(self.watchdog_pid_file, pid, logging, "windows")  

        # the pid file should not be there normally. If it is try to kill it to attempt to auto rectify
        wf.old_pid_check(pid_file, self.pid, logger, "windows")  

        self.write_pid()

        self.running = False

        self.tray = QSystemTrayIcon()
        icon_path = os.path.join(appdata_local, "Resources", "recentchanges.ico")  # os.path.join(Path(os.path.dirname(__file__)).parent, "Resources", "recentchanges.ico")
        self.tray.setIcon(QIcon(icon_path))

        self.menu = QMenu()

        self.tray.activated.connect(self.on_tray_activated)

        start_action = self.menu.addAction("Start")
        stop_action = self.menu.addAction("Stop")
        self.menu.addSeparator()
        exit_action = self.menu.addAction("Exit")

        start_action.triggered.connect(self.start_watch)
        stop_action.triggered.connect(self.stop_watch)
        exit_action.triggered.connect(self.exit_app)

        self.tray.setContextMenu(self.menu)
        self.tray.show()
    
        self.timer = QTimer()
        self.timer.setSingleShot(True)
        self.timer.timeout.connect(self.stop_watch)

        # optional delayed auto-start
        QTimer.singleShot(0, self.start_watch)
        FLBRAND = datetime.now().strftime("MDY_%m-%d-%y-TIME_%H_%M_%S")
        emit_log("DEBUG", f"{FLBRAND} inotify started", self.service.handler.log_queue, logger=self.logger)

    def write_pid(self):
        # with open(self.pid_file, "w") as f:  # original this could close a reused pid?
        #     f.write(str(self.pid) + '\n')

        proc = psutil.Process(self.pid)
        with open(self.pid_file, "w") as f:
            f.write(f"{self.pid}|{proc.create_time()}\n")

    def on_tray_activated(self, reason):
        # print("reason =", reason)  # probe
        # print(self.menu.actions())
        if reason == QSystemTrayIcon.Trigger:  # left click

            print("Left click")
            # self.menu.popup(QCursor.pos())

            # QTimer.singleShot(
            #     0,
            #     lambda: self.menu.exec(QCursor.pos())
            # )

        elif reason == QSystemTrayIcon.DoubleClick:
            print("Double click")

        # elif reason == QSystemTrayIcon.Context:  # right click
        #     print("Right click")

        # This can be used to give an alert. but if something goes wrong the indication is just quit and launcher disapears **
        #     self.tray.showMessage(
        #         "Watchdog",
        #         "Monitor is running",
        #         QSystemTrayIcon.Information,
        #         3000
        #     )

    def start_watch(self):
        if not self.running:
            self.service.start()
            self.running = True
            self.timer.stop()
            self.timer.start(int(self._time * 1000))

    def stop_watch(self):
        if self.running:
            self.running = False
            self.service.handler.pending_prune_timer.stop()
            self.service.stop()

    def exit_app(self):
        self.stop_watch()
        self.timer.stop()
        self.service.shutdown()
        # for t in threading.enumerate():
        #     print(t.name, t.daemon, t.is_alive())
        QApplication.quit()
        

def main(appdata_local, home_dir, output_file, CACHE_F, cdir, pid_file, lockfile, log_path, ll_level, _time, algo, user, moduleNAME, usrDIR, temp_dir, gnupg_home, debug_mode, *supbrwLIST):

    debug_mode = to_bool(debug_mode)
    wf.DEBUG = debug_mode
    if debug_mode:
        print("=== DEBUG ===")

    _time = int(_time)
    appdata_local = Path(appdata_local)
    # home_dir is used for linux persistent data directory
    base = "C:\\"
    flth = str(appdata_local / "flth.csv")  # filter hits
    dbtarget = str(appdata_local / "recent.gpg")  # database
    cache_f = str(appdata_local / "ctimecache.gpg")
    cache_s = str(appdata_local / "systimeche.gpg")
    cache_s, _ = os.path.splitext(cache_s)  # to match index drives as well as cache_s it is `systimeche`

    config_data = get_config_data(appdata_local, user)
    exclDIRS = config_data.exclDIRS

    inclusions = (appdata_local, usrDIR, temp_dir, user, flth, dbtarget, cache_f, cache_s, log_path, gnupg_home)

    debug_file = appdata_local / "logs" / "watchdog.log"
    logging.getLogger('watchdog').setLevel(logging.WARNING)  # turn off the intercepted watchdog logging
    logger = setup_logger(str(debug_file), ll_level, "WATCHDOG")

    try:
        app = QApplication(sys.argv)

        service = WatchdogService(
            base,
            output_file,
            CACHE_F,
            cdir,
            lockfile,
            algo,
            moduleNAME,
            debug_file,
            exclDIRS,
            inclusions,
            supbrwLIST,
            logger
        )

        tray = TrayApp(service, appdata_local, pid_file, _time, logger)

        # This can be commented out. But if an exception happens write it to a file and quit Watchdog this is to
        # indicate that there is problem with the code.
        def hook(exctype, value, tb):
            sys.__excepthook__(exctype, value, tb)
            with open(os.path.join(cdir, "crash.txt"), "a") as f:
                f.write("".join(traceback.format_exception(exctype, value, tb)))
            print(f"Unhandled exception {exctype.__name__} stack trace logged to: /tmp/crash.txt")
            # app = QApplication.instance()
            # if app is not None:
            #     app.quit()
        sys.excepthook = hook
        
        res = app.exec()
        removefile(pid_file)
        sys.exit(res)
    except Exception as e:
        em = "Failed to initialize Watchdog service:"
        print(f"{em} {type(e).__name__} err: {e} \n {traceback.format_exc()}")
        # QMessageBox.critical(None, "Error", f"{e}")
        logging.error(em, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":

    main(*sys.argv[1:])
