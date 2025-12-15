import logging
import json
import os
import psutil
import random
import re
import requests
import subprocess
import sys
import time
import traceback
import wmi
import webbrowser
from packaging import version
from pathlib import Path
from PySide6.QtCore import Signal, QDateTime, QObject
from PySide6.QtGui import QTextCharFormat, QColor, QTextCursor
from PySide6.QtSql import QSqlDatabase, QSqlQuery
from PySide6.QtWidgets import QPlainTextEdit, QVBoxLayout, QDialog, QPushButton, QLabel, QComboBox, QInputDialog, QMessageBox
from src.dirwalkerfnts import os_walk_error
from src.rntchangesfunctions import decr
from src.rntchangesfunctions import encr
from src.rntchangesfunctions import update_toml_setting
from src.rntchangesfunctions import parse_drive
from src.rntchangesfunctions import removefile
from src.wmipy import get_disk_and_volume_for_drive
# 11/21/2025
# gestures
#
# QMessageBox.critical(None, "Error", "query failed")
# #
#


def wdisplay_prompt(parent, title, message, affirm, rejct):  # y/n
    msg_box = QMessageBox(parent)
    msg_box.setWindowTitle(title)
    msg_box.setText(message)

    import_button = msg_box.addButton(affirm, QMessageBox.ButtonRole.AcceptRole)
    default_button = msg_box.addButton(rejct, QMessageBox.ButtonRole.RejectRole)
    msg_box.exec()
    return msg_box.clickedButton() == import_button


def wdisplay_message(parent, message, title="Status", default=True):  # ok
    msg = QMessageBox(parent)
    if default:
        msg.setIcon(QMessageBox.Icon.Warning)
    else:
        msg.setIcon(QMessageBox.Icon.Information)
    msg.setWindowTitle(title)
    msg.setText(message)
    msg.setStandardButtons(QMessageBox.StandardButton.Ok)
    msg.exec()


def wdisplay_get_input(parent, title, value_title):
    return QInputDialog.getText(parent, title, value_title)


ANSI_COLOR_MAP = {
    "36": QColor("cyan"),
    "31": QColor("red"),
    "1;32": QColor("limegreen"),
    "34": QColor("blue"),
    "33": QColor("orange"),
    "35": QColor("magenta"),
    "37": QColor("white")
}

RESET_CODE = "0"
ANSI_REGEX = re.compile(r'\033\[([0-9;]+)m')


class FastColorText(QPlainTextEdit):
    def __init__(self):
        super().__init__()
        self.setReadOnly(True)
        self.cursor = self.textCursor()
        self.current_color = None

    def append_colored_output(self, line: str):
        parts = ANSI_REGEX.split(line)
        # parts = [text, code, text, code, text, ...]
        i = 0

        while i < len(parts):
            text = parts[i]
            if text:
                self.insert_colored_text(text, self.current_color)  # original
                # for segment in text.splitlines(keepends=True):  # new
                #     self.insert_colored_text(segment, self.current_color)
            i += 1
            if i < len(parts):
                code = parts[i]
                if code == RESET_CODE:
                    self.current_color = None
                elif code in ANSI_COLOR_MAP:
                    self.current_color = ANSI_COLOR_MAP[code]
                i += 1

        self.insert_colored_text("\n", self.current_color)  # original
        self.verticalScrollBar().setValue(self.verticalScrollBar().maximum())

    def insert_colored_text(self, text, color):
        fmt = QTextCharFormat()
        if color:
            fmt.setForeground(color)

        self.cursor.movePosition(QTextCursor.MoveOperation.End)
        self.cursor.insertText(text, fmt)


class QTextEditLogger(QObject):  # gui/console
    new_message = Signal(str)

    def __init__(self, output_handler):
        super().__init__()
        self.output_handler = output_handler
        self.console = sys.__stdout__  # save original stdout

    def write(self, message):
        if message.strip():
            self.new_message.emit(message)
            # self.output_handler(message)   # show in GUI
            self.console.write(message + '\n')    # also show in console
            self.console.flush()

    def flush(self):
        self.console.flush()


class Worker(QObject):
    progress = Signal(float)
    log = Signal(str)
    complete = Signal(int)

    exception = Signal(object, object, object)

    def __init__(self, database):
        super().__init__()
        self.database = database
        self._should_stop = False

    def stop(self):
        self._should_stop = True


class DBConnectionError(Exception):
    pass


class DBMexec:
    def __init__(self, db_path, conn_name="sq_9", ui_logger=None):
        self.db_path = db_path
        self.conn_name = conn_name
        self.ui_logger = ui_logger
        self.db = None
        self.dbname = os.path.basename(db_path)

        self._conn_context = False

    def log(self, message):
        if self.ui_logger:
            self.ui_logger.appendPlainText(message)
        else:
            print(message)

    def __enter__(self):
        if not self.connect():
            raise DBConnectionError(f"Failed to connect to database: {self.db_path}")
        self.db.transaction()
        self._conn_context = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn_context:
            if exc_type is None:
                self.db.commit()
            else:
                self.db.rollback()
            self.close()
        self._conn_context = False

    def connect(self):
        if QSqlDatabase.contains(self.conn_name):
            self.db = QSqlDatabase.database(self.conn_name)
        else:
            self.db = QSqlDatabase.addDatabase("QSQLITE", self.conn_name)
            self.db.setDatabaseName(self.db_path)

        if not self.db.isOpen() and not self.db.open():
            err = self.db.lastError().text()
            self.log(f"couldnt connect to {self.dbname}: {err}")
            return False
        return True

    def close(self):
        if self.db and self.db.isOpen():
            self.db.close()
        self.remove_conn()

    def remove_conn(self):
        if self.conn_name in QSqlDatabase.connectionNames():
            # del self.db
            self.db = None
            QSqlDatabase.removeDatabase(self.conn_name)

    def table_exists(self, table_name):
        return table_name in self.db.tables() if self.db and self.db.isOpen() else False

    def table_has_data(self, table_name):
        if not self.table_exists(table_name):
            return False

        query = QSqlQuery(self.db)
        sql = f"SELECT 1 FROM {table_name} LIMIT 1"

        if not query.exec(sql):
            self.log(f"SQL Error in table_has_data: {query.lastError().text()}\n {sql}")
            return False
        return query.next()

    def execute(self, sql, params=None):
        if not self.db or not self.db.isOpen():
            raise DBConnectionError("No open connection for execute()")

        query = QSqlQuery(self.db)
        if params:
            query.prepare(sql)
            for key, value in params.items():
                query.bindValue(f":{key}", value)
            ok = query.exec()
        else:
            ok = query.exec(sql)

        if not ok:
            self.log(f"SQL Error: {query.lastError().text()}\n→ {sql}")
            return None
        return query

    def drop_table(self, table_name):
        if self.table_exists(table_name):
            return self.execute(f"DROP TABLE IF EXISTS {table_name}")
        return False

    def clear_table(self, table_name):
        if self.table_exists(table_name):

            if not self.execute(f"DELETE FROM {table_name}"):
                self.log(f"Failed to clear data from {table_name}")
                return False
            try:
                self.execute("DELETE FROM sqlite_sequence WHERE name = :name", {"name": table_name})
            except Exception as e:
                self.log(f"Warning: could not reset sequence for {table_name}: {e}")
        return True


# QSql
def get_conn(db_path, conn_name):
    if QSqlDatabase.contains(conn_name):
        db = QSqlDatabase.database(conn_name)
    else:
        db = QSqlDatabase.addDatabase("QSQLITE", conn_name)
        db.setDatabaseName(db_path)

    if not db.isOpen():
        if not db.open():
            return None, f"Failed to open database: exit {db.lastError().text()}"
    return db, None


def qttable_has_data(dbopt, conn_func, ui_logger, table):
    db = None
    query = None
    conn_nm = "sq_1"
    db_name = os.path.basename(dbopt)
    try:
        db, err = conn_func(dbopt, conn_nm)
        if err:
            ui_logger.appendPlainText(f"unable to connect to database {db_name}")
            return False
        else:
            query = QSqlQuery(db)
            tables = db.tables()
            if table in tables:
                sql = f"SELECT 1 FROM {table} LIMIT 1"
                if query.exec(sql) and query.next():
                    return True

    except Exception as e:
        ui_logger.appendPlainText(f"Sql exception: type: {type(e).__name__} err:{e}")
        if query:
            ui_logger.appendPlainText(f"query error: {query.lastError().text()}\n")
    finally:
        if query:
            del query
        if db:
            db.close()
    return False


# if sys table has data prompt before continuing
# sys - sys_n for n drive
# cache_s for C:\\ or cache_n for n drive if its S:\\ its cache_s2    these are directories at time of profile
# systimeche - systimeche_n for n drive      these are dirs as cache is updated from search downloads/files
def has_sys_data(dbopt, logger, sys_table, prompt, parent=None):
    db = None
    query = None
    conn_nm = "sq_1"
    db_name = os.path.basename(dbopt)

    try:
        db, err = get_conn(dbopt, conn_nm)
        if err:
            logger.appendPlainText(f"could not connect to {db_name} database {err}")
        else:
            query = QSqlQuery(db)
            if sys_table in db.tables():
                query.prepare(f"SELECT 1 FROM {sys_table} LIMIT 1")
                if query.exec() and query.next():
                    uinpt = wdisplay_prompt(parent, "Confirm Action", prompt, "Yes", "No")
                    if not uinpt:
                        return False

            return True
    except Exception as e:
        mg = f"query error {type(e).__name__}: {e}"
        print(mg)
        if query:
            mg = mg + f"{query.lastError().text()}\n"
        logger.appendPlainText(mg)
        logging.error(mg, exc_info=True)
    finally:
        if query:
            del query
        if db:
            db.close()
    return False


def save_note(logger, notes, query):
    try:

        query.prepare("""
            INSERT INTO extn (id, notes)
            VALUES (1, :notes)
            ON CONFLICT(id) DO UPDATE SET notes = excluded.notes
        """)
        query.bindValue(":notes", notes)
        if query.exec():
            return True
        else:
            err = query.lastError()
            if err.isValid():
                logger.appendPlainText(f"savenote query err: {err.text()}\n")
            logger.appendPlainText("Failed to save notes to db")
    except Exception as e:
        logger.appendPlainText(f"Unable update notes savenote qtfunctions {type(e).__name__} err: {e} \n{traceback.format_exc()}")
    return False
# end QSql


class DriveSelectorDialog(QDialog):
    def __init__(self, filter_out=None, parent=None):
        super().__init__(parent)

        self.filter_drv = filter_out
        self.setWindowTitle("Select Drive")

        layout = QVBoxLayout(self)

        self.drive_combo = QComboBox()
        if not filter_out:
            self.drives = [p.device for p in psutil.disk_partitions()]
        else:
            self.drives = [
                p.device for p in psutil.disk_partitions()
                if p.device.lower() != filter_out.lower()  # remove system basedir
            ]
        self.drive_combo.addItems(self.drives)
        layout.addWidget(self.drive_combo)

        select_btn = QPushButton("Select")
        select_btn.clicked.connect(self.accept)
        layout.addWidget(select_btn)

    def selected_drive(self):
        return self.drive_combo.currentText()


def load_gpg(dbopt, dbtarget, logger):
    if os.path.isfile(dbtarget):
        if decr(dbtarget, dbopt):
            return True
        else:
            print("Database failed to decrypt")
    else:
        logger.setText("No database to load")
    return False


def open_html_resource(parent, lclhome):
    fp_ = os.path.join(lclhome, "Resources", "Welcomefile.html")
    fpth = os.path.abspath(fp_)

    html_file = Path(fpth).resolve()
    webbrowser.open(html_file.as_uri())
    # win = QMainWindow(parent)  # web engine is 200mb
    # central = QWidget()
    # layout = QVBoxLayout(central)
    # win.setCentralWidget(central)
    # browser = QWebEngineView()
    # browser.setUrl(QUrl.fromLocalFile(fpth))
    # layout.addWidget(browser)
    # win.resize(800, 600)
    # win.show()
    # win.raise_()
    # win.activateWindow()
    # return win


def show_cmddoc(cmddoc, lclhome, default_gpg, gpg_path, gnupg_home, email, example_gpg, hudt):

    hudt.clear()
    fingerprint = None

    # custom quick commands
    if os.path.isfile(cmddoc):
        with open(cmddoc, 'r') as f:
            content = f.read()
            hudt.appendPlainText(content)
            # for line in f:
            #     print(line.strip())

    # gpg info
    hudt.appendPlainText("\n")

    if gnupg_home:
        gpg_command = str(default_gpg)  # gpg_command = ".\\gpg\\gpg.exe"
        command = [gpg_command, '--homedir', gnupg_home]
    else:
        gpg_command = str(gpg_path)
        command = [gpg_command]

    command += ['--list-secret-keys']
    try:

        result = subprocess.run(command, capture_output=True, text=True)
        pattern = r'\s+([A-F0-9]{40})\n.*?uid\s+\[.*?\]\s+([^\n<]+<([^>]+)>)'

        output = result.stdout
        matches = re.findall(pattern, output)
        for match in matches:

            user_email = match[2]
            if user_email == email:
                fingerprint = match[0]
                break

    except Exception:
        hudt.appendPlainText("An error occurred while trying to list GPG keys.")
        pass

    gpg_install_l = str(gpg_path).lower()
    app_frm_l = str(default_gpg).lower()

    if gpg_install_l == app_frm_l:
        if fingerprint:
            hudt.appendPlainText(f"Delete a GPG key for: {email}\n")
            lclgpg = f"{lclhome}\\gpg\\gpg"
            hudt.appendPlainText(f'& "{lclgpg}" --homedir {gnupg_home} --delete-secret-key {fingerprint}')
            hudt.appendPlainText(f'& "{lclgpg}" --homedir {gnupg_home} --delete-key {fingerprint}')
            hudt.appendPlainText("\n")

        gpg_command = f'& "{default_gpg}" --homedir "{gnupg_home}"'

    else:
        if fingerprint:
            hudt.appendPlainText(f"Delete a GPG key for: {email}\n")
            hudt.appendPlainText(f"gpg --delete-secret-key {fingerprint}")
            hudt.appendPlainText(f"gpg --delete-key {fingerprint}")

        gpg_command = "gpg"

    hudt.appendPlainText("\n")
    hudt.appendPlainText("decrypt something (example check a cache file) from app directory")
    hudt.appendPlainText(
        f"{gpg_command} -o myfile.txt --decrypt {example_gpg}.gpg"
    )
    # end gpg info


def get_help(lclhome, hudt):
    hudt.clear()
    fp_ = os.path.join(lclhome, "Resources", "versionquery")
    with open(fp_, 'r') as f:
        for line in f:

            if line.startswith("#"):
                continue
            line = line.replace("\\t", "\t")

            hudt.appendPlainText(line.rstrip("\n"))


def get_latest_github_release(user, repo):
    url = f"https://api.github.com/repos/{user}/{repo}/releases/latest"
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        latest_version = data["tag_name"].lstrip("v").rstrip("-py1")  # # .removesuffix("-py1")
        download_url = data["html_url"]
        return latest_version, download_url
    except Exception as e:
        print("Failed to fetch latest release:", e)
        return None, None


def check_for_updates(app_version, user, repo, parent=None):

    latest_version, _ = get_latest_github_release(user, repo)
    if latest_version and version.parse(latest_version) > version.parse(app_version):
        wdisplay_message(parent, f"New version available: {latest_version}", "Update msg", default=False)
    else:
        wdisplay_message(parent, f"You are running the latest version. {app_version}", "Update msg", default=False)


def show_licensing(lclhome, hudt):
    hudt.clear()
    l_folder = os.path.join(lclhome, "Licenses")
    if not os.path.isdir(l_folder):
        return
    for filename in os.listdir(l_folder):
        fp = os.path.join(l_folder, filename)

        if not os.path.isfile(fp):
            continue
        with open(fp, "r", encoding="utf-8", errors="ignore") as f:
            contents = f.read()
        hudt.appendPlainText(contents)
        hudt.appendPlainText("\n")


def help_about(lclhome, hudt):
    dlg = QDialog()
    dlg.setWindowTitle("About Recent Changes")

    layout = QVBoxLayout()
    # layout.setSpacing(15)
    # layout.setContentsMargins(20, 20, 20, 20)

    label = QLabel("v3.0\n\nCreated by Colby Saigeon\nh&k enterprisez\n\nFind recent files using powershell.")
    # label.setWordWrap(True)
    layout.addWidget(label)

    run_btn = QPushButton("Licensing")
    run_btn.clicked.connect(lambda: show_licensing(lclhome, hudt))
    run_btn.setFixedWidth(run_btn.sizeHint().width() + 20)
    layout.addWidget(run_btn)  # alignment=Qt.AlignHCenter

    close_btn = QPushButton("Close")
    close_btn.clicked.connect(dlg.close)
    close_btn.setFixedWidth(run_btn.sizeHint().width() + 20)
    layout.addWidget(close_btn)

    dlg.setLayout(layout)
    dlg.exec()


def load_cmdpmpt(lclhome, popPATH=None):
    # launch command prompt as admin
    # shell32 = ctypes.windll.shell32
    # shell32.ShellExecuteW(
    #     None,
    #     "runas",
    #     "cmd.exe",
    #     None,
    #     lclhome,
    #     1
    # )

    # app is already admin so will work
    work_area = lclhome if not popPATH else popPATH

    subprocess.Popen(
        ["cmd"],
        cwd=work_area,
        creationflags=subprocess.CREATE_NEW_CONSOLE
    )


# launch powershell as admin. it is inherited from app
def load_pshell(lclhome, popPATH=None):

    work_area = lclhome if not popPATH else popPATH

    # subprocess.Popen(["powershell", "-NoExit"])
    # subprocess.Popen(['cmd', '/c', 'start', 'powershell', '-NoExit'])
    env = os.environ.copy()
    # env["PATH"] = r";" + env["PATH"]
    # env=env,

    subprocess.Popen(
        ["pwsh", "-NoExit"],       # or "powershell" for v5
        cwd=work_area,  # start directory
        creationflags=subprocess.CREATE_NEW_CONSOLE
    )


def load_explorer(lclhome, popPATH=None):
    # open windows explorer
    work_area = lclhome if not popPATH else popPATH
    os.startfile(work_area)


def set_j_settings(updates: dict = None, drive=None, filepath="usrprofile.json"):

    path = Path(filepath)
    if path.is_file():
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError:
            data = {}
    else:
        data = {}

    if drive:  # Drive-info
        if updates is None:
            data.pop(drive, None)  # Remove entire drive listing
        else:
            if drive not in data or not isinstance(data[drive], dict):
                data[drive] = {}
            target = data[drive]
            for k, v in updates.items():
                if v is None:
                    target.pop(k, None)
                else:
                    target[k] = v
    else:
        # Top-level
        if updates is not None:
            target = data
            for k, v in updates.items():
                if v is None:
                    target.pop(k, None)
                else:
                    target[k] = v

    path.write_text(json.dumps(data, indent=4))


def get_j_settings(keys=None, drive=None, filepath="usrprofile.json"):

    path = Path(filepath)
    if not path.is_file():
        return {} if keys is None else {k: None for k in keys}

    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        return {} if keys is None else {k: None for k in keys}

    target = data.get(drive, {}) if drive else data

    if keys is None:
        return target

    return {k: target.get(k) for k in keys}


# find file combo box extensions
def fill_extensions(combffile, default_extensions, new_extension=None):
    index = None

    prev_items = [combffile.itemText(i) for i in range(1, combffile.count())]
    combffile.clear()
    combffile.addItem("")
    if new_extension:
        combffile.addItem(new_extension)
        combffile.addItems(prev_items)
        index = combffile.findText(new_extension)
    else:
        combffile.addItems(default_extensions)

    if index is not None and index >= 0:
        combffile.setCurrentIndex(index)


def add_extension(default_extensions, logger, combffile, dbopt, dbtarget, email, nc, parent=None):

    res = False
    extension_value, ok = wdisplay_get_input(parent, 'Add ext', 'extension:')
    if ok:
        if not re.fullmatch(r'\.?[A-Za-z0-9_-]+', extension_value):
            wdisplay_message(parent, "Improper syntax for an extension")
            return
        else:

            ix = combffile.findText(extension_value)  # dont display extn table
            if ix == -1:

                try:
                    with DBMexec(dbopt, "sq_1", ui_logger=logger) as dmn:  # dmn.table_has_data(table_nm):

                        ts = QDateTime.currentDateTime().toString("yyyy-MM-dd HH:mm:ss")
                        sql = "INSERT OR IGNORE INTO extn (extension, timestamp) VALUES (:extn, :timestamp)"
                        params = {"extn": extension_value, "timestamp": ts}
                        if dmn.execute(sql, params):

                            fill_extensions(combffile, default_extensions, extension_value)
                            res = True
                        else:
                            logger.appendPlainText(f"Query failed add_extension for extension: {extension_value}")
                except DBConnectionError as e:
                    err_msg = f"Database connection error sq_1 {dbopt} in addext error: {e}"
                    logger.appendPlainText(err_msg)
                    logging.error(err_msg, exc_info=True)
                except Exception as e:
                    err_msg = f"Error while inserting extensions {type(e).__name__} {e}"
                    logger.appendPlainText(err_msg)
                    logging.error(err_msg, exc_info=True)

            else:
                logger.appendPlainText("Extension already listed")
    if res:
        if not encr(dbopt, dbtarget, email, nc, False):
            print("Failed to encrypt changes while saving extension. from add_extension qtfunctions")


def select_extensions(combffile, extensions, query):
    rlt = False

    combffile.clear()
    combffile.addItem("")
    if query.exec():
        rlt = True
        while query.next():
            extension = query.value(0)
            combffile.addItem(extension)
    combffile.addItems(extensions)
    return rlt


def user_data_to_database(notes, logger, dbopt, dbtarget, email, nc, isexit=False, parent=None):

    db = None
    query = None
    try:

        db, err = get_conn(dbopt, "sq_9")
        if err:
            print("Failed to connect to database in save_user_data")
        else:
            query = QSqlQuery(db)
            if save_note(logger, notes, query):  # save last used drive index to json
                db.close()

                if encr(dbopt, dbtarget, email, nc, False):
                    if not isexit:
                        logger.appendPlainText("Settings saved.")
                    return True

    except (FileNotFoundError, Exception) as e:
        logger.appendPlainText(f"unable to save user data save_user_data err:{type(e).__name__} {e}")
    finally:
        if query:
            del query
        if db:
            db.close()
    wdisplay_message(parent, "There was a problem rencrypting notes.", "Status")
    return False


def user_data_from_database(logger, textEdit, combffile, extensions, dbopt):
    query = None
    data_name = ""
    try:
        with DBMexec(dbopt, "sq_1", ui_logger=logger) as dmn:
            data_name = "extn"
            sql = "SELECT extension FROM extn WHERE id != 1"
            query = dmn.execute(sql)
            if query:
                select_extensions(combffile, extensions, query)
            else:
                logger.appendPlainText("Query failed extn table in user_data_from_database.")

            data_name = "notes"
            sql = "SELECT notes FROM extn WHERE id = 1"
            query = dmn.execute(sql)
            if query.exec() and query.next():
                notes = query.value(0)
                textEdit.setPlainText(notes)

    except DBConnectionError as e:
        err = ""
        if query:
            err = f":{query.lastError().text()}"
        err_msg = f'Database failed to load user data loading {data_name}  last query error: {err} error : {e}'
        logger.appendPlainText(err_msg)
        logging.error(err_msg, exc_info=True)
    except Exception as e:
        err = ""
        if query:
            err = f":{query.lastError().text()}"
        err_msg = f"err while loading user data table extn loading {data_name} fail: {type(e).__name__} {e} query err: {err}"
        logger.appendPlainText(err_msg)
        logging.error(err_msg, exc_info=True)


def is_model_ssd(model: str) -> bool:
    SSD_KEYWORDS = [
        "SSD", "NVME", "NVM", "M.2", "EVO",
        "SOLID", "FLASH", "V-NAND", "3D NAND"
    ]
    if not model:
        return False
    m = model.upper()
    return any(keyword in m for keyword in SSD_KEYWORDS)


def current_drive_type_model_check(ROOT_DIR="C:\\"):
    try:
        drive_model = "Unknown"
        drive_type = None

        drive = parse_drive(ROOT_DIR).upper() + ":"

        c = wmi.WMI()

        partitions = c.query(
            f"ASSOCIATORS OF {{Win32_LogicalDisk.DeviceID='{drive}'}} "
            "WHERE AssocClass = Win32_LogicalDiskToPartition"
        )
        if not partitions:
            return drive_model, "SSD"  # Ram Disk as not listed as logical drives

        logical = c.Win32_LogicalDisk(DeviceID=drive)[0]
        if not logical:
            return None, None

        partition = partitions[0]
        disk_number = partition.DiskIndex
        volume_number = partition.Index

        print("disk_number, volume_number", disk_number, volume_number)
        disk = partition.associators("Win32_DiskDriveToDiskPartition")[0]
        if not disk:
            return None, None

        model_frm = disk.Model
        if model_frm:
            drive_model = model_frm

        # print(dir(disk)) debug
        # disks = c.Win32_DiskDrive()
        # disk_full = next((d for d in disks if d.DeviceID == disk.DeviceID), None)
        # if disk_full is None:
        #     print("Disk not found!")
        # else:
        #     for prop in disk_full.properties:
        #         print(f"{prop}: {getattr(disk_full, prop)}")

        # see if its an SSD possibly newer hard disk with RotationRate in wmi

        # media = (disk.MediaType or "").lower()
        if is_model_ssd(drive_model):
            drive_type = "SSD"
        pnp = (disk.PNPDeviceID or "").lower()
        if "nvme" in pnp:
            drive_type = "SSD"

        if drive_type != "SSD":
            if hasattr(disk, "RotationRate"):
                rotation = disk.RotationRate
                if rotation == 0:
                    drive_type = "SSD"
                elif rotation is not None:
                    drive_type = "HDD"
            else:
                print("Tried to use RotationRate and not available, fallback needed")

    except Exception:
        return None, None
    return drive_model, drive_type


# check by model type, pnp description or rotation. if not run read test fall back to write test. if all fails set to HDD.
# user can set in config file config.toml for basedir. user can set in usrprofile.toml for index drive.
# Newer HDD drives have RotationRate in wmi. Older or legacy drives do not.
def setup_drive_settings(basedir, modelTYPE, user_json, toml_file=None, idx_drive=False, lclapp_data=None):

    if basedir != "C:\\" and not os.path.exists(basedir):
        print("setup_drive_setting setting drive:", basedir)
        print("unable to find drive")
        return None
    if modelTYPE:
        return modelTYPE

    drive_type = "HDD"
    drive_model = None
    mmode = None
    speedMB = None

    print("Determining drive type by model or speed test")
    dmodel, dtype = current_drive_type_model_check(basedir)

    if dmodel is None and dtype is None:
        print("Couldnt determine speed defaulting to HDD. Change in config.toml to SSD to use multicore")
    else:
        print("Running speed test")
        if dtype == "SSD":
            drive_type = dtype
        elif dtype is None:
            mmode = "read"
            speedMB = measure_read_speed(basedir)

            if speedMB is None:
                mmode = "write"
                if lclapp_data and not idx_drive:
                    target_path = lclapp_data
                else:
                    target_path = basedir
                speedMB = measure_write_speed(basedir, target_path, WRITE_MB=200)

            if speedMB is None:
                mmode = None
                print("Couldnt determine speed of drive defaulting to HDD for serial fsearch and ha")
            elif speedMB > 300:
                drive_type = "SSD"
        drive_model = dmodel
        if dmodel is None:
            drive_model = "Unknown"

    if toml_file and not idx_drive:
        update_toml_setting('search', 'modelTYPE', drive_type, toml_file)  # update config.toml the basedir

    # config.toml is where basedir ie C:\\ info is stored. the 'modelTYPE' HDD or SSD
    # if its a basedir we only want to put the info in the usrprofile.toml if we have it. This is used for diagnostics to return more info about settings in ui.
    # if we were to put the wrong info in usrprofile.toml and config.toml the user would have to update two config files which is unlikely.
    #
    # if its an idx_drive we need this info regardless as usrprofile.toml is where its info is stored. 'drive_type' and 'drive_model'
    if idx_drive or drive_model:
        if drive_model is None:  # if we dont know its HDD and "Unknown" so it defaults to serial
            drive_model = "Unknown"
        set_j_settings({"drive_type": drive_type, "drive_model": drive_model}, drive=basedir, filepath=user_json)

    if mmode and speedMB:
        if mmode == "read":
            set_j_settings({"read_speed": speedMB}, drive=basedir, filepath=user_json)
        elif mmode == "write":
            set_j_settings({"write_speed": speedMB}, drive=basedir, filepath=user_json)

    if dmodel:
        print(f"model {dmodel}")
    print(f"type {drive_type}")
    return drive_type


def collect_files(root, min_size_mb=100):
    skip_dirs = {"appdata", "windows"}
    file_list = []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False, onerror=os_walk_error):
        dirnames[:] = [d for d in dirnames if d.lower() not in skip_dirs]
        for name in filenames:
            path = os.path.join(dirpath, name)
            try:
                if os.path.isfile(path) and os.path.getsize(path) >= min_size_mb*1024*1024:
                    file_list.append(path)
            except Exception:
                pass
    return file_list


def measure_read_speed(root_dir="C:\\", target_gb=1):
    TARGET_BYTES = target_gb * 1024 * 1024 * 1024
    BLOCK_SIZE = 1024 * 1024
    files = collect_files(root_dir)
    if not files:
        print("No large files found to read for speed test")
        return None
    total_read = 0
    start = time.time()
    for file_path in random.sample(files, len(files)):  # for file_path in files:
        if total_read >= TARGET_BYTES:
            break
        try:
            with open(file_path, "rb", buffering=0) as f:
                remaining = TARGET_BYTES - total_read
                while remaining > 0:
                    chunk_size = min(BLOCK_SIZE, remaining)
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    total_read += len(chunk)
                    remaining -= len(chunk)
        except Exception:
            pass
    end = time.time()
    elapsed = end - start
    if elapsed == 0:
        print("Elapsed time is zero; cannot measure read speed.")
        return None
    total_mb = total_read / (1024 * 1024)
    speedMB = total_mb / elapsed
    print(f"Read {total_mb:.2f} MB in {elapsed:.2f} seconds")
    print(f"Average speed: {speedMB:.1f} MB/s")
    return speedMB


def measure_write_speed(ROOT_DIR="C:\\", target_path="C:\\", WRITE_MB=200):

    def write_file(write_path):
        with open(write_path, "wb") as f:
            f.write(b"A" * WRITE_MB*1024*1024)
            f.flush()
            os.fsync(f.fileno())

    try:
        drive_info = get_disk_and_volume_for_drive(ROOT_DIR)
        if drive_info:
            device_id, _ = drive_info

            pd = f"PhysicalDrive{device_id}"
            io1 = psutil.disk_io_counters(perdisk=True)[pd]
            start = time.time()
            write_path = os.path.join(target_path, "speedtest.bin")
            write_file(write_path)
            end = time.time()
            io2 = psutil.disk_io_counters(perdisk=True)[pd]

            written_bytes = io2.write_bytes - io1.write_bytes

            speed_MBps = written_bytes / (1024*1024) / (end - start)

        else:  # could be a virtual disk or ram drive and not listed. Its path was verified earlier

            write_path = os.path.join(target_path, "speedtest.bin")
            start = time.time()
            write_file(write_path)
            end = time.time()

            speed_MBps = WRITE_MB / (end - start)
        print(f"Write speed: {speed_MBps:.2f} MB/s")
        removefile(write_path)
        return speed_MBps
    except (RuntimeError, ValueError, TypeError):
        return None
