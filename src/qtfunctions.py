import ctypes
import logging
import os
import re
import requests
import sqlite3
import subprocess
import traceback
import webbrowser
import winreg
from packaging import version
from pathlib import Path
from PySide6.QtCore import QDateTime
from PySide6.QtGui import QIcon, QFontDatabase, QImage
from PySide6.QtSql import QSqlDatabase, QSqlQuery
from PySide6.QtWidgets import QVBoxLayout, QDialog, QPushButton, QLabel, QInputDialog, QMessageBox, QHBoxLayout
from .config import update_toml_values
from .dbmexec import DBConnectionError
from .dbmexec import DBMexec
from .gpgcrypto import decr
from .gpgcrypto import decrypt_from_text
from .gpgcrypto import encr
from .gpgcrypto import encrypt_to_text
from .pyfunctions import is_integer
from .rntchangesfunctions import get_default_distro
from .rntchangesfunctions import get_version1
from .rntchangesfunctions import is_wsl
from .rntchangesfunctions import set_to_wsl1

# 03/06/2026
# gestures
#
# QMessageBox.critical(None, "Error", "query failed")
# #
#


def window_prompt(parent, title, message, affirm, reject):  # y/n
    msg_box = QMessageBox(parent)
    msg_box.setWindowTitle(title)
    msg_box.setText(message)
    msg_box.setWindowIcon(QIcon("/usr/local/recentchanges/Resources/cleo.png"))
    import_button = msg_box.addButton(affirm, QMessageBox.ButtonRole.AcceptRole)
    default_button = msg_box.addButton(reject, QMessageBox.ButtonRole.RejectRole)  # noqa: F841
    msg_box.exec()
    return msg_box.clickedButton() == import_button


def window_message(parent, message, title="Status", default=True):  # ok
    msg = QMessageBox(parent)
    if default:
        msg.setIcon(QMessageBox.Icon.Warning)
    else:
        msg.setIcon(QMessageBox.Icon.Information)
    msg.setWindowTitle(title)
    msg.setText(message)
    msg.setWindowIcon(QIcon("/usr/local/recentchanges/Resources/48.png"))
    msg.setStandardButtons(QMessageBox.StandardButton.Ok)
    msg.exec()


def window_input(parent, title, value_title):
    return QInputDialog.getText(parent, title, value_title)


def profile_to_str(ps, is_exec):
    sep = ' ' if is_exec else ', '
    return sep.join(ps)


def ps_profile_type(profile):
    if not profile:
        return False
    if len(profile) == 1:
        if "exec" in profile[0]:
            return True
    return False


# Take care of setting this so not to prompt repeatedly

def find_wsl(self, parent=None):
    dm = "switching to powershell"
    # if hasattr(self, 'timer'):
    #     self.timer.stop()
    if is_wsl():
        default = get_default_distro()
        res = get_version1()

        if default and not res:
            reply = QMessageBox.question(
                parent,
                "Confirm Action",
                "WSL installed. it is required to change to WSL1 continue? Otherwise powershell will be used",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.Yes:
                if set_to_wsl1(default):
                    return True
                else:
                    window_message(parent, f"Unable to set wsl1. {dm}")
        elif not default:  # and not res:
            window_message(parent, f"Unable to get default distro for wsl.. {dm}")
        else:
            return True

    else:
        window_message(parent, "WSL not installed defaulting to off")

    update_toml_values({'search': {'wsl': False}}, self.toml_file)

    return False


def sort_right(tables, cache_table, systimeche, suffix):
    a, b, c, tbl = [], [], [], []
    # systime = self.systimeche.split("_", 1)[-1]  # sys_delim = systime + "_"
    is_basedir = suffix == "c"  # "C:\\"
    if is_basedir:
        for t in tables:
            if "sys" in t and "_" in t:
                b.append(t)
            elif "cache" in t and t != cache_table:
                b.append(t)
            else:
                a.append(t)
        tbl = a + b
    else:
        for t in tables:
            if t.startswith("sys"):
                if suffix in t:
                    if t == systimeche:
                        c.append(t)  # major long string goes to c
                    else:
                        a.append(t)  # major
                else:
                    b.append(t)
            elif t.startswith("cache"):
                if t == cache_table:
                    c.append(t)  # minor goes to c
                else:
                    b.append(t)
            else:
                tbl.append(t)  # reg sql table
        tbl = tbl + a + c + b
    return tbl


def select_custom(parent, title, msg, importjpg, defaultjpg, importcrest, defaultcrest):

    dlg = QDialog(parent)
    dlg.setWindowTitle(title)

    layout = QVBoxLayout(dlg)

    label = QLabel(msg)
    layout.addWidget(label)

    # Jpg
    row1 = QHBoxLayout()
    btn_importjpg = QPushButton(importjpg)
    btn_resetjpg = QPushButton(defaultjpg)
    row1.addStretch()
    row1.addWidget(btn_importjpg)
    row1.addWidget(btn_resetjpg)
    row1.addStretch()
    layout.addLayout(row1)
    # Crest
    row2 = QHBoxLayout()
    btn_importcrest = QPushButton(importcrest)
    btn_resetcrest = QPushButton(defaultcrest)
    row2.addStretch()
    row2.addWidget(btn_importcrest)
    row2.addWidget(btn_resetcrest)
    row2.addStretch()
    layout.addLayout(row2)

    row3 = QHBoxLayout()
    btn_embosscrest = QPushButton("Emboss Crest")
    row3.addStretch()
    row3.addWidget(btn_embosscrest)
    row3.addStretch()
    layout.addLayout(row3)

    result = {"value": None}
    buttons = [btn_importjpg, btn_resetjpg, btn_importcrest, btn_resetcrest, btn_embosscrest]
    max_w = max(b.sizeHint().width() for b in buttons)
    max_h = max(b.sizeHint().height() for b in buttons)

    for b in buttons:
        b.setFixedSize(max_w, max_h)
    # Connect buttons to results
    btn_importjpg.clicked.connect(lambda: (result.update(value="jpg"), dlg.accept()))
    btn_resetjpg.clicked.connect(lambda: (result.update(value="defjpg"), dlg.accept()))
    btn_importcrest.clicked.connect(lambda: (result.update(value="crest"), dlg.accept()))
    btn_resetcrest.clicked.connect(lambda: (result.update(value="defcrest"), dlg.accept()))
    btn_embosscrest.clicked.connect(lambda: (result.update(value="emboss"), dlg.accept()))

    dlg.exec()
    return result["value"]


def valid_crest(parent, crest_path):
    img = QImage(crest_path)

    if img.isNull():
        QMessageBox.warning(parent, "Error", "Invalid image file.")
        return False

    w = img.width()
    h = img.height()

    if w > 255 or h > 333:
        window_message(parent, f"Image size must be 250x333 or less.\n\nSelected image: {w}x{h}", "Invalid size")
        return False
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


# if sys table has data prompt before continuing
# sys  for / sys_n for n drive
# cache_s for / or cache_sdx for device     directories at time of profile
# systimeche - systimeche_sdx   these are dirs as cache is updated
def has_sys_data(dbopt, logger, sys_table, prompt, parent=None):
    db = query = None
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
                if query.exec():
                    if query.next():
                        uinpt = window_prompt(parent, "Confirm Action", prompt, "Yes", "No")
                        if uinpt:
                            return True
                        if not uinpt:
                            return None
                else:
                    err = query.lastError()
                    if err.isValid():
                        error_message = f"query failed: {query.lastError().text()}"
                        logger.appendPlainText(error_message)
                        return None

            return False
    except Exception as e:
        mg = f"query error {type(e).__name__}: {e}"
        print(mg)
        if query:
            mg = mg + f"{query.lastError().text()}\n"
        logger.appendPlainText(mg)
        logging.error(mg, exc_info=True)
    finally:
        if db:
            db.close()
    return None


# check if there actually is a table before trying to do anything
def table_loaded(dbopt, table_nm, logger):
    try:
        if os.path.isfile(dbopt):
            with DBMexec(dbopt, "sq_1", ui_logger=logger) as dmn:
                if dmn.table_has_data(table_nm):
                    return True
    except DBConnectionError as e:
        logger.appendPlainText(f"Database table_loaded error: {e}")
    except Exception as e:
        emsg = f"err while checking table in table_loaded: {type(e).__name__} {e}"
        logger.appendPlainText(emsg)
        logging.error(f"{emsg} \n{traceback.format_exc()}")
    return False


def has_log_data(dbopt, logger, parent=None):
    try:
        with DBMexec(dbopt, "sq_1", ui_logger=logger) as dmn:  # dmn.table_has_data(table_nm):
            sql = "SELECT COUNT(*) FROM logs WHERE hardlinks IS NOT NULL AND hardlinks != ''"
            result = dmn.execute(sql)
            if result:
                result.next()
                count = result.value(0)
                if count > 0:
                    uinpt = window_prompt(parent, "Confirm Action", "Previous 'hardlinks' data has to be cleared. Continue? (y/n): ", "Yes", "No")
                    if not uinpt:
                        return False
                return True
            else:
                logger.appendPlainText("Query failed hlinks function")
    except DBConnectionError as e:
        err_msg = f"Database connection error sq_1 {dbopt} in hlinks error: {e}"
        logger.appendPlainText(err_msg)
        logging.error(err_msg, exc_info=True)
    except Exception as e:
        err_msg = f"Error while setting hardlinks {type(e).__name__} {e}"
        logger.appendPlainText(err_msg)
        logging.error(err_msg, exc_info=True)
    return False


def commit_note(logger, notes, email, query):
    try:
        encrypted_data = encrypt_to_text(notes, email)
        if encrypted_data is None:
            print("Problem encrypting notes aborting")
            return False
        # gpg = gnupg.GPG()
        # encrypted_data = gpg.encrypt(notes, recipients=[email])
        # if not encrypted_data.ok:
        #     print(encrypted_data.stderr)
        #     print("Problem encrypting notes aborting")
        #     return False
            # raise RuntimeError(f"Encryption failed: {encrypted_data.status}")
        ciphertext = str(encrypted_data)

        query.prepare("""
            INSERT INTO extn (id, notes)
            VALUES (1, :notes)
            ON CONFLICT(id) DO UPDATE SET notes = excluded.notes
        """)
        query.bindValue(":notes", ciphertext)
        if query.exec():
            return True
        else:
            err = query.lastError()
            if err and err.isValid():
                logger.appendPlainText(f"commit_note query err: {err.text()}\n")
            logger.appendPlainText("Failed to save notes to db")
    except Exception as e:
        logger.appendPlainText(f"Unable update notes savenote qtfunctions {type(e).__name__} err: {e} \n{traceback.format_exc()}")
    return False
# end QSql


def clear_cache(conn, cur, cachermPATTERNS, log_fn=print):

    files_d = cachermPATTERNS
    filename_pattern = None
    try:
        for filename_pattern in files_d:
            cur.execute("DELETE FROM logs WHERE filename LIKE ?", (filename_pattern,))
            cur.execute("DELETE FROM stats WHERE filename LIKE ?", (filename_pattern,))
        if filename_pattern is not None:
            conn.commit()
        log_fn("Cache files cleared.")
        return True
    except sqlite3.Error as e:
        conn.rollback()
        log_fn(f"cache_clear query.py failed to write to db. on {filename_pattern if filename_pattern else ''} {e} {type(e).__name__}")
    except Exception as e:
        conn.rollback()
        log_fn(f'General failure in query.py clear_cache: {e}')
    return False


def load_gpg(dbopt, dbtarget, user, logger):
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

        lclgpg, _ = os.path.splitext(default_gpg)

        if fingerprint:
            hudt.appendPlainText(f"Delete a GPG key for: {email}\n")

            hudt.appendPlainText(f'& "{lclgpg}" --homedir {gnupg_home} --delete-secret-key {fingerprint}')
            hudt.appendPlainText(f'& "{lclgpg}" --homedir {gnupg_home} --delete-key {fingerprint}')
            hudt.appendPlainText("\n")
        gpg_command = f'& "{lclgpg}" --homedir "{gnupg_home}"'

    else:
        if fingerprint:
            hudt.appendPlainText(f"Delete a GPG key for: {email}\n")
            hudt.appendPlainText(f"gpg --delete-secret-key {fingerprint}")
            hudt.appendPlainText(f"gpg --delete-key {fingerprint}")
        gpg_command = "gpg"

    hudt.appendPlainText("\n")
    hudt.appendPlainText("decrypt something (example check a cache file) from app directory")
    hudt.appendPlainText(
        f"{gpg_command} -o myfile.txt --decrypt {lclhome}\\{example_gpg}.gpg"
    )
    # end gpg info


def available_fonts(hudt):
    hudt.appendPlainText("System installed fonts\n")
    fonts = QFontDatabase.families()
    for f in fonts:
        hudt.appendPlainText(f)


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
        latest_version = data["tag_name"].lstrip("v")
        download_url = data["html_url"]
        return latest_version, download_url
    except Exception as e:
        print("Failed to fetch latest release:", e)
        return None, None


def check_for_updates(app_version, user, repo, parent=None):

    latest_version, _ = get_latest_github_release(user, repo)
    if latest_version and version.parse(latest_version) > version.parse(app_version):
        window_message(parent, f"New version available: {latest_version}", "Update msg", default=False)
    elif latest_version:
        window_message(parent, f"You are running the latest version. {app_version}", "Update msg", default=False)


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

    label = QLabel("v5.0\n\nCreated by Colby Saigeon\nh&k enterprisez\n\nFind recent files using powershell.")
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


def add_to_path(app_dir):
    key = winreg.OpenKey(
        winreg.HKEY_LOCAL_MACHINE,
        r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment",
        0, winreg.KEY_READ | winreg.KEY_WRITE
    )
    current_path, _ = winreg.QueryValueEx(key, "Path")
    paths = [p.strip() for p in current_path.split(";")]
    if app_dir.lower() not in [p.lower() for p in paths]:
        new_path = current_path + ";" + app_dir
        winreg.SetValueEx(key, "Path", 0, winreg.REG_EXPAND_SZ, new_path)
    winreg.CloseKey(key)
    ctypes.windll.user32.SendMessageTimeoutW(0xFFFF, 0x001A, 0, "Environment", 0x0002, 5000, None)


def remove_from_path(app_dir):
    key = winreg.OpenKey(
        winreg.HKEY_LOCAL_MACHINE,
        r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment",
        0, winreg.KEY_READ | winreg.KEY_WRITE
    )
    current_path, _ = winreg.QueryValueEx(key, "Path")
    paths = [p for p in current_path.split(";") if p.strip().lower() != app_dir.lower()]
    new_path = ";".join(paths)
    winreg.SetValueEx(key, "Path", 0, winreg.REG_EXPAND_SZ, new_path)
    winreg.CloseKey(key)
    ctypes.windll.user32.SendMessageTimeoutW(0xFFFF, 0x001A, 0, "Environment", 0x0002, 5000, None)


def command_prompt(lclhome, popPATH=None):
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
    # env = os.environ.copy()
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


def add_new_extension(default_extensions, logger, combffile, dbopt, dbtarget, email, nc, parent=None):

    res = False
    extension_value, ok = window_input(parent, 'Add ext', 'extension:')
    if ok:
        if not re.fullmatch(r'\.[A-Za-z0-9_-]+', extension_value):
            window_message(parent, "Improper syntax for an extension")
            return res
        else:

            ix = combffile.findText(extension_value)
            if ix == -1:

                try:
                    with DBMexec(dbopt, "sq_1", ui_logger=logger) as dmn:  # dmn.table_has_data(table_nm):

                        ts = QDateTime.currentDateTime().toString("yyyy-MM-dd HH:mm:ss")
                        sql = "INSERT OR IGNORE INTO extn (extension, timestamp) VALUES (:extn, :timestamp)"
                        params = {"extn": extension_value, "timestamp": ts}
                        if dmn.execute(sql, params):
                            prev_items = [combffile.itemText(i) for i in range(1, combffile.count())]
                            # if extension_value:
                            #     combffile.insertItem(0, extension_value) changes format use below
                            index = fill_extensions(combffile, default_extensions, extension_value, prev_items)
                            if is_integer(index) and index >= 0:
                                combffile.setCurrentIndex(index)
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
        if encr(dbopt, dbtarget, email, nc, True):
            return extension_value
        else:
            res = False
            print("Failed to encrypt changes while saving extension. from add_extension qtfunctions")
    return res


# find file combo box extensions
def fill_extensions(combffile, default_extensions, new_extension=None, prev_extensions=None, query=None):
    rlt = False
    c_text = combffile.currentText()

    combffile.clear()
    combffile.addItem("")
    if query:
        extns = []
        while query.next():
            extension = query.value(0)
            combffile.addItem(extension)
            extns.append(extension)
        combffile.addItems(default_extensions)
        return extns
    elif new_extension:
        combffile.addItem(new_extension)
        combffile.addItems(prev_extensions)
        index = combffile.findText(new_extension)
        return index
    elif prev_extensions:
        combffile.addItems(prev_extensions)
        ix = combffile.findText(c_text)
        if ix != -1:
            combffile.setCurrentIndex(ix)
        else:
            if combffile.count() > 0:
                combffile.setCurrentIndex(0)
    combffile.addItems(default_extensions)
    return rlt


def user_data_to_database(notes, logger, dbopt, dbtarget, email, nc, isexit=False, parent=None):
    db = None
    res = False
    try:

        db, err = get_conn(dbopt, "sq_9")
        if err:
            print("Failed to connect to database in save_user_data")
        else:
            query = QSqlQuery(db)
            res = commit_note(logger, notes, email, query)  # save last used drive index to json

    except (FileNotFoundError, Exception) as e:
        logger.appendPlainText(f"unable to save user data save_user_data err:{type(e).__name__} {e}")
    finally:
        if db:
            db.close()
            del db
    if res:
        if encr(dbopt, dbtarget, email, nc, True):
            if not isexit:
                logger.appendPlainText("Settings saved.")
            return True
    window_message(parent, "There was a problem rencrypting notes.", "Status")
    return False


def user_data_from_database(logger, textEdit, combffile, extensions, dbopt, parent=None):
    query = None
    extension_data = []
    data_name = ""
    try:
        with DBMexec(dbopt, "sq_1", ui_logger=logger) as dmn:
            data_name = "extn"
            sql = "SELECT extension FROM extn WHERE id != 1"
            query = dmn.execute(sql)
            if query:
                extension_data = fill_extensions(combffile, extensions, query=query)
            else:
                logger.appendPlainText("Query failed extn table in user_data_from_database.")
            data_name = "notes"
            sql = "SELECT notes FROM extn WHERE id = 1"
            query = dmn.execute(sql)
            if query and query.exec() and query.next():
                if query.value(0):

                    # logging.getLogger('gnupg').setLevel(logging.CRITICAL)
                    # gpg = gnupg.GPG()
                    encrypted_blob = query.value(0)
                    decrypted_data = decrypt_from_text(encrypted_blob)
                    if decrypted_data or decrypted_data == "":
                        notes = str(decrypted_data)
                        textEdit.setPlainText(notes)
                    # decrypted_data = gpg.decrypt(encrypted_blob)
                    # if decrypted_data.ok:
                    #     notes = str(decrypted_data)
                    #     textEdit.setPlainText(notes)
                    elif decrypted_data is None:
                        print("Could not decrypt notes")
                        # passphrase, ok = QInputDialog.getText(parent, "Decrypt Notes", "Enter GPG passphrase:", echo=QLineEdit.EchoMode.Password)
                        # if ok and passphrase:
                        #     decrypted_data = gpg.decrypt(
                        #         encrypted_blob,
                        #         passphrase=passphrase,
                        #         extra_args=["--pinentry-mode", "loopback"]
                        #     )
                        #     if decrypted_data.ok:
                        #         notes = str(decrypted_data)
                        #         textEdit.setPlainText(notes)
                        # else:
                        #     logger.appendPlainText(decrypted_data.stderr)
                        #     print("Could not decrypt notes")
            return extension_data
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
    return []
