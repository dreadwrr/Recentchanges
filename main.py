# 12/08/2025               Qt gui windows 11                  Developer buddy 3.0
import glob
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import traceback
import win32api
from pathlib import Path
from PySide6.QtCore import Qt, Slot, Signal, QThread, QTimer, QSortFilterProxyModel, QSize
from PySide6.QtGui import QStandardItemModel, QStandardItem, QIcon, QPixmap, QImage  # QFontDatabase
from PySide6.QtSql import QSqlQuery
from PySide6.QtWidgets import QApplication, QFileDialog, QMessageBox, QMainWindow, QInputDialog, QLineEdit, QVBoxLayout, QMenu, QHeaderView, QStyle, QHBoxLayout, QDialog, QLabel, QPushButton
# project_root = Path(__file__).parent # original during development nwo using relative imports
# sys.path.append(str(project_root / "src"))
from src.clearworker import ClearWorker
from src.dbworkerstream import DbWorkerIncremental
from src.imageraster import raised_image
from src.mftworker import MftWorker
from src.processhandler import ProcessHandler
from src.pstsrg import create_db
from src.pyfunctions import is_integer
from src.pyfunctions import load_config
from src.pyfunctions import setup_logger
from src.qtfunctions import add_extension
from src.qtfunctions import check_for_updates
from src.qtfunctions import current_drive_type_model_check
from src.qtfunctions import show_cmddoc
from src.qtfunctions import DBMexec
from src.qtfunctions import DBConnectionError
from src.qtfunctions import DriveSelectorDialog
from src.qtfunctions import FastColorText
from src.qtfunctions import fill_extensions
from src.qtfunctions import get_conn
from src.qtfunctions import get_help
from src.qtfunctions import has_sys_data
from src.qtfunctions import help_about
from src.qtfunctions import load_cmdpmpt
from src.qtfunctions import load_explorer
from src.qtfunctions import load_gpg
from src.qtfunctions import load_pshell
from src.qtfunctions import open_html_resource
from src.qtfunctions import QTextEditLogger
from src.qtfunctions import save_note
from src.qtfunctions import user_data_from_database
from src.qtfunctions import user_data_to_database
from src.qtfunctions import set_j_settings
from src.qtfunctions import get_j_settings
from src.qtfunctions import setup_drive_settings
from src.qtfunctions import wdisplay_prompt
from src.qtfunctions import wdisplay_message
from src.qtfunctions import wdisplay_get_input
from src.query import clear_extn_tbl
from src.query import dbtable_has_data
from src.rntchangesfunctions import check_for_gpg
from src.rntchangesfunctions import check_utility
from src.rntchangesfunctions import convertn
from src.rntchangesfunctions import decr
from src.rntchangesfunctions import display
from src.rntchangesfunctions import encr
from src.rntchangesfunctions import find_user_folder
from src.rntchangesfunctions import genkey
from src.rntchangesfunctions import getnm
from src.rntchangesfunctions import get_cache_s
from src.rntchangesfunctions import get_default_distro
from src.rntchangesfunctions import get_diffFile
from src.rntchangesfunctions import get_idx_tables
from src.rntchangesfunctions import get_usr
from src.rntchangesfunctions import get_version1
from src.rntchangesfunctions import intst
from src.rntchangesfunctions import is_admin
from src.rntchangesfunctions import is_wsl
from src.rntchangesfunctions import check_installed_app
from src.rntchangesfunctions import iskey
from src.rntchangesfunctions import res_path
from src.rntchangesfunctions import resolve_editor
from src.rntchangesfunctions import mftec_version
from src.rntchangesfunctions import multi_value
# from src.rntchangesfunctions import pwsh_7
from src.rntchangesfunctions import set_gpg
from src.rntchangesfunctions import set_to_wsl1
from src.rntchangesfunctions import update_toml_setting
from src.ui_mainwindow import Ui_MainWindow
from src.wmipy import get_disk_and_volume_for_drive
from src.wmipy import get_mounted_partitions
from src.wmipy import validmft


class MainWindow(QMainWindow):

    worker_timeout_sn = Signal()
    proc_timeout_sn = Signal()
    stop_worker_sn = Signal()  # stop thread or proc
    stop_proc_sn = Signal()
    reload_database_elesn = Signal(int, bool, object)  # hudt append text
    reload_ui_elesn = Signal(int, str)  # change checkboxes after QProcess or thread
    reload_drives_sn = Signal(int, int, str)  # update drive combobox on complete
    reload_sj_sn = Signal(int, str, str)  # also update drive combo on complete

    def __init__(self, appdata_local, config, toml_file, json_file, driveTYPE, dbopt, dbtarget, gpg_path, gnupg_home, dspEDITOR, dspPATH, popPATH, email, usr, tempdir):
        super().__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

        self.USRDIR = find_user_folder("Desktop")  # change for linux
        if self.USRDIR is None:
            raise EnvironmentError("Could not find user Desktop folder")
        self.PWD = os.getcwd()

        self.toml_file = toml_file  # /home/guest/.config/config.toml linux
        self.sj = json_file
        self.driveTYPE = driveTYPE
        self.dbopt = dbopt  # db
        self.dbtarget = dbtarget  # gpg
        self.gnupg_home = gnupg_home
        self.dspEDITOR = dspEDITOR
        self.dspPATH = dspPATH
        self.popPATH = popPATH
        self.email = email
        self.usr = usr

        self.tempdir = tempdir  # thisapp

        self.ANALYTICSECT = config['analytics']['ANALYTICSECT']
        self.FEEDBACK = config['analytics']['FEEDBACK']
        self.compLVL = config['logs']['compLVL']
        self.cacheidx = config['diagnostics']['proteus_CACHE']
        self.hudCOLOR = config['display']['hudCOLOR']
        self.hudSZE = config['display']['hudSZE']
        self.hudFNT = config['display']['hudFNT']
        self.MODULENAME = config['paths']['MODULENAME']  # difffileprefix

        flth_frm = appdata_local / "flth.csv"  # res_path(config['paths']['flth'], usr)
        self.flth = str(flth_frm)

        CACHE_S_frm = appdata_local / "systimeche.gpg"  # res_path(config['paths']['CACHE_S'], usr)
        self.CACHE_S = str(CACHE_S_frm)
        self.oldCACHE_S = self.CACHE_S

        self.wsl = config['search']['wsl']
        self.basedir = config['search']['drive']  # search target

        psEXTN = get_j_settings(["proteus_EXTN"], drive=self.basedir, filepath=self.sj).get("proteus_EXTN")
        if not psEXTN:
            psEXTN = config['diagnostics']['proteus_EXTN']
        self.psEXTN = psEXTN
        self.checksum = config['diagnostics']['checkSUM']
        self.updatehlinks = config['diagnostics']['updatehlinks']
        self.proteusSHIELD = config['diagnostics']['proteusSHIELD']
        self.EXCLDIRS = res_path(config['search']['EXCLDIRS'], usr)
        zipPROGRAM = config['search']['zipPROGRAM']
        self.zipPROGRAM = zipPROGRAM.lower()
        self.zipPATH = config['search']['zipPATH']
        self.downloads = res_path(config['search']['downloads'], usr)
        self.extensions = config['search']['extension']

        # original
        # check for wsl shortly
        # self.timer = QTimer(self)
        # self.timer.timeout.connect(self.findwsl)
        # self.timer.start(1000)

        # load the database at some point if not already by pg change
        # QTimer.singleShot(5000, self.displaydb)

        # Vars
        self.app_version = "3.0.0"
        self.lclhome = appdata_local
        self.lclscripts = appdata_local / "scripts"
        self.myapp = appdata_local / "src" / "recentchangessearch.py"  # /usr/local/save-changesnew/ linux
        self.filter_file = appdata_local / "filter.py"
        self.default_gpg = appdata_local / "gpg" / "gpg.exe"

        self.gpg_path = self.default_gpg
        if gpg_path:
            self.gpg_path = gpg_path

        self.defaultzipPATH = appdata_local / "7-zip" / "7z.exe"

        self.exe_path = self.lclhome / "bin"
        self.mftec_command = self.exe_path / "MFTECmd.exe"
        self.icat_command = self.exe_path / "icat.exe"
        self.ntfs_command = self.exe_path / "ntfstool.x86.exe"
        self.fsstat_command = self.exe_path / "fsstat.exe"

        self.jpgdir = appdata_local / "Documents"  # str(Path.home() / "Documents")   /home/guest/.config/icons/
        self.crestdir = self.jpgdir / "crests"

        self.picture = self.jpgdir / "background.png"  # current png
        self.crest = self.crestdir / "dragonm.png"  # . crest

        self.jpgdefault = "background.bak"  # default png
        self.crestdefault = "dragonm.bak"  # . crest

        self.cmddoc = self.lclhome / "Resources" / "commands.txt"

        self.defaultdiff = os.path.join(self.USRDIR, f'{self.MODULENAME}xSystemDiffFromLastSearch500.txt')
        self.mftflnm = f'{self.MODULENAME}xMftchanges'

        self.CACHE_S, self.systimeche = get_cache_s(self.basedir, self.CACHE_S)  # ie C:\\path\\systemche J:\\path\\systimeche_j
        sys_tables, self.cache_table = get_idx_tables(self.basedir)
        self.sys_a, self.sys_b = sys_tables

        self.isexec = False
        self.is_user_abort = False
        self.dirtybit = False  # something to save while the db is connected or program exit

        self.difffile = None

        self.mft = None  # is imported Mft
        self.ramdisk = None  # self.lclhome

        self.worker_thread = None
        self.worker = None
        self.worker2 = None  # database streamer
        self.proc = None

        self.db = None  # set after first db load
        self.table = None  # last loaded table

        self.lastdir = None
        self.last_drive = self.basedir

        self.result = None
        self.resStatus = None

        self.nc = False

        # initialize
        self.init_timers()
        self.init_events()
        self.install_logger()
        self.initialize_ui()

    @Slot(str)
    def append_log(self, text):
        self.ui.hudt.append_colored_output(text)

    @Slot(str)
    def update_db_status(self, text):
        # control dbmainlabel / ui elements
        self.ui.dbmainlabel.setText(text)
        if self._status_reset_timer.isActive():
            self._status_reset_timer.stop()
        self._status_reset_timer.start(40000)

    @Slot(float)
    def increment_progress(self, value):
        self.ui.progressBAR.setValue(value)

    @Slot(float)
    def increment_db_progress(self, value):
        self.ui.dbprogressBAR.setValue(value)

    def install_logger(self):
        # hudt
        old_widget = self.ui.hudt
        layout = self.ui.gridLayout
        index = layout.indexOf(old_widget)
        position = layout.getItemPosition(index)
        layout.removeWidget(old_widget)
        old_widget.deleteLater()

        self.ui.hudt = FastColorText()  # hud/terminal colors gui
        self.ui.hudt.setStyleSheet(
            "QPlainTextEdit {"
            " background-color: black;"
            " font-family: Consolas, Courier, monospace;"
            " font-size: 12pt;"
            "}"
        )
        layout.addWidget(self.ui.hudt, *position)

        # self.ui.hudt.append_colored_output("\033[31mRed text\033[0m")  test nested ansi colors
        # self.ui.hudt.append_colored_output("\033[1;32mGreen bold\033[0m\033[31mRed text\033[0m")

        self.logger = QTextEditLogger(None)  # self.append_colored_output)
        self.logger.new_message.connect(self.ui.hudt.append_colored_output)
        self.main_stdout = sys.stdout
        self.main_stderr = sys.stderr
        sys.stdout = self.logger
        sys.stderr = self.logger  # self.logger.output_handler = self.ui.hudt.append_colored_output  not thread safe
        # end hudt

    def init_events(self):

        self.reload_ui_elesn.connect(self.update_ui_settings)
        self.reload_database_elesn.connect(self.reload_database)
        self.reload_drives_sn.connect(self.reload_drives)
        self.reload_sj_sn.connect(self.manage_sj)

        # Menu bar
        self.ui.actionStop.triggered.connect(self.x_action)

        self.ui.actionSave.triggered.connect(self.save_user_data)
        self.ui.actionClearh.triggered.connect(lambda _: self.ui.hudt.clear())

        self.ui.actionExit.triggered.connect(QApplication.quit)
        self.ui.actionClear_expensions.triggered.connect(self.clear_extensions)
        self.ui.actionUpdates.triggered.connect(lambda: check_for_updates(self.app_version, "dreadwrr", "Recentchanges", self))

        self.ui.actionCommands_2.triggered.connect(lambda: show_cmddoc(self.cmddoc, self.lclhome, self.default_gpg, self.gpg_path, self.gnupg_home, self.email, self.systimeche, self.ui.hudt))
        self.ui.actionQuick1.triggered.connect(lambda: display(self.dspEDITOR, self.cmddoc, True, self.dspPATH))
        self.ui.actionDiag1.triggered.connect(self.show_status)

        self.ui.actionAbout.triggered.connect(lambda: help_about(self.lclhome, self.ui.hudt))
        self.ui.actionResource.triggered.connect(self.open_resource)
        self.ui.actionHelp.triggered.connect(lambda: get_help(self.lclhome, self.ui.hudt))

        # # 1 left <
        self.ui.queryButton.clicked.connect(self.execute_query)

        # Main window

        # Stop Reset defaults button #1 mid ^
        self.ui.resetButton.clicked.connect(self.x_action)
        # Find createdfiles
        self.ui.downloadButton.clicked.connect(lambda: self.find_downloads(self.basedir))
        self.ui.rmvButton.clicked.connect(self.rmv_idx_drive)
        self.ui.addButton.clicked.connect(self.idx_drive)
        # right >
        self.ui.jpgb.clicked.connect(self.load_jpg)
        # tomlb `settings` button
        # self.ui.tomlb.clicked.connect(self.showsettings)
        menu = QMenu(self)
        menu.addAction("Settings", lambda: display(self.dspEDITOR, self.toml_file, True, self.dspPATH))
        menu.addAction("Explorer", lambda: load_explorer(self.lclhome, popPATH=self.popPATH))
        menu.addAction("Launch Cmd Prompt", lambda: load_cmdpmpt(self.lclhome, popPATH=self.popPATH))
        menu.addAction("Launch Powershell", lambda: load_pshell(self.lclhome, popPATH=self.popPATH))
        menu.addSeparator()
        menu.addAction("Filter", lambda: display(self.dspEDITOR, self.filter_file, True, self.dspPATH))
        menu.addAction("Clear Hudt", lambda: self.ui.hudt.clear())
        self.ui.tomlb.setMenu(menu)
        self.ui.tomlb.setPopupMode(self.ui.tomlb.ToolButtonPopupMode.InstantPopup)
        # bottom right
        self.ui.textEdit.textChanged.connect(lambda: setattr(self, 'dirtybit', True))
        # Top search
        self.ui.ftimeb.clicked.connect(lambda checked=False, s=self.ui.ftimeb: self.tsearch(s))
        self.ui.ftimebf.clicked.connect(lambda checked=False, s=self.ui.ftimebf: self.tsearch(s, True))
        self.ui.stimeb.clicked.connect(lambda checked=False, s=self.ui.stimeb: self.tsearch(s))
        self.ui.stimebf.clicked.connect(lambda checked=False, s=self.ui.stimebf: self.tsearch(s, True))
        # New than search
        self.ui.ntsb.clicked.connect(self.ntsearch)
        self.ui.ntbrowseb.clicked.connect(self.ntsearch)
        self.ui.ntbrowseb2.clicked.connect(self.ntsearch)
        # findfile                                   # End Top
        self.ui.ffileb.clicked.connect(lambda: self.ffile(False))

        self.ui.ffileb2.clicked.connect(self.new_extension)
        self.ui.ffilecb.clicked.connect(self.ffcompress)
        # mft
        self.ui.mftbrowseb.clicked.connect(self.imprt_mft_brws)
        self.ui.mftsearchb.clicked.connect(self.mftsearch)
        self.ui.combdb.currentTextChanged.connect(self.displaydb)
        # page_2
        self.ui.dbmainb1.clicked.connect(self.clear_cache)
        self.ui.dbmainb2.clicked.connect(self.super_impose)
        # refresh button pg2
        self.ui.dbmainb3.setIcon(self.ui.dbmainb3.style().standardIcon(QStyle.StandardPixmap.SP_ArrowLeft))
        self.ui.dbmainb3.setIconSize(QSize(20, 20))
        self.ui.dbmainb3.clicked.connect(self.reload_table)

        self.ui.dbidxb1.clicked.connect(lambda checked, drive=self.basedir: self.clear_sys(drive))

        self.ui.dbidxb2.clicked.connect(self.build_idx)
        self.ui.dbidxb3.clicked.connect(self.scan_idx)
        # nav   # End page_2
        self.ui.toolhomeb_2.clicked.connect(self.show_page)
        self.ui.toolrtb.clicked.connect(self.show_page_2)
        self.ui.toolrtb_2.clicked.connect(self.show_page)
        self.ui.toollftb_2.clicked.connect(self.show_page)
        self.ui.toollftb.clicked.connect(self.show_page_2)
        # End nav
        # End Main window

    def load_last_drive(self, last_drive=None):

        def set_drive(target):
            idx = self.ui.combd.findText(target)
            if idx != -1:
                self.ui.combd.setCurrentIndex(idx)
                self.last_drive = target
                return True
            return False

        if last_drive:
            set_drive(last_drive)  # set it and return
            return

        last_drive_setting = get_j_settings(["last_drive"], filepath=self.sj)
        drive = last_drive_setting.get("last_drive")

        if drive and drive != self.basedir:
            if not set_drive(drive):
                self.last_drive = self.basedir
                self.ui.combd.setCurrentIndex(0)
                set_j_settings({"last_drive": self.basedir}, filepath=self.sj)

    def initialize_ui(self):

        self.ui.dbprogressBAR.setValue(0)
        pixmap = QPixmap(self.crest)  # Load the image from the path      '.\\Documents\\crests\\dragonm.png'  # original
        self.ui.jpgcr.setPixmap(pixmap)  # Set the pixmap on the label
        self.ui.jpgcr.setScaledContents(True)

        self.change_format()  # apply hudt settings
        self.refresh_jpg()  # load pic

        if os.path.isfile(self.dbopt):

            QTimer.singleShot(1000, self.load_user_data)

        elif not os.path.isfile(self.dbtarget):
            try:
                create_db(self.dbopt, (self.sys_a, self.sys_b))
                # if not encr(self.dbopt, self.dbtarget, self.email, self.nc, False):
                # self.ui.hudt.appendPlainText("Unable to create database")
            except Exception as e:
                QMessageBox.critical(None, "Error", f"Problem creating database through initializer. Exiting.. {e}")
                QApplication.exit(1)

        # fill combos
        a_drives = self.load_saved_indexes()  # drive combo pg_2
        self.fill_download_combo(a_drives)

        self.load_last_drive()

        downloads = self.downloads
        if downloads.strip():
            # index = self.ui.combffileout.count() - 1
            # self.ui.combffileout.setCurrentIndex(index)
            self.ui.combffileout.addItem("Downloads")
            self.ui.combffileout.setCurrentText("Downloads")
        # end fill combos

        self.wsl = self.findwsl()

    # Custom settings for hudt
    def set_stylesht(self, f_f, ccolor):
        # print(QFontDatabase().families())
        if not is_integer(self.hudSZE):
            self.ui.hudt.appendPlainText(f"Invalid size format hudSZE: {self.hudSZE} defaulting to 12")
            self.hudSZE = 12
            update_toml_setting('display', 'hudSZE', 12, self.toml_file)
        else:
            if self.hudSZE == 0:
                self.hudSZE = 12
        self.ui.hudt.setStyleSheet(f"""
            QPlainTextEdit {{
                background-color: black;
                color: #{ccolor};
                font-family: {f_f};
                font-size: {self.hudSZE}pt;
            }}
        """)

    def change_format(self):
        # print(QFontDatabase().families()) original
        # cfonts = ["Lucida", "Courier New", "Consolas"]
        # if self.hudFNT not in cfonts:
        # self.hudFNT = cfonts[2]
        # self.ui.hudt.appendPlainText(f"incorrect font setting {self.hudFNT} using default Consolas")
        f_f = f"{self.hudFNT}, Courier, monospace"
        color = ""
        if self.hudCOLOR == "unix":
            color = "00FF00"
        elif self.hudCOLOR == "wb":
            color = "FFFFFF"
        elif self.hudCOLOR == "solar":
            color = "2AA198"
        elif self.hudCOLOR == "monochrome":
            color = "C0C0C0"
        self.set_stylesht(f_f, color)
    # end Custom settings for hudt

    def init_timers(self):
        self.timeout_timer = QTimer(self)
        self.timeout_timer.setSingleShot(True)
        self.timeout_timer.timeout.connect(self.thread_timeout)
        #
        self.proctimeout_timer = QTimer(self)
        self.proctimeout_timer.setSingleShot(True)
        self.proctimeout_timer.timeout.connect(self.handle_proctimeout)
        #
        self.worker_timeout_sn.connect(self.timeout_timer.stop)
        self.proc_timeout_sn.connect(self.proctimeout_timer.stop)
        #
        self._status_reset_timer = QTimer(self)
        self._status_reset_timer.setSingleShot(True)
        self._status_reset_timer.timeout.connect(
            lambda: self.ui.dbmainlabel.setText(
                "Status: Connected" if self.isloaded() else "Status: offline"
            )
        )
        self.database_reload_timer = QTimer(self)
        self.database_reload_timer.setSingleShot(True)

    def keyPressEvent(self, event):
        if event.key() == Qt.Key.Key_C and event.modifiers() & Qt.KeyboardModifier.ControlModifier:
            QApplication.quit()
        super().keyPressEvent(event)

    def x_action(self):
        sender = self.sender()
        if self.isexec:
            self.cleanup()
            if hasattr(self, 'ui') and hasattr(self.ui, 'hudt'):
                self.ui.hudt.appendPlainText("Stopping current jobs or process")
        else:
            if sender != self.ui.actionStop:
                self.reset_settings()

    def is_thread(self):
        try:
            thread = getattr(self, 'worker_thread', None)
            if thread and thread.isRunning():
                self.ui.hudt.appendPlainText("Thread did not stop cleanly")
        except (RuntimeError, AttributeError) as e:
            logging.error("is_thread on closeEvent failed with the following exception: %s err: %s", e, type(e).__name__)

    def cleanup(self):
        if getattr(self, "is_user_abort", False):
            return
        self.is_user_abort = True

        for t_name in ['proctimeout_timer', 'timeout_timer']:
            t = getattr(self, t_name, None)
            if t and t.isActive():
                t.stop()

        if getattr(self, 'worker', None) is not None:
            self.stop_worker_sn.emit()

        proc = getattr(self, "proc", None)
        if proc and proc.is_running():
            self.stop_proc_sn.emit()

        thread = getattr(self, 'worker_thread', None)
        if thread and isinstance(thread, QThread) and thread.isRunning():
            thread.quit()
            QTimer.singleShot(3000, lambda: self.is_thread())

    # on exit close threads processes and savenotes
    def closeEvent(self, event):
        if self.dirtybit:
            self.dirtybit = False
            uinpt = wdisplay_prompt(self, "Saved changes", "Save changes?", "Yes", "No")
            if uinpt:
                self.save_user_data(isexit=True)

        if getattr(self, 'isexec', False):
            self.cleanup()
            thread = getattr(self, 'worker_thread', None)
            try:
                if thread and thread.isRunning():
                    thread.wait(3000)
            except (RuntimeError, AttributeError):
                pass
            proc = getattr(self, 'proc', None)
            if proc:
                try:
                    if hasattr(proc, 'process') and proc.process:
                        proc.process.waitForFinished(10000)
                except (RuntimeError, AttributeError):
                    pass
        event.accept()

    # overview of configuration also debug generalized
    def show_status(self):

        ps = False  # check if profile made
        # if self.proteusSHIELD:
        try:
            with DBMexec(self.dbopt, "sq_1", ui_logger=self.ui.hudt) as dmn:  # dmn.table_has_data(table_nm):
                if dmn.table_has_data(self.sys_a):
                    ps = True
        except DBConnectionError as e:
            logging.error("Error connecting to %s while query checking proteus shield %s fail: %s", self.dbopt, self.sys_a, e)
        except Exception as e:
            logging.error("query checking proteus shield in show_status fail: %s %s \n", self.sys_a, type(e).__name__)

        stat_value = {}

        stat_value['Exhibit'] = "Pwrshell" if not self.wsl else "WSL"

        drive_model = get_j_settings(["drive_model"], drive=self.basedir, filepath=self.sj).get("drive_model")
        if not drive_model:
            dmodel, _ = current_drive_type_model_check(self.basedir)
            if dmodel:
                set_j_settings({"drive_model": dmodel}, drive=self.basedir, filepath=self.sj)
            drive_model = dmodel if dmodel is not None else "Unknown"

        typeModel = f"{drive_model} / {self.driveTYPE}"

        stat_value.update({
            "Drive or basedir:": self.basedir,
            "Type/model": typeModel,
            "Empty1":  "",
            "Proteus Shield active": str(ps),
            "Drive Caching": "y" if self.cacheidx else "n",
            "Checksum and Caching": "y" if self.checksum else "n",
            "Empty2":  "",
            "Database": self.dbopt,
            "Last table": self.table,
            "Empty3":  "",
            "Empty4": "",
            "Debug line1": f"self.db is {self.db}",  # debuger
            "Debug line2": f'worker is {"active" if self.worker else ""}'
        })
        hudt = self.ui.hudt.appendPlainText
        self.ui.hudt.clear()
        for key, value in stat_value.items():
            if not key.startswith("Debug") and not value:
                hudt('')
            elif key == "Exhibit":
                hudt(value)
            else:
                hudt(f"{key} {value}")
        hudt('\n')

        if self.result is not None:
            hudt(f"Last Return: {self.result}")
            if self.resStatus != -1:
                hudt("QProcess")
                hudt(f"QExitStatus: {self.resStatus}")
            else:
                hudt("Thread")
        # self.ui.hudt.appendPlainText(f"value: {self.sys_a}")

    def save_user_data(self, isexit=False):
        last_drive = self.ui.combd.currentText()

        notes = self.ui.textEdit.toPlainText()
        nc = intst(self.dbopt, self.compLVL)
        user_data_to_database(notes, self.ui.hudt, self.dbopt, self.dbtarget, self.email, nc, isexit=isexit, parent=self)
        set_j_settings({"last_drive": last_drive}, filepath=self.sj)
        self.last_drive = last_drive

        self.dirtybit = False

    def load_user_data(self):
        self.ui.textEdit.blockSignals(True)
        self.is_ps = user_data_from_database(self.ui.hudt, self.ui.textEdit, self.ui.combffile, self.extensions, self.dbopt)
        self.ui.textEdit.blockSignals(False)

    def open_resource(self):
        # self.doc_window = open_html_resource(self, self.lclhome)
        open_html_resource(self, self.lclhome)

    def open_file_dialog(self, start_dir=""):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select a File",
            str(start_dir),
            "All Files (*);;Image Files (*.png *.jpg *.jpeg)"
        )
        return file_path

    # Take care of setting this so not to prompt repeatedly
    def findwsl(self):
        dm = "switching to powershell"
        # if hasattr(self, 'timer'):
        #     self.timer.stop()
        is_subsystem = False
        if self.wsl:
            if is_wsl():
                default = get_default_distro()
                res = get_version1()
                if default and not res:
                    reply = QMessageBox.question(
                        self,
                        "Confirm Action",
                        "WSL installed. it is required to change to WSL1 continue? Otherwise powershell will be used",
                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                    )
                    if reply == QMessageBox.StandardButton.Yes:
                        if set_to_wsl1(default):
                            is_subsystem = True
                        else:
                            wdisplay_message(self, f"Unable to set wsl1. {dm}")
                elif not default and not res:
                    wdisplay_message(self, f"Unable to get default distro for wsl.. {dm}")
                else:
                    is_subsystem = True
            else:
                wdisplay_message(self, "WSL not installed defaulting to off")

            if not is_subsystem:
                update_toml_setting('search', 'wsl', False, self.toml_file)

        return is_subsystem

    def reset_settings(self, retry=0, max_ret=5):
        if self.isexec:
            if retry < max_ret:
                QTimer.singleShot(100, lambda: self.reset_settings(retry + 1, max_ret))
            return
        self.ui.progressBAR.setValue(0)
        self.ui.dbprogressBAR.setValue(0)

        self.ui.combftimeout.setCurrentIndex(0)  # output
        self.ui.stime.setValue(self.ui.stime.minimum())  # rng
        self.ui.combt.setCurrentIndex(0)  # ntfilter

        self.ui.ntlineEDIT.clear()
        self.ui.ffilet.clear()
        self.ui.hudt.clear()

        self.ui.diffchka.setChecked(False)
        self.ui.diffchkb.setChecked(False)
        self.ui.diffchkc.setChecked(False)

        self.ui.dbchka.setChecked(False)

        self.mft = None

        self.ui.combffile.setCurrentIndex(0)

        # self.ui.combd.setCurrentIndex(0)
        last_drive = self.ui.combd.currentText()
        if last_drive != self.last_drive:
            idx = self.ui.combd.findText(self.last_drive)
            if idx != -1:
                self.ui.combd.setCurrentIndex(idx)

        self.ui.mftrange.setValue(24)

        self.ui.mftchka.setChecked(False)
        self.ui.mftchkb.setDisabled(False)
        self.ui.mftchkb.setChecked(False)

        self.resetMftImport()

    '''  jpg / crest '''

    def refresh_jpg(self):
        pixmap = QPixmap(self.picture)
        if not pixmap.isNull():
            scaled = pixmap.scaled(533, 300, Qt.AspectRatioMode.IgnoreAspectRatio, Qt.TransformationMode.SmoothTransformation)

            self.ui.jpgv.setPixmap(scaled)
        else:
            self.ui.hudt.appendPlainText(f"Failed to load image: {self.picture}")

    def refresh_crest(self):
        pixmap = QPixmap(self.crest)
        if pixmap.isNull():
            self.ui.hudt.appendPlainText(f"Failed to load crest: {self.crest}")
            return
        self.ui.jpgcr.setPixmap(pixmap)  # Set the pixmap on the label
        self.ui.jpgcr.setScaledContents(True)

    def emboss(self):

        if self.lastdir == self.crestdir:
            self.lastdir = self.lclhome
        crest = self.open_file_dialog(self.lastdir)
        if crest:

            self.lastdir = Path(os.path.dirname(crest))
            if self.valid_crest(crest):
                flnm_frm, ext = os.path.splitext(crest)
                outpath = flnm_frm + "_raised" + ext
                i = 1
                while os.path.exists(outpath):
                    outpath = f"{flnm_frm}_raised_{i}{ext}"
                    i += 1

                raised_image(crest, outpath)

    def valid_crest(self, crest_path):
        img = QImage(crest_path)

        if img.isNull():
            QMessageBox.warning(self, "Error", "Invalid image file.")
            return False

        w = img.width()
        h = img.height()

        if w > 255 or h > 333:
            wdisplay_message(self, f"Image size must be 250x333 or less.\n\nSelected image: {w}x{h}", "Invalid size")
            return False
        return True

    def load_jpg(self):

        def reset_default(default_path, default_file, target):
            defaultflnm = os.path.join(default_path, default_file)
            shutil.copy(defaultflnm, target)
        # copy .bak to .png

        def selectcustom(title, msg, importjpg, defaultjpg, importcrest, defaultcrest):

            dlg = QDialog(self)
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

        res = selectcustom(
            "Choose an Option",
            "Please select an option:",
            "Jpg",
            "Reset",
            "Crest",
            "Reset"
        )

        if res == "jpg":
            jpg = self.open_file_dialog(self.lastdir)
            if jpg:
                self.lastdir = Path(os.path.dirname(jpg))
                if os.path.abspath(jpg) != os.path.abspath(self.picture):
                    image = QImage(jpg)
                    if image.isNull():
                        QMessageBox.warning(self, "Invalid Image", f"Cannot open the selected file:\n{jpg}")
                        return
                    image.save(str(self.picture), "PNG")
                    self.refresh_jpg()
        elif res == "defjpg":
            self.lastdir = None
            reset_default(self.jpgdir, self.jpgdefault, self.picture)
            self.refresh_jpg()
        elif res == "crest":

            crest = self.open_file_dialog(self.crestdir)  # crest dir always
            if crest:

                file_root = Path(os.path.dirname(crest))
                if file_root != self.crestdir:
                    self.lastdir = file_root

                if os.path.abspath(crest) != os.path.abspath(self.crest):  # dont reemboss the same crest in use
                    if self.valid_crest(crest):
                        shutil.copy(crest, self.crest)
                        self.refresh_crest()

        elif res == "defcrest":
            self.lastdir = None
            reset_default(self.crestdir, self.crestdefault, self.crest)
            self.refresh_crest()

        elif res == "emboss":
            self.emboss()

    ''' Combo boxes '''

    # download button combo box
    # return drive cache glob and default name
    def get_cache_pattern(self):
        systimename = getnm(self.oldCACHE_S)
        systime_pattern = systimename + "*"  # + "_*"

        pattern = os.path.join(self.lclhome, systime_pattern)
        return pattern, systimename

    # values
    def load_saved_indexes(self):
        pattern, systimename = self.get_cache_pattern()

        filepath = glob.glob(pattern)

        dindex = []
        current_drive = self.basedir
        dindex.append(current_drive)

        for path in filepath:
            fname = os.path.basename(path)  # the full file name

            is_drive_c, _ = os.path.splitext(fname)  # case where basedir is not C:\\
            if is_drive_c == systimename:
                if current_drive != "C:\\":
                    dindex.append("C:\\")
                continue

            part_frm = fname.split(systimename + "_", 1)[-1]  # anything after _
            drive_name = os.path.splitext(part_frm)[0]
            drive_name = drive_name.upper() + ":\\"

            if drive_name != current_drive:  # our basedir was already added first
                dindex.append(drive_name)
        return dindex

    def fill_download_combo(self, drives):  # fill download combo box values
        combo = self.ui.combd
        combo.clear()
        combo.addItems(drives)
        combo.setCurrentIndex(0)

    # end download button combo box

    def clear_extensions(self):
        if not self.validata(True):
            return
        self.nc = intst(self.dbopt, self.compLVL)
        if clear_extn_tbl(self.dbopt, False):
            if encr(self.dbopt, self.dbtarget, self.email, self.nc, False):
                fill_extensions(self.ui.combffile, self.extensions)
        self.isexec = False

    def new_extension(self):
        if not self.validata(True):
            return
        add_extension(self.extensions, self.ui.hudt, self.ui.combffile, self.dbopt, self.dbtarget, self.email, self.nc, parent=self)
        self.isexec = False

    def isloaded(self):
        mdl = self.ui.tableView.model()
        if isinstance(mdl, QSortFilterProxyModel):
            mdl = mdl.sourceModel()
        return bool(self.db and mdl and mdl.rowCount())

    def init_page2(self):
        if self.isloaded():  # self.db (first load) and has model rows
            return True
        else:

            if not os.path.isfile(self.dbopt):
                if not load_gpg(self.dbopt, self.dbtarget, self.ui.dbmainlabel):
                    return False
            return self.displaydb('logs', False, False)

    def show_page_2(self):

        if not self.init_page2():
            self.ui.dbmainlabel.setText("Status: offline")

        self.ui.stackedWidget.setCurrentWidget(self.ui.page_2)

    def show_page(self):
        self.ui.stackedWidget.setCurrentWidget(self.ui.page)

    ''' QProcess '''  # Thread ln2089

    # Process
    #  for search,tsearch,nt,ffile,sys idx, sys scan, find downloads

    def cleanup_proc(self):
        if self.proc:
            self.proc.deleteLater()
            self.proc = None

    def openp(self, timeout=90000):
        self.ui.progressBAR.setValue(0)
        self.result = None
        self.resStatus = None

        self.proctimeout_timer.start(timeout)

        self.proc.progress.connect(self.increment_progress)
        self.proc.log.connect(self.append_log)  # self.append_colored_output)
        self.proc.error.connect(lambda e: self.append_log("Error: " + e))
        self.stop_proc_sn.connect(self.proc.stop)  # lambda: self.proc.stop()
        self.proc.complete.connect(self.finalp)
        self.proc.complete.connect(self.cleanup_proc)
        self.isexec = True

    def p_initdbui(self):
        self.ui.dbprogressBAR.setValue(0)
        self.proc.progress.connect(self.increment_db_progress)
        self.proc.status.connect(self.update_db_status)

    @Slot(int, int)
    def finalp(self, exit_code, exit_status):
        if exit_code != 4:
            if self.proctimeout_timer.isActive():
                self.proc_timeout_sn.emit()

        self.isexec = False
        self.is_user_abort = False

        self.result = exit_code
        self.resStatus = exit_status

        if exit_code != 0:  # and not exit_status != QProcess.NormalExit:
            exit_str = str(exit_status)
            if exit_code == 7:
                self.ui.hudt.appendPlainText(f"QProcess replied to exit request Exit status: {exit_str}")
            else:
                self.ui.hudt.appendPlainText(f"Exit code: {exit_code}, Exit status: {exit_str}")
        self.ui.resetButton.setEnabled(True)

    def handle_proctimeout(self):
        if getattr(self, "proc", None) and self.proc.is_running():

            self.ui.hudt.appendPlainText("Requesting process stop due to timeout...")
            self.stop_proc_sn.emit()

    #
    # End Process

    ''' Main search recentchangessearch.py'''

    # top search
    def search(self, output, THETIME, argf):
        if not self.validata():
            return
        method = ""
        SRCDIR = "noarguser"

        if output == "AppData":
            argone = THETIME
            THETIME = SRCDIR
            SRCDIR = "noarguser"
            method = "rnt"
        else:
            argone = "search"

        scanidx = self.ui.diffchkb.isChecked()
        postop = self.ui.diffchka.checkState() == Qt.CheckState.Checked
        showDiff = self.ui.diffchkc.isChecked()

        if postop:
            doctrine = os.path.join(self.USRDIR, "doctrine.tsv")
            if os.path.exists(doctrine):
                self.ui.hudt.appendPlainText("A file doctrine already exists skipping")

        self.proc = ProcessHandler()
        self.openp(180000)
        ismcore = True
        self.proc.set_mcore(ismcore)  # uses multicore dont cancel while those processes are running   between 21 - 59 % and 66 and 89%
        if postop or scanidx:
            self.proc.complete.connect(lambda code, _: self.reload_ui_elesn.emit(code, "search"))

        s_path = os.path.join(self.lclhome, "recentchangessearch.py")  # "src",

        args = [
            str(argone),
            str(THETIME),
            str(self.usr),
            str(self.PWD),
            str(argf),
            str(method),
            "True",
            str(self.dbopt),
            str(postop),
            str(scanidx),
            str(showDiff),
            str(self.wsl),
            str(self.dspPATH)
        ]
        is_search = True
        if self.wsl:
            is_search = False
        self.proc.start_pyprocess(s_path, args, is_search=is_search, is_postop=postop, is_scanIDX=scanidx)  # self.myapp

    # fork
    # 5 Min, 5 Min Filtered, Search by time and . Filtered
    def tsearch(self, clicked_button, filtered=None):
        output = ""
        argf = ""

        if filtered:
            output = "Desktop"
            argf = "filtered"

        if clicked_button == self.ui.stimebf or clicked_button == self.ui.stimeb:
            THETIME = self.ui.stime.value()
            if THETIME == 0:
                self.ui.hudt.appendPlainText("Time cant be 0.")
                return
        else:
            THETIME = "noarguser"

        if clicked_button == self.ui.stimeb or clicked_button == self.ui.ftimeb:
            output = self.ui.combftimeout.currentText()  # AppData or Desktop

        self.search(output, THETIME, argf)

    # fork
    def ntsearch(self):
        clicked_button = self.sender()
        if clicked_button == self.ui.ntbrowseb:
            fpath = self.open_file_dialog()  # Add folders button***
            #
            if fpath:
                self.ui.ntlineEDIT.setText(fpath)
            return
        elif clicked_button == self.ui.ntbrowseb2:
            fpath = QFileDialog.getExistingDirectory(self, "Select a folder")
            if fpath:
                self.ui.ntlineEDIT.setText(fpath)
            return

        fpath = self.ui.ntlineEDIT.text().strip()
        if not fpath:
            wdisplay_message(self, "Browse to select a file.", "No target")
            return
        elif not os.path.exists(fpath):
            wdisplay_message(self, "please enter valid filename.", "NSF")
            return

        output = "search"
        THETIME = fpath

        argf = self.ui.combt.currentText()
        if argf == "Filtered":
            argf = ""
        else:
            argf = "filtered"

        self.search(output, THETIME, argf)

    # Find file
    def ffile(self, compress, time_range=None):
        if not self.validata():
            return
        extension = self.ui.combffile.currentText()
        if extension:
            if not extension.startswith("."):
                self.isexec = False
                self.ui.hudt.appendPlainText(f"invalid extension {extension}")
                return
        fpath = self.ui.ffilet.text().strip()
        if not (fpath or extension):
            self.isexec = False
            wdisplay_message(self, "please enter a filename and or extension")
            return

        self.proc = ProcessHandler()
        self.openp(120000)

        if compress:
            downloads = self.ui.combffileout.currentText()
            if downloads == "Desktop":
                downloads = self.USRDIR
            elif downloads == "Downloads":
                downloads = self.downloads.strip()
            else:
                downloads = self.lclhome

            self.proc.set_compress(time_range, self.zipPROGRAM, self.zipPATH, self.USRDIR, downloads)  # compress button?

        action = "pwsh"
        if self.wsl:
            action = "wsl"

        cmd = os.path.join(self.lclhome, "ffsearch.py")  # "src",

        self.proc.start_pyprocess(cmd, [action, fpath, extension, self.basedir, self.usr, self.dspEDITOR, self.dspPATH, self.tempdir])

    # compress
    def ffcompress(self):
        zip_pth = None
        zipPROGRAM = self.zipPROGRAM
        if not zipPROGRAM:
            self.ui.hudt.appendPlainText("No zipPROGRAM specified")
            return

        if not self.zipPATH:
            if zipPROGRAM == "winrar":
                zip_pth = check_installed_app("WinRAR.exe", "WinRAR")
            elif zipPROGRAM == "7zip":
                zip_pth = check_installed_app("7zFM.exe", "7-Zip")
                if not zip_pth:
                    zip_pth = self.defaultzipPATH
                else:
                    seven_path_frm = os.path.dirname(zip_pth)
                    seven_path = os.path.join(seven_path_frm, "7z.exe")
                    if os.path.isfile(seven_path):
                        zip_pth = seven_path
                    else:
                        self.ui.hudt.appendPlainText(f"Failed to find 7z command-line 7z.exe beside 7zFM.exe path: {seven_path_frm}")
                        self.ui.hudt.appendPlainText("Defaulting to app 7-zip")
                        zip_pth = self.defaultzipPATH
            if zip_pth:
                self.zipPATH = zip_pth
            else:
                wdisplay_message(self, "No zipPATH specified and failed to find a path for 7zip or winrar on system", "Info")
                return

        time_range, ok = wdisplay_get_input(self, "Enter search time", "Seconds:")
        if ok and time_range:
            try:

                range_float = convertn(int(time_range), 60, 2)
                if range_float == 0:
                    uinpt = wdisplay_prompt(self, "Compress archive", "You have entered 0. This will compress all file matches. Continue", "Yes", "No")
                    if not uinpt:
                        return
                self.ffile(True, range_float)
            except ValueError:
                self.ui.hudt.appendPlainText("Invalid number")
                return
        elif ok:
            self.ui.hudt.appendPlainText("specify 0 to compress all results. or enter a time range to compress")
    #
    # End Find file

    ''' Main Mft '''

    # Mft

    # Mft helpers
    def clearmfts(self):

        pattern = os.path.join(self.USRDIR, self.mftflnm + "*")
        filepath = glob.glob(pattern)
        for path in filepath:
            try:
                os.remove(path)
            except Exception as e:
                self.ui.hudt.appendPlainText(f"Error removing {path} while clearing Mfts : {e}")

    def validaction(self):
        method = "mftdump"
        tool = None

        if os.path.isfile(self.mftec_command):   # '.\\bin\\MFTECmd.exe'
            method = "mftec"
            c_ver = mftec_version(str(self.mftec_command), self.tempdir)
            if c_ver == "mftec_cutoff":
                method = "mftec_cutoff"
            elif c_ver:
                method = "mftec"

        if method == "mftdump":

            if os.path.isfile(self.icat_command):  # '.\\bin\\icat.exe'

                if os.path.isfile(self.fsstat_command):  # '.\\bin\\fsstat.exe'
                    tool = "icat_fsstat"
                else:
                    wdisplay_message(self, "1.fsstat.exe", f"Cant find executable in {self.exe_path}")
            elif os.path.isfile(self.ntfs_command):  # '.\\bin\\ntfstool.x86.exe'
                tool = "ntfstool"
            else:
                wdisplay_message(self, "1.icat/fstat or 2.ntfstools", f"Cant find any executable in {self.exe_path}")
            if tool is None:
                return None, None
        return method, tool

    def prescreen(self, tool, mmin, method, output_f, csvnm, flnm, OLDSORT, flnmout, flnmdffout, drive, USRDIR):

        if tool == "icat_fsstat":

            if validmft(str(self.fsstat_command)):  # First check for $MFT
                self.start_mfttrd("Mfticat_fstat", mmin, method, output_f, csvnm, flnm, OLDSORT, flnmout, flnmdffout, drive, USRDIR)
                return True
        # ntfstool
        else:
            drive_info = get_disk_and_volume_for_drive("C:")
            if drive_info:
                disk, _ = drive_info
                tgt = f"disk={disk}"

                mounted = get_mounted_partitions('C:\\', tgt, str(self.ntfs_command))

                if mounted:
                    vol = mounted['id']
                    volume = vol
                    if disk and vol:  # First check for $MFT - # mft found
                        self.start_mfttrd("Mftntfs", mmin, method, output_f, csvnm, flnm, OLDSORT, flnmout, flnmdffout, drive, USRDIR, disk, volume)
                        return True
                        # self.worker.set_task(opt, disk, volume) #pass ins
                    else:
                        self.ui.hudt.appendPlainText("failed to locate mft for C:\\ before running ntfs tools")
                else:
                    self.ui.hudt.appendPlainText("failed volno or volume number for C:\\")
            else:
                self.ui.hudt.appendPlainText("failed diskno unable to find disk number for C:\\")
        return False

    def get_ramdrive(self, basedir, cmsg):
        if not isinstance(cmsg, list):
            self.ui.hudt.appendPlainText(f"error get_ramdrive cmsg {cmsg} is not a list")
            return None
        dialog = DriveSelectorDialog(self.basedir, self)
        if dialog.exec():
            target = dialog.selected_drive()
            if os.path.exists(target):
                self.ramdisk = target
                return target
            else:
                self.ramdisk = None  # no default for index drives applies to Mft
                uinpt = wdisplay_prompt(self, *cmsg)
                #
                if uinpt:
                    return basedir
        return None

    def get_newdrive(self):
        dialog = DriveSelectorDialog(self.basedir, self)
        if dialog.exec():
            target = dialog.selected_drive()
            if os.path.exists(target):
                return target
            else:
                wdisplay_message(self, f"selected {target} not found.")
        return None

    def resetMftImport(self):

        self.ui.mftchkb.setDisabled(False)
        self.ui.mftrange.setDisabled(False)
        self.ui.mftmainlabel.setStyleSheet("")
        self.ui.mftmainlabel.setText("System Mft")
        self.mft = None
    # end Mft helpers

    # Main mft search
    def mftsearch(self):
        if not self.validata(False):
            self.isexec = False
            return
        method, tool = self.validaction()
        if (method, tool) == (None, None):
            self.isexec = False
            return
        elif method == "mftdump" and self.mft:
            self.isexec = False
            self.ui.hudt.appendPlainText("MFTECmd is required to convert a raw mft to csv")
            return

        mmin = self.ui.mftrange.value()  # spinbox
        if not mmin or mmin == 0:
            self.isexec = False
            self.ui.hudt.appendPlainText("Mft range cant be 0.")
            return

        drive = self.lclhome  # default
        if self.ui.mftchka.isChecked():
            if not self.ramdisk:
                drive = self.get_ramdrive(drive, ["Unable to locate ramdisk", "continue?", "Yes", "No"])
                if not drive:
                    self.isexec = False
                    return
            else:
                if not os.path.exists(self.ramdisk):
                    self.ramdisk = None
                    wdisplay_message(self, "ramdisk not found", "Error")
                    self.isexec = False
                    return
                drive = self.ramdisk

        mft = None
        if self.mft:
            if os.path.isfile(self.mft):
                mft = self.mft  # import
            else:
                self.mft = None
                self.resetMftImport()
                wdisplay_message(self, f"mft {self.mft} not found.")
                self.isexec = False
                return

        csvnm = "mft.csv"
        mftraw = "Mft.raw"

        output_f = os.path.join(drive, mftraw)

        flnm = f'{self.mftflnm}{mmin}.txt'
        flnmout = os.path.join(self.USRDIR, flnm)
        flnmdff = f'{self.mftflnm}DiffFromLastSearch{mmin}.txt'
        flnmdffout = os.path.join(self.USRDIR, flnmdff)

        OLDSORT = None  # find prev search
        testp = os.path.join(self.USRDIR, flnm)
        if os.path.isfile(testp):
            OLDSORT = os.path.join(drive, flnm)
            try:
                shutil.copy(testp, OLDSORT)  # copy it to AppData
                self.clearmfts()
            except Exception:
                OLDSORT = None
                self.ui.hudt.appendPlainText(f'failed to copy old results: {testp}')

        isMftSave = self.ui.mftchkb.isChecked()

        if isMftSave:  # Save the system Mft
            if not self.prescreen(tool, mmin, method, output_f, csvnm, flnm, OLDSORT, flnmout, flnmdffout, drive, self.USRDIR):  # check for NTFS valid $MFT
                self.isexec = False
                return  # optional pass ins
            self.worker.set_task(self.mftec_command, self.icat_command, self.fsstat_command, self.ntfs_command)

            self.worker_thread.started.connect(self.worker.save_mft)
            self.open_trd(150000)

        elif mft is None:  # Default make the search
            self.ui.progressBAR.setValue(5)

            if not self.prescreen(tool, mmin, method, output_f, csvnm, flnm, OLDSORT, flnmout, flnmdffout, drive, self.USRDIR):  # .
                self.isexec = False
                return
            self.worker.set_task(self.mftec_command, self.icat_command, self.fsstat_command, self.ntfs_command)

            self.worker_thread.started.connect(self.worker.run)
            self.open_trd(150000)

        else:  # From imported Mft to csv

            self.ui.progressBAR.setValue(15)

            newf = getnm(mft)
            optf = f'{newf}.csv'

            self.start_mfttrd(
                "Mftimport", mmin, method, optf, csvnm,
                flnm, OLDSORT, flnmout, flnmdffout,
                drive, self.USRDIR, None, None, mft
            )
            self.worker.set_task(self.mftec_command, self.icat_command, self.fsstat_command, self.ntfs_command)
            self.worker_thread.started.connect(self.worker.outputmft)
            self.worker.complete.connect(lambda code: self.reload_ui_elesn.emit(code, "Import"))
            self.open_trd()
        # end Main mft search

    # Mft raw file
    def imprt_mft_brws(self):
        # clicked_button = self.sender()
        # if clicked_button == self.ui.mftbrowseb:
        mft = self.open_file_dialog()
        if mft:
            self.mft = mft
            self.ui.mftchkb.setDisabled(True)
            self.ui.mftrange.setDisabled(True)
            self.ui.mftmainlabel.setStyleSheet("color: red;")
            self.ui.mftmainlabel.setText("Imported Mft")
        return
    #
    # end Mft raw file
    #
    # end Mft

    ''' Proteus Shield / System Profile '''

    # Main db task
    #
    # also contains Index drive aka Find Downloads
    # System profile / Proteus Shield
    # page_2

    # Build IDX &
    # Drive index
    #
    # Moved here as database integrated. central hub for core feature system index.   QProcess start logic above. Thread start logic below

    # Main Scan IDX
    def scan_idx(self):
        if not self.validata():
            return

        if not dbtable_has_data(self.dbopt, self.sys_a):
            self.isexec = False
            return  # check if a sys profile exists

        basedir = self.basedir
        email = self.email
        diff_file = get_diffFile(self.lclhome, self.USRDIR, self.MODULENAME)

        showDiff = self.ui.dbchka.isChecked()

        self.ui.hudt.append_colored_output("\033[1;32mSystem index scan..\033[0m")

        self.proc = ProcessHandler()
        self.openp(300000)
        self.p_initdbui()
        ismcore = True
        self.proc.set_mcore(ismcore)  # os.scandir workers cant be stopped flag. leave process open until complete
        self.proc.status.connect(self.update_db_status)

        cmd = os.path.join(self.lclhome, "dirwalker.py")  # , "src"

        self.proc.start_pyprocess(cmd, ['scan', self.dbopt, self.dbtarget, basedir, diff_file, str(self.updatehlinks), self.CACHE_S, email, str(self.ANALYTICSECT), str(showDiff), str(self.cacheidx), str(self.compLVL), 'False', 'True'], dbopt=self.dbopt, status_message="Index scan")

    # Main Build IDX
    def run_build_idx(self, basedir, CACHE_S, stsmsg, tables, idx_drive=False, drive_value=None):

        self.proc = ProcessHandler()
        self.openp(300000)

        ismcore = True
        drive_idx = None

        if idx_drive:
            drive_idx = drive_value
            self.proc.complete.connect(lambda code, _, d_value=drive_value: self.reload_drives(code, None, d_value))

        else:  # connect pg_2
            self.p_initdbui()  # label/pbar
            self.proc.status.connect(self.update_db_status)  # reset label

        self.proc.complete.connect(lambda code, _, table_tuple=tables: self.reload_database_elesn.emit(code, False, table_tuple))
        self.proc.complete.connect(lambda code, _: self.reload_sj_sn.emit(code, drive_idx, 'add'))
        self.proc.set_mcore(ismcore)  # cant stop until some point

        cmd = os.path.join(self.lclhome, "dirwalker.py")  # "src",

        self.proc.start_pyprocess(cmd, ['build', self.dbopt, self.dbtarget, basedir, str(self.updatehlinks), CACHE_S, self.email, str(self.ANALYTICSECT), str(idx_drive), str(self.cacheidx), str(self.compLVL), 'True'], dbopt=self.dbopt, status_message=stsmsg)
        # cmd = os.path.join(self.lclhome, "sysprofile.py")  find command script
        # self.proc = ProcessHandler()
        # self.openp(180000)
        # self.proc.start_pyprocess(cmd, [ self.dbopt, self.dbtarget, self.email, str(self.wsl) ])

    # fork build button pg2
    def build_idx(self):
        if not self.validata():
            self.isexec = False
            return

        if not has_sys_data(self.dbopt, self.ui.hudt, self.sys_a, "Previous sys profile has to be cleared. Continue?", parent=self):  # prompt to delete
            self.isexec = False
            return

        tables = (self.sys_a, self.sys_b, self.cache_table, self.systimeche)
        self.ui.hudt.appendPlainText("Hashing system profile...")
        self.run_build_idx(self.basedir, self.CACHE_S, "System profile", tables)

    # fork       pg1
    # add index button page 1 '''
    def idx_drive(self):
        if self.isexec:
            wdisplay_message(self, "there is a current job started.", "Execution")
            return
        drive = self.get_newdrive()
        if not drive:
            return
        if drive == self.basedir:
            self.ui.hudt.appendPlainText(f"{drive} sys basedir Requires build idx on db page")
            return
        if not self.cacheidx:
            self.ui.hudt.appendPlainText("proteus_CACHE setting disabled unable to index")
            return

        sys_tables, cache_table = get_idx_tables(drive)
        CACHE_S, systimeche = get_cache_s(drive, self.oldCACHE_S)

        tables = (*sys_tables, cache_table, systimeche)  # for updating ui elements

        if dbtable_has_data(self.dbopt, sys_tables[0]):
            self.ui.hudt.appendPlainText(f"Drive {drive} has sys profile. switch basedir in config.toml and then clear IDX and rebuild on page2")
            return

        drive_type = setup_drive_settings(drive, None, self.sj, idx_drive=True)
        if drive_type is None:
            if os.path.isdir(drive):
                set_j_settings({"drive_type": "HDD", "drive_model": "Unknown"}, drive=drive, filepath=self.sj)
                self.ui.hudt.appendPlainText(f"Drive defaulting to HDD to change it go to {self.sj}")
            else:
                self.ui.hudt.appendPlainText(f"Unable to locate drive. quitting {drive}")
                return

        self.isexec = True
        self.ui.hudt.appendPlainText(f"Indexing {drive}")
        self.run_build_idx(drive, CACHE_S, f"Drive {drive} profile", tables, True, drive)

    # remove index button page 1 '''
    def rmv_idx_drive(self):
        drive = self.ui.combd.currentText()
        if drive == self.basedir:
            return
        idx = self.ui.combd.currentIndex()
        sys_tables, cache_table = get_idx_tables(drive)

        CACHE_S, systimeche_table = get_cache_s(drive, self.oldCACHE_S)

        # remove the drive cache .gpg file
        # call a thread as the database delete can freeze ui
        self.clear_sys(drive, CACHE_S, sys_tables, cache_table, systimeche_table, idx)

    # downloads button page 1
    # find downloads or files with the directory cache. os.scandir recursion
    def find_downloads(self, basedir="C:\\"):
        if not self.validata():
            return

        if not self.cacheidx:
            self.isexec = False
            self.ui.hudt.appendPlainText(f"setting proteus_CACHE is not enabled enable setting to continue. If its {self.basedir} use build IDX \nif its a regular drive remove and add again to build index")
            return

        idx_basedir = False

        CACHE_S = self.CACHE_S
        systimeche = self.systimeche

        idx_drive = self.ui.combd.currentText()  # index selected
        if idx_drive != basedir:
            idx_basedir = True
            CACHE_S, systimeche = get_cache_s(idx_drive, self.oldCACHE_S)
            basedir = idx_drive

        if not dbtable_has_data(self.dbopt, systimeche):  # indexed?
            self.isexec = False
            msg = f"{basedir} not indexed."
            if not idx_basedir:
                self.ui.hudt.appendPlainText(f"{msg} requires Build idx")
            else:
                self.ui.hudt.appendPlainText(msg)
            return

        if not os.path.isfile(CACHE_S):  # missing cache?
            self.ui.hudt.appendPlainText(f"Error cache not found. {'re index drive' if idx_basedir else 'requires rebuild IDX'}, file not found: {CACHE_S}")
            self.isexec = False
            if idx_basedir:  # dont remove C:\ or basedir
                ix = self.ui.combd.findText(idx_drive)
                if ix != -1:
                    self.ui.combd.removeItem(ix)
                    set_j_settings({"last_drive": self.basedir}, filepath=self.sj)
                    if self.ui.combd.count() > 0:
                        self.ui.combd.setCurrentIndex(0)
            return

        drive_type = get_j_settings(["drive_type"], drive=basedir, filepath=self.sj).get("drive_type")
        if not drive_type:
            drive_type = "HDD"

        cmd = os.path.join(self.lclhome, "dirwalker.py")  # "src",
        self.proc = ProcessHandler()
        self.openp(120000)

        # disable stop button ****

        self.proc.start_pyprocess(cmd, ['downloads', self.dbopt, self.dbtarget, basedir, drive_type, self.tempdir, CACHE_S, self.dspEDITOR, self.dspPATH, self.email, str(self.ANALYTICSECT), str(self.compLVL)])
        #
        # End Main db task
        #
    ''' Database '''

    # database

    # populate a table in tableView on pg_2
    #
    # first start the .db decrypted from __init__. Primary focus is to minimally load the database.
    # only load the main elements once and set self.db to True. Then user selects tables from the drop
    # down box to reload.
    #
    # methods can reload the page or the table selector. switching to page 2 only will load once.
    #
    def displaydb(self, table="logs", rfsh=False, isreload=False):

        sender = self.sender()
        dybit = self.dirtybit
        if sender != self.ui.combdb and self.isloaded() and not rfsh:
            return True
        if not self.validata(database=True, override=True):
            return False
        if not isreload:
            self.ui.combdb.setEnabled(False)
            QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)

        def load_combdb():
            cd = self.ui.combdb
            if tables:
                c_text = cd.currentText()
                cd.blockSignals(True)
                cd.clear()
                cd.addItems(tables)

                ix = cd.findText('extn')  # dont display extn table
                if ix != -1:
                    cd.removeItem(ix)

                ix = cd.findText(c_text)  # restore prev setting
                if ix != -1:
                    cd.setCurrentIndex(ix)
                else:
                    if cd.count() > 0:
                        cd.setCurrentIndex(0)
                        self.table = cd.currentText()
                cd.blockSignals(False)
        db = None
        query = None
        res = False

        try:
            db, err = get_conn(self.dbopt, "sq_7")
            if err:
                self.ui.hudt.appendPlainText(f"Failed to display {table} table: {err}")
                self.ui.tableView.setModel(None)
            else:
                self.ui.dbmainb2.setEnabled(False)
                if table == "sys":
                    self.ui.dbmainb2.setEnabled(True)

                tables = db.tables()
                if tables:

                    res = True
                    self.db = True
                    load_combdb()  # Update combobox if tables exist

                    if not isreload:
                        self.table = table
                        self.model = QStandardItemModel()
                        self.proxy = QSortFilterProxyModel()
                        self.proxy.setSourceModel(self.model)
                        self.proxy.setSortCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
                        self.ui.tableView.setModel(self.proxy)
                        self.ui.tableView.setSortingEnabled(True)

                        self.init_dbstreamer(table, batch_size=500)
                        self.worker2.start()

                    if dybit:  # Anything to append?
                        query = QSqlQuery(db)
                        self.dirtybit = False
                        last_drive = self.ui.combd.currentText()
                        set_j_settings({"last_drive": last_drive}, filepath=self.sj)
                        notes = self.ui.textEdit.toPlainText()
                        save_note(self.ui.hudt, notes, query)

        except Exception as e:
            res = False
            self.ui.hudt.appendPlainText(f"failure in displaydb err: {e} \n {traceback.format_exc()}")

        finally:
            if query:
                del query
            if db:
                db.close()

        if res and dybit:  # Released the connection above to append, otherwise locked out
            if not encr(self.dbopt, self.dbtarget, self.email, self.nc, False):
                self.ui.hudt.appendPlainText("Problem rencrypting notes.")

        if not (rfsh and isreload):  # Only update connection status on combo selection
            if self._status_reset_timer.isActive():
                self._status_reset_timer.stop()
            self.ui.dbmainlabel.setText("Status: Connected" if res else "Status: offline")

        if not (isreload or res) and getattr(self, 'worker2', None) is None:
            self.isexec = False
            self.ui.combdb.setEnabled(True)
            QApplication.restoreOverrideCursor()
        elif isreload:
            self.isexec = False
        return res

    # Sql helpers
    def init_dbstreamer(self, table, systables=None, superimpose=False, batch_size=500):

        self.result = None
        self.resStatus = -1
        self.worker2 = DbWorkerIncremental(self.dbopt, table, sys_tables=systables, superimpose=superimpose, batch_size=batch_size)
        self.worker2.log.connect(self.append_log)
        self.worker2.exception.connect(
            lambda t, v, tb: sys.excepthook(t, v, tb)
        )
        self.worker2.headers_ready.connect(lambda headers, tname=table: self.on_header_values(headers, tname))
        self.worker2.batch_ready.connect(self.append_rows_to_model)
        self.worker2.finished_loading.connect(
            lambda code, tname=table: self.on_load_finished(tname, code)
        )
        # self.worker.finished.connect(self.cleanup_thread)

    # main dn draw set appropriate sizes
    def on_header_values(self, headers, table):
        self.model.setHorizontalHeaderLabels(headers)
        header = self.ui.tableView.horizontalHeader()
        # header.setSectionResizeMode(QHeaderView.Fixed)
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)

        # uniform width for all columns
        # fixed_width = 150
        # for i in range(len(headers)):
        # header.resizeSection(i, fixed_width)

        # maximum width 1000
        if table != 'sys':
            header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
            for i in range(len(headers)):
                width = header.sectionSize(i)
                if width > 1000:
                    header.resizeSection(i, 1000)
        else:
            # per-table per-column width list
            column_widths = [60, 110, 900, 110, 130, 110, 215, 75, 50, 115, 115, 50, 50, 110, 70]
        # else:
            # column_widths = [100] * len(headers)
            for i, w in enumerate(column_widths):
                if i < len(headers):
                    header.resizeSection(i, w)

    def append_rows_to_model(self, rows):
        for row_data in rows:
            items = []
            for val in row_data:
                item = QStandardItem(str(val))
                if isinstance(val, (int, float)):
                    item.setData(val, Qt.ItemDataRole.DisplayRole)
                items.append(item)
            self.model.appendRow(items)

    def on_load_finished(self, table, code):

        self.result = code
        self.ui.combdb.setEnabled(True)
        QApplication.restoreOverrideCursor()
        self.worker2 = None
        self.isexec = False

    def reload_table(self):
        self.displaydb(self.table, True, False)

    def super_impose(self):
        if not self.validata(database=True, override=True):
            return

        # self.ui.combdb.setEnabled(False)
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)

        self.model = QStandardItemModel()
        self.proxy = QSortFilterProxyModel()
        self.proxy.setSourceModel(self.model)
        self.proxy.setSortCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.ui.tableView.setModel(self.proxy)
        self.ui.tableView.setSortingEnabled(True)

        table = self.sys_a + "_" + self.sys_b
        combo = self.ui.combdb
        if table not in [combo.itemText(i) for i in range(combo.count())]:
            combo.blockSignals(True)
            combo.addItem(table)
            ix = combo.findText(table)
            if ix != -1:
                combo.setCurrentIndex(ix)
            combo.blockSignals(False)

        sys_tables = (self.sys_a, self.sys_b)
        self.init_dbstreamer("sys", sys_tables, superimpose=True, batch_size=500)
        self.worker2.start()
    # end Sql helpers

    ''' Thread '''

    # Thread
    #
    def validata(self, database=False, override=False):
        if not self.isexec:
            self.isexec = True
            if database:
                if os.path.isfile(self.dbopt):
                    return True
                else:
                    self.isexec = False
                    self.ui.hudt.appendPlainText("No database loaded.")
            else:
                return True
        else:
            if not override:
                wdisplay_message(self, "there is a current job started.", "Execution")
        return False

    def cleanup_thread(self):
        if self.worker_thread:
            self.worker_thread = None
        if self.worker:
            self.worker = None

    def init_thread(self):
        self.worker.log.connect(self.append_log)  # self.append_colored_output)
        self.worker.complete.connect(lambda code: self.finalize(code))
        self.worker.complete.connect(self.worker_thread.quit)
        self.worker.complete.connect(self.worker.deleteLater)
        self.worker_thread.finished.connect(self.cleanup_thread)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)

    def open_trd(self, timeout=60000):
        self.isexec = True
        self.result = None
        self.resStatus = -1
        self.timeout_timer.start(timeout)
        self.stop_worker_sn.connect(self.worker.stop)  # type: ignore
        self.worker_thread.start()

    @Slot(int)
    def finalize(self, code):
        if code != 4:
            if self.timeout_timer.isActive():
                self.worker_timeout_sn.emit()  # stop the timer from main gui

        self.is_user_abort = False

        self.result = code

        if code != 0:
            # 7 stop requested return
            #
            hudt = self.ui.hudt
            if code == 7:
                hudt.appendPlainText("worker replied to stop")
            elif code != 4:
                hudt.appendPlainText(f"Exit code {code}")
            else:
                hudt.appendPlainText(f"Worker exited with error code {code}")
        # self.worker = None
        self.isexec = False

    def thread_timeout(self):
        t = getattr(self, "worker_thread", None)
        if getattr(self, "worker", None) and isinstance(t, QThread) and t.isRunning():
            self.ui.hudt.appendPlainText("Thread time out forcing quit")
            self.stop_worker_sn.emit()
            t.quit()
            QTimer.singleShot(5000, self._fk_thread)
        else:
            self.ui.hudt.appendPlainText("Thread closed unexpectedly")
            self.finalize(4)

    def _fk_thread(self):
        try:
            t = getattr(self, "worker_thread", None)
            if isinstance(t, QThread):
                if t and t.isRunning():
                    self.ui.hudt.appendPlainText("Thread did not quit properly after timeout")
                    t.terminate()
                    t.wait(1000)
        except (RuntimeError, AttributeError) as e:
            logging.error("Error in _fk_thread on thread timeout: %s", e, exc_info=True)
        self.finalize(4)
    #
    # end Thread

    # Thread types
    # Mft
    def start_mfttrd(self, log_label, mmin, method, output_f, csvnm, flnm, OLDSORT, flnmout, flnmdffout, drive, USRDIR, disk=None, volume=None, mft=None):
        self.worker_thread = QThread()
        self.worker = MftWorker(self.lclhome, log_label, mmin, method, output_f, csvnm, flnm, OLDSORT, flnmout, flnmdffout, drive, USRDIR, disk, volume, mft)
        self.worker.progress.connect(self.increment_progress)
        self.ui.progressBAR.setValue(0)
        self.worker.exception.connect(
            lambda t, v, tb: sys.excepthook(t, v, tb)
        )
        self.init_thread()
        self.worker.moveToThread(self.worker_thread)

    # Db
    def start_cleartrd(self):

        self.worker_thread = QThread()
        self.worker = ClearWorker(self.dbopt, self.dbtarget, self.nc, self.email, self.usr, self.flth, False)
        self.worker.moveToThread(self.worker_thread)

        self.worker.progress.connect(self.increment_db_progress)

        self.ui.dbprogressBAR.setValue(0)
        self.worker.exception.connect(
            lambda t, v, tb: sys.excepthook(t, v, tb)
        )
        self.init_thread()
    # end Thread types

    # general tasks db

    def _run_clear_task(self, worker_method, set_task=None):

        self.worker_thread.started.connect(worker_method)
        if set_task:  # pass in
            self.worker.set_task(*set_task)
        self.open_trd()

    # fork query button
    def execute_query(self):
        if not self.validata(True):
            return
        if not self.isloaded():
            if not self.table_loaded('logs'):
                self.isexec = False
                return
        self.start_cleartrd()
        self.worker.progress.connect(self.increment_progress)
        self.worker.status.connect(self.update_db_status)
        self.ui.hudt.appendPlainText("\n")
        self._run_clear_task(self.worker.run_query)

    # fork cache clear button
    def clear_cache(self):
        if not self.validata(True):
            return
        if not self.table_loaded('logs'):
            self.isexec = False
            return
        self.nc = intst(self.dbopt, self.compLVL)
        self.start_cleartrd()
        self.worker.status.connect(self.update_db_status)  # db label pg2
        self.worker.complete.connect(lambda code: self.reload_database_elesn.emit(code, False, ("logs",)))  # db reload pg2
        self._run_clear_task(self.worker.run_cacheclr, None)

    # fork clear IDX button
    # From _pg2 or remove index button on page 1. the former is a basedir the latter is a drive index from find downloads
    def clear_sys(self, drive, cache_s=None, sys_tables=None, cache_table=None, systimeche=None, idx=None):
        if not self.validata(True):
            return False

        prompt_v = "Previous sys profile has to be cleared. Continue?" if not idx else f"drive {drive} has a sys profile and has to be cleared. Continue?"

        cache_s = cache_s or self.CACHE_S
        sys_tables = sys_tables or (self.sys_a, self.sys_b)
        cache_table = cache_table or self.cache_table
        systimeche = systimeche or self.systimeche

        sys_a = sys_tables[0]
        sys_b = sys_tables[1]

        tables = (sys_a, sys_b, cache_table, systimeche)

        # if it is basedir is there anything to clear?
        if drive == self.basedir:
            if not self.table_loaded(sys_tables[0]):
                self.isexec = False
                return

        # if it is a drive index that is another basedir it could have a system profile. prompt to delete or exit. if an error exit
        if idx:
            if not has_sys_data(self.dbopt, self.ui.hudt, sys_tables[0], prompt_v, parent=self):
                self.isexec = False
                return

        self.nc = intst(self.dbopt, self.compLVL)
        self.start_cleartrd()
        self.worker.status.connect(self.update_db_status)  # db label pg2
        self.worker.complete.connect(lambda code, tables=tables: self.reload_database_elesn.emit(code, True, tables))

        drive_idx = None
        if idx:
            drive_idx = drive

            self.worker.complete.connect(lambda code, idx=idx: self.reload_drives_sn.emit(code, idx, drive))

        self.worker.complete.connect(lambda code: self.reload_sj_sn.emit(code, drive_idx, 'rmv'))

        self._run_clear_task(self.worker.run_sysclear, [cache_s, sys_tables, cache_table, systimeche])

    #
    # end general tasks db

    # On completion Database Helpers

    # check if there actually is a table before trying to do anything
    def table_loaded(self, table_nm):
        try:
            if os.path.isfile(self.dbopt):
                with DBMexec(self.dbopt, "sq_1", ui_logger=self.ui.hudt) as dmn:
                    if dmn.table_has_data(table_nm):
                        return True
        except DBConnectionError as e:
            self.ui.hudt.appendPlainText(f"Database table_loaded main.py error: {e}")
        except Exception as e:
            logging.error(f"err while checking table in table_loaded: {type(e).__name__} {e} \n{traceback.format_exc()}")
        return False

    # On completion of a db task either update the table or only combo depending if the user is on that page
    def reload_database(self, code, is_remove=False, tables=('logs',)):
        if code == 0:

            only_combo = False
            current_table = self.ui.combdb.currentText()  # if the user is on the selected table we want to reload it.

            if is_remove:
                only_combo = True
            else:
                if current_table not in tables:
                    only_combo = True  # the user is not on that table just refresh combobox selector

            if not self.database_reload_timer.isActive():
                self.database_reload_timer.timeout.connect(
                    lambda: self.displaydb(current_table, rfsh=True, isreload=only_combo)
                )
                self.database_reload_timer.start(5000)

        elif code == 52:
            self.ui.hudt.appendPlainText("A problem saving changes was detected everything preserved. Diagnose if there are any gpg related problems.")
            self.ui.hudt.appendPlainText("Report any database bugs to colby.saigeon@gmail.com")
            # loadgpg(self.dbopt, self.dbtarget, self.ui.dbmainlabel) # roll back on failure
    # end On completion Database Helpers

    # General Helpers

    # Main search 5 min, search, 5 min filtered, filtered search update ui on completion
    def update_ui_settings(self, code, action_type):
        if code == 0:
            if action_type == "search":
                self.ui.diffchka.setChecked(False)
                self.ui.diffchkb.setChecked(False)
                self.ui.diffchkc.setChecked(False)
            elif action_type == "Import":
                self.resetMftImport()

    # pg 1 index combo
    def reload_drives(self, code, idx=None, drive=None):
        if code == 0:

            if idx and idx > 0:  # removed index

                self.ui.combd.removeItem(idx)

                self.load_last_drive(self.basedir)

            elif drive:  # added index

                cache_s, _ = get_cache_s(drive, self.oldCACHE_S)
                cf = os.path.join(self.lclhome, cache_s)
                if os.path.isfile(cf):
                    self.ui.combd.addItem(drive)
                    ix = self.ui.combd.findText(drive)
                    if ix != -1:
                        self.ui.combd.setCurrentIndex(ix)
                else:
                    self.ui.hudt.appendPlainText(f"Drive not found {drive}")
            else:
                self.ui.hudt.appendPlainText("Invalid argument no drive specified, reload_drives")

    # manage json
    def manage_sj(self, code, drive_idx, locale):
        if code == 0:
            if drive_idx:
                if locale == 'add':

                    self.last_drive = drive_idx
                    set_j_settings({"last_drive": drive_idx}, filepath=self.sj)

                if locale == 'rmv':
                    set_j_settings(drive=drive_idx, filepath=self.sj)  # remove drive info for index
                    last_drive = get_j_settings(["last_drive"], filepath=self.sj)
                    last_drive_value = last_drive.get("last_drive")

                    if last_drive_value and last_drive_value == drive_idx:
                        set_j_settings({"last_drive": self.basedir}, filepath=self.sj)  # set to default as index removed

            else:  # add or remove proteus EXTN from usrprofile.json for basedir
                if locale == 'add':
                    extn = self.psEXTN
                    set_j_settings({"proteus_EXTN": extn}, drive=self.basedir, filepath=self.sj)
                    extn = ', '.join(extn)
                    self.ui.hudt.appendPlainText(f'Profile drive {self.basedir} saved extensions: {extn}\n')

                if locale == 'rmv':
                    set_j_settings({"proteus_EXTN": None}, drive=self.basedir, filepath=self.sj)

        else:
            if drive_idx:
                if locale == 'add':
                    set_j_settings(drive=drive_idx, filepath=self.sj)  # remove preliminary drive info as add index had failed

    # end General Helpers
    #
    # end page_2


def start_main_window():
    # inst, ver = pwsh_7()
    # if not inst:
    #     if ver is not None:
    #         print(f"PowerShell 7 is required. Installed version: {ver}")
    #     sys.exit(1)
    is_admin()
    usr = get_usr()
    if not usr:
        print("Unable to get username exiting.")
        sys.exit(1)

    appdata_local = Path(__file__).resolve().parent  # Path(sys.argv[0]).resolve().parent # get_wdir() # software install aka workdir
    json_file = appdata_local / "config" / "usrprofile.json"
    iconPATH = appdata_local / "Resources" / "rntchanges.ico"
    toml_file = appdata_local / "config" / "config.toml"

    config = load_config(toml_file)
    email = config['backend']['email']
    email_name = config['backend']['name']
    dspEDITOR = config['display']['dspEDITOR']
    if dspEDITOR:
        dspEDITOR = multi_value(dspEDITOR)
    dspPATH_frm = config['display']['dspPATH']
    popPATH = config['display']['popPATH']
    dbtarget_frm = appdata_local / "recent.gpg"  # res_path(config['paths']['pydbpst'], usr)
    dbtarget = str(dbtarget_frm)
    downloads = res_path(config['search']['downloads'], usr)
    ll_level = config['search']['logLEVEL']
    basedir = config['search']['drive']
    driveTYPE = config['search']['modelTYPE']

    driveTYPE = setup_drive_settings(basedir, driveTYPE, json_file, toml_file, False, appdata_local)
    if driveTYPE is None:
        sys.exit(1)
    elif driveTYPE.lower() not in ('hdd', 'ssd'):
        print(f"Incorrect setting modelTYPE: {driveTYPE}, must be in HDD or SSD in config: {toml_file}")
        sys.exit(1)
    zipPATH = config['search']['zipPATH']

    # startup/initialize
    setup_logger(ll_level, process_label="mainwindow", wdir=appdata_local)

    gpg_path = shutil.which("gpg")
    gnupg_home = None
    if gpg_path is None:
        gnupg_home = set_gpg(appdata_local, "gpg")
    else:
        gpg_path = Path(gpg_path).resolve()

    if not check_for_gpg:
        print("Unable to verify gpg in path. Likely path was partially initialized. quitting")
        return 1

    check_utility(zipPATH, downloads, popPATH)

    dspPATH = ""
    if dspEDITOR:  # user wants results output in text editor
        dspEDITOR, dspPATH = resolve_editor(dspEDITOR, dspPATH_frm, toml_file)  # verify we have a working one

    domain_frm = os.environ.get('USERDOMAIN', '.')  # used to set perms for database location below
    if not domain_frm:
        domain_frm = win32api.GetDomainName()
        if not domain_frm:
            print("Unable to get domain to set perms for recent changes qt app. exiting")
            sys.exit(1)
    # end startup

    with tempfile.TemporaryDirectory() as tempdir:
        try:
            # set perms for temp directory
            set_userperm = f"{domain_frm}\\" + usr
            subprocess.run(["icacls", tempdir, "/inheritance:r"], stdout=subprocess.DEVNULL)
            subprocess.run(["icacls", tempdir, "/reset"], stdout=subprocess.DEVNULL)  # remove administrators
            subprocess.run(["icacls", tempdir, "/grant", f"{set_userperm}:(OI)(CI)F"], stdout=subprocess.DEVNULL)
            # subprocess.run(["icacls", tempdir, "/grant", "NT AUTHORITY\\SYSTEM:(OI)(CI)F"], check=True) # add admins back
            # subprocess.run(["icacls", tempdir, "/grant", "BUILTIN\\Administrators:(OI)(CI)F"], check=True)

            output = os.path.splitext(os.path.basename(dbtarget))[0]
            dbopt = os.path.join(tempdir, output + '.db')

            app = QApplication(sys.argv)

            if not iskey(email):
                res = False

                pawd, ok = QInputDialog.getText(
                    None,
                    "Enter GPG Password",
                    "Password:",
                    QLineEdit.EchoMode.Password
                )
                if ok and pawd:
                    res = genkey(email, email_name, tempdir, pawd)
                    if res:
                        print("Got password (hidden):", "*" * len(pawd) + "\n")
                if not ok or not res:
                    QMessageBox.critical(None, "Error", "Failed to generate key")
                    sys.exit(1)

            if os.path.isfile(dbtarget):
                if not decr(dbtarget, dbopt):
                    QMessageBox.critical(None, "Error", "Decryption failed. exitting.")
                    sys.exit(1)
            # end startup/initialize

            print("Qt database in ", tempdir)
            icon_path = str(iconPATH)

            def excepthook(exc_type, exc_value, exc_traceback):
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                logging.error("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))
                # if QApplication.instance() is not None:
                #     QMessageBox.critical(None, "Error", f"{exc_value}")

                app_inst = QApplication.instance()
                if app_inst is None:
                    app_inst = QApplication(sys.argv)

                msg = QMessageBox()
                msg.setIcon(QMessageBox.Icon.Critical)
                msg.setWindowTitle("Error")
                msg.setText(f"An unexpected error occurred:\n{exc_value}")
                msg.setStandardButtons(QMessageBox.StandardButton.Ok)
                msg.exec_()

                log_pth = os.path.join(appdata_local, "logs", "errs.log")
                print(f"Unhandled exception {exc_type.__name__} stack trace logged to: {log_pth}")
                sys.exit(1)
            sys.excepthook = excepthook

            exit_code = 0
            window = MainWindow(appdata_local, config, toml_file, json_file, driveTYPE, dbopt, dbtarget, gpg_path, gnupg_home, dspEDITOR, dspPATH, popPATH, email, usr, tempdir)
            window.setWindowIcon(QIcon(icon_path))
            window.show()
            exit_code = app.exec()

            sys.exit(exit_code)

        except Exception as e:
            em = "Failed to initialize qt app:"
            print(f"{em} {type(e).__name__} err: {e} \n {traceback.format_exc()}")
            QMessageBox.critical(None, "Error", f"{e}")
            logging.error(em, exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    sys.exit(start_main_window())
