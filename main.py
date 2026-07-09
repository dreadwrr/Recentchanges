# 06/18/2026               Qt gui windows 11                  Developer buddy 5.0
import glob
import logging
import multiprocessing
import os
import shutil
import sqlite3
import subprocess
import sys
# 05/30/2026 this can bypass prevent cp1252 unicode error
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
import tempfile
import threading
import traceback
import win32api
from pathlib import Path
from PySide6.QtCore import Qt, Slot, Signal, QThread, QTimer, QSortFilterProxyModel, QSize
from PySide6.QtGui import QStandardItemModel, QStandardItem, QIcon, QPixmap, QImage
from PySide6.QtSql import QSqlQuery
from PySide6.QtWidgets import QApplication, QFileDialog, QMessageBox, QMainWindow, QVBoxLayout, QMenu, QHeaderView, QStyle, QHBoxLayout, QDialog, QLabel, QPushButton
from src.alarmclock import AlarmClock
from src.calculator import SCalculator
from src.clearworker import ClearWorker
from src.config import dump_j_settings
from src.config import dump_toml
from src.config import load_toml
from src.config import update_dict
from src.config import update_j_settings
from src.config import update_toml_setting
from src.config import update_toml_values
from src.configfunctions import check_config
from src.configfunctions import find_gnupg_home
from src.configfunctions import find_user_folder
from src.configfunctions import get_config
from src.dbworkerstream import DbWorkerIncremental
from src.dirwalkerfunctions import create_profile_baseline
from src.dirwalkerfunctions import EXEC_EXTN
from src.gpgcrypto import check_for_gpg
from src.gpgcrypto import decr
from src.gpgcrypto import encr
from src.gpgkeymanagement import genkey
from src.gpgkeymanagement import iskey
from src.imageraster import raised_image
from src.inotifyfunctions import old_pid_check
from src.inotifyfunctions import process_by_target
from src.inotifyfunctions import trim_tout
from src.logs import change_logger
from src.logs import setup_logger
from src.mftfunctions import mftec_version
from src.mftworker import MftWorker
from src.processhandler import ProcessHandler
from src.pyfunctions import cache_clear_patterns
from src.pyfunctions import cnc
from src.pyfunctions import user_path
from src.pysql import create_db
from src.pysql import dbtable_has_data
from src.pysql import get_lifetime_throughput
from src.pysql import get_unique_files
from src.qtclasses import BasedirDrive
from src.qtclasses import BasedirProfiles
from src.qtclasses import ConfigurationError
from src.qtclasses import DriveLogicError
from src.qtclasses import DriveSelectorDialog
from src.qtclasses import FastColorText
from src.qtclasses import PassphraseDialog
from src.qtclasses import QTextEditLogger
from src.qtdrivefunctions import current_drive_type_model_check
from src.qtdrivefunctions import get_cache_s
from src.qtdrivefunctions import get_drive_from_partguid
from src.qtdrivefunctions import get_idx_tables
from src.qtdrivefunctions import get_mount_partguid
from src.qtdrivefunctions import parse_drive
from src.qtdrivefunctions import setup_drive_cache
from src.qtdrivefunctions import setup_drive_settings
from src.qtfunctions import add_new_extension
from src.qtfunctions import available_fonts
from src.qtfunctions import check_for_updates
from src.qtfunctions import clear_from_extn_tbl
from src.qtfunctions import command_prompt
from src.qtfunctions import commit_note_history
from src.qtfunctions import fill_extensions
from src.qtfunctions import get_conn
from src.qtfunctions import get_help
from src.qtfunctions import get_history_view
from src.qtfunctions import has_log_data
from src.qtfunctions import has_sys_data
from src.qtfunctions import help_about
from src.qtfunctions import load_explorer
from src.qtfunctions import load_gpg
from src.qtfunctions import load_pshell
from src.qtfunctions import open_html_resource
from src.qtfunctions import profile_to_str
from src.qtfunctions import ps_profile_type
from src.qtfunctions import rmv_path
from src.qtfunctions import set_path
from src.qtfunctions import show_cmddoc
from src.qtfunctions import sort_right
from src.qtfunctions import table_loaded
from src.qtfunctions import user_data_from_database
from src.qtfunctions import user_data_to_database
from src.qtfunctions import window_prompt
from src.qtfunctions import window_message
from src.qtparser import dispatch_internal as dispatcher
from src.query import blank_count
from src.rntchangesfunctions import check_installed_app
from src.rntchangesfunctions import check_utility
from src.rntchangesfunctions import display
from src.rntchangesfunctions import get_diff_file
from src.rntchangesfunctions import is_admin
from src.rntchangesfunctions import multi_value
from src.rntchangesfunctions import name_of
from src.rntchangesfunctions import removefile
from src.rntchangesfunctions import resolve_editor
from src.rntchangesfunctions import set_gpg
from src.rntchangesfunctions import time_convert
from src.rntchangesfunctions import windows_version
from src.ui_mainwindow import Ui_MainWindow
from src.wmipy import get_disk_and_volume_for_drive
from src.wmipy import get_mounted_partitions
from src.wmipy import validmft
# from src.ui_alarmclock import Ui_AlarmClock

# class AlarmClock(QWidget):
#     def __init__(self, parent=None):
#         super().__init__(parent)
#         self.ui = Ui_AlarmClock()
#         self.ui.setupUi(self)
# self.alarm_widget = AlarmClock(self.ui.page)

# layout = self.ui.gridLayout

# for i in range(layout.count()):
#     item = layout.itemAt(i)
#     if item.widget() == self.ui.lcdNumber:
#         row, column, rowspan, colspan = layout.getItemPosition(i)
#         layout.addWidget(self.alarm_widget, row, column, rowspan, colspan)

#         self.ui.lcdNumber.deleteLater()
#         break
# else:
#     raise RuntimeError("Placeholder not found in layout")


class MainWindow(QMainWindow):

    worker_timeout_sn = Signal()
    proc_timeout_sn = Signal()
    stop_worker_sn = Signal()  # stop thread or proc
    stop_proc_sn = Signal()
    reload_database_sn = Signal(int, bool, object)  # hudt append text
    update_ui_sn = Signal(int, str)  # change checkboxes after QProcess or thread
    reload_drives_sn = Signal(int, int, str)  # update drive combobox on complete
    reload_sj_sn = Signal(int, object, str, bool)  # also update drive combo on complete

    def __init__(
        self, appdata_local, home_dir, config, j_settings, toml_file, json_file, log_dir, log_path, driveTYPE, distro_name,
        dbopt, dbtarget, cache_s, cache_s_str, systimeche, suffix, gpg_path, gnupg_home, dspEDITOR, dspPATH, popPATH,
        alarm_soundFILE, alarm_set_soundFILE, downloads, email, usr, cachermPATTERNS, tempdir
    ):
        super().__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

        # self.ui.lcdNumber.ui.downb.hide()
        # self.ui.lcdNumber.ui.setb.hide()

        # import winsound
        # print("\a")
        # frequency in Hz, duration in ms
        # winsound.Beep(1000, 500)

        self.toml_file = toml_file
        self.sj = json_file
        self.driveTYPE = driveTYPE
        self.distro_name = distro_name
        self.log_dir = log_dir
        self.log_path = log_path
        self.dbopt = dbopt  # db
        self.dbtarget = dbtarget  # gpg
        self.gpg_path = gpg_path
        self.gnupg_home = gnupg_home
        self.cache_s = cache_s
        self.cache_s_str = cache_s_str
        self.systimeche = systimeche  # ie C:\\path\\systemche J:\\path\\systimeche_j
        self.suffix = suffix
        self.dspEDITOR = dspEDITOR
        self.dspPATH = dspPATH
        self.popPATH = popPATH
        self.downloads = downloads
        self.email = email
        self.usr = usr
        self.cachermPATTERNS = cachermPATTERNS
        self.tempdir = tempdir  # thisapp

        self.config = None
        self.analytics = config['analytics']['analytics']
        self.feedback = config['analytics']['feedback']
        self.compLVL = config['logs']['compLVL']
        self.pageIDX = config['display']['pageIDX']
        self.hudCOLOR = config['display']['hudCOLOR']
        self.hudSZE = config['display']['hudSZE']
        self.hudFNT = config['display']['hudFNT']

        self.alarm_24h = config['display']['alarm_24hr']
        self.alarm_soundFILE = alarm_soundFILE
        self.alarm_set_soundFILE = alarm_set_soundFILE
        self.alarmCOLOR = config['display']['alarmCOLOR']

        self.moduleNAME = config['paths']['moduleNAME']  # diff_file prefix
        self.pwrshell = config['search']['pwrshell']
        self.basedir = config['search']['drive']  # search target
        self.oldbasedir = self.basedir
        proteusEXTN = config['shield']['proteusEXTN']
        self.proteusEXTN = ["[no extension]" if p == "" else p for p in proteusEXTN]
        self.proteusPATH = config['shield']['proteusPATH']
        self.checksum = config['diagnostics']['checkSUM']
        self.proteusSHIELD = config['shield']['proteusSHIELD']
        self.psEXEC = config['shield']['exec']
        self.exclDIRS = user_path(config['search']['exclDIRS'], usr)
        self.xRC = config['search']['xRC']
        self.high_water = config['search']['high_water']
        self.low_water = config['search']['low_water']
        self.min_span = config['search']['min_span']
        self.zipPROGRAM = config['compress']['zipPROGRAM'].lower()
        self.zipPATH = config['compress']['zipPATH']
        self.extensions = config['search']['extension']
        self.cmode = config['calculator']['mode']
        self.decimals = config['calculator']['decimals']
        self.cTHRESHOLD = config['calculator']['scientific_threshold']
        self.ctheme = config['calculator']['theme']
        self.chistory = config['calculator']['history']
        self.randintMAX = config['calculator']['randintMAX']
        self.randintMIN = config['calculator']['randintMIN']
        self.clogLEVEL = config['calculator']['logLEVEL']
        self.j_settings = j_settings  # usrprofile

        self.psEXTN = self.j_settings.get(self.basedir, {}).get("proteusEXTN")
        self.is_exec_profile = False
        if self.psEXTN:
            self.is_exec_profile = ps_profile_type(self.psEXTN)

        self.timer = QTimer(self)
        self.timer.timeout.connect(self.remaining_startup)  # any other routines
        self.timer.start(1000)

        # load the database at some point if not already by pg change
        # QTimer.singleShot(5000, self.display_db)

        # Vars
        self.app_version = "6.1.2"
        self.pwd = os.getcwd()
        self.home_dir = home_dir
        config_local = appdata_local / "config"
        self.usrDIR = find_user_folder("Desktop")
        if self.usrDIR is None:
            raise EnvironmentError("Could not find user Desktop folder")
        self.lclhome = appdata_local
        self.lclscripts = appdata_local / "scripts"
        self.inotify_creation_file = self.lclscripts / "file_creation_log.txt"  # 07/04/2026
        self.search_pattern = "watchdog_win.py"
        self.watchdog_pid_file = os.path.join(self.lclscripts, 'inotify_watcher.pid')
        self.resources = appdata_local / "Resources"
        self.filter_file = appdata_local / "filter.py"
        flth_frm = appdata_local / "flth.csv"
        self.flth = str(flth_frm)
        self.dispatch = sys.executable
        self.app = str(appdata_local / "src" / "set_recent_helper.py")
        self.filter_file = appdata_local / "filter.py"

        self.default_gpg = appdata_local / "gpg" / "gpg.exe"
        self.defaultzipPATH = appdata_local / "7-zip" / "7z.exe"

        self.exe_path = self.lclhome / "bin"
        self.parsec_command = self.exe_path / "parsec.exe"
        self.mftec_command = self.exe_path / "MFTECmd.exe"
        self.icat_command = self.exe_path / "icat.exe"
        self.ntfs_command = self.exe_path / "ntfstool.x86.exe"
        self.fsstat_command = self.exe_path / "fsstat.exe"

        self.jpgdir = appdata_local / "Documents"  # str(Path.home() / "Documents")   /home/guest/.config/icons/
        self.crestdir = self.jpgdir / "crests"
        self.jpgdefault = "background.bak"  # default png
        self.crestdefault = "dragonm.bak"  # . crest

        # 06/09/2026
        # self.alarmdir = self.resources  # alarms are stored in \\Resources\\
        self.alarm_sounddefault = "alarm.mp3"  # default in \\Resources\\
        self.alarm_set_sounddefault = "alarmt.mp3"

        self.picture = self.jpgdir / "background.png"  # current png
        self.crest = self.crestdir / "dragonm.png"  # . crest

        self.command_file = self.resources / "commands.txt"

        self.defaultdiff = os.path.join(self.usrDIR, f'{self.moduleNAME}xSystemDiffFromLastSearch500.txt')
        self.mftflnm = f'{self.moduleNAME}xMftchanges'

        self.tomldefault = config_local / "config.bak"
        self.tomldefault_imt = None  # initial mtime

        sys_tables, self.cache_table, _ = get_idx_tables(self.basedir, self.cache_s_str, suffix)
        self.sys_a, self.sys_b = sys_tables

        self.is_pyinstall = False
        if getattr(sys, "frozen", False) or "__compiled__" in globals():
            self.is_pyinstall = True
            self.dispatch = Path(sys.argv[0]).resolve()  # set internal python
        self.isexec = False

        self.new_sound_file = False  # when there is a pending sound file this is set to the path
        self.is_user_edit = False
        self.is_user_abort = False
        self.dirtybit = False  # something to save while the db is connected or program exit

        self.diff_file = None
        self.user_extensions = []

        self.mft = None  # is imported Mft
        self.ramdisk = None  # self.lclhome

        # 07/01/2026
        self.calculator = None
        self.saved_history = ""  # 07/08/2026 save history view alongside encrypted notes in extn table

        self.worker = None
        self.worker2 = None

        self.worker_thread = None  # database streamer
        self.proc = None

        self.db = None  # set after first db load
        self.table = None  # last loaded table
        self.sys_step = None

        # 06/06/2026
        self.nt_path = None

        self.lastdir = None
        self.lastdrive = self.basedir

        self.result = None
        self.exit_result = None

        self.nc = None

        # initialize
        self.init_timers()
        self.install_clock(alarm_soundFILE, alarm_set_soundFILE)
        self.init_events()

        self.install_logger()

        self.ui.dbprogressBAR.setValue(0)
        pixmap = QPixmap(self.crest)  # Load the image from the path      '.\\Documents\\crests\\dragonm.png'  # original
        self.ui.jpgcr.setPixmap(pixmap)  # Set the pixmap on the label
        self.ui.jpgcr.setScaledContents(True)

        # self.change_format()  # apply hudt settings
        self.refresh_jpg()  # load pic

        # one time items
        ro = self.j_settings.get("search_range")
        if ro:
            self.ui.stime.setValue(int(ro))
        fo = self.j_settings.get("find_range")
        if fo:
            self.ui.sffile.setValue(int(fo))

        json_dump = False

        so = self.j_settings.get("search_output")
        if so:
            ix = self.ui.combftimeout.findText(so)
            if ix != -1:
                self.ui.combftimeout.setCurrentIndex(ix)
            else:
                update_dict(None, self.j_settings, "search_output")
                json_dump = True
                print(f"Couldnt find search output setting {so}")
        else:
            self.ui.combftimeout.setCurrentText("Desktop")

        # newer than
        no = self.j_settings.get("newer_output")
        if no:
            ix = self.ui.combt.findText(no)
            if ix != -1:
                self.ui.combt.setCurrentIndex(ix)
            else:
                update_dict(None, self.j_settings, "newer_output")
                json_dump = True
                print(f"Couldnt find newer than output setting {no}")
        # newer than starting directory for convenience
        self.nt_path = str(self.lclhome)
        ntp = self.j_settings.get("newer_path")
        if ntp:
            if os.path.exists(ntp):
                self.nt_path = ntp
            else:
                update_dict(None, self.j_settings, "newer_path")
                json_dump = True
                print(f"Couldnt find newer than output setting {no}")
        # end newer than

        # find file output is set in load_find_file_combo

        ao = self.j_settings.get("alarm_time")
        if ao:
            res = self.ui.widget.set_alarm_time(ao)
            if res == 2 or res == 3:
                update_dict(None, self.j_settings, "alarm_time")
                json_dump = True
                print(f"Alarm saved time was invalid using default value {ao}")

        if json_dump:
            dump_j_settings(self.j_settings, self.sj)

        self.initialize_ui(is_startup=True)
        # end one time items

        # loose ends set initial state before finished loading
        if not self.chistory:
            self.ui.actionHistoryv.setEnabled(False)

        if self.pageIDX == 2:
            self.show_page_2()
        elif self.pageIDX == 1:
            self.show_page()

    @Slot(str)
    def append_log(self, text):
        cursor = self.ui.hudt.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        if text and cursor.positionInBlock() != 0 and not text.startswith(("\n", "\r")):
            text = "\n" + text
        if text and not text.endswith(("\n", "\r")):
            text += "\n"
        self.ui.hudt.append_colored_output(text)

    @Slot(str)
    def update_db_status(self, text):
        """ control dbmainlabel / ui elements """
        self.ui.dbmainlabel.setText(text)
        if self._status_reset_timer.isActive():
            self._status_reset_timer.stop()
        self._status_reset_timer.start(40000)

    @Slot(int)
    def increment_progress(self, value):
        self.ui.progressBAR.setValue(value)

    @Slot(int)
    def increment_db_progress(self, value):
        self.ui.dbprogressBAR.setValue(value)

    @Slot(bool)
    def set_nc(self, nc):
        self.nc = nc

    def install_clock(self, sound_file, sound_set_file):
        old_widget = self.ui.widget
        layout = self.ui.gridLayout
        index = layout.indexOf(old_widget)
        position = layout.getItemPosition(index)
        layout.removeWidget(old_widget)
        old_widget.deleteLater()

        sound_path = os.path.join(self.resources, sound_file) if sound_file else None
        sound_set_path = os.path.join(self.resources, sound_set_file) if sound_set_file else None

        if self.alarmCOLOR == "":
            self.alarmCOLOR = None
        self.ui.widget = AlarmClock(self, theme=self.alarmCOLOR, _24hformat=self.alarm_24h, sound_file=sound_path, sound_set_file=sound_set_path)

        # self.ui.widget.setMinimumSize(QSize(50, 50))
        # self.ui.widget.setLayoutDirection(Qt.LayoutDirection.LeftToRight)
        layout.addWidget(self.ui.widget, *position)

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

        self.update_ui_sn.connect(self.update_ui_settings)
        self.reload_database_sn.connect(self.reload_database)
        self.reload_drives_sn.connect(self.reload_drives)
        self.reload_sj_sn.connect(self.manage_sj)

        # Menu bar
        self.ui.actionStop.triggered.connect(self.x_action)

        self.ui.actionSave.triggered.connect(self.save_user_data)
        self.ui.actionClearh.triggered.connect(lambda _: self.ui.hudt.clear())

        self.ui.actionExit.triggered.connect(QApplication.quit)
        self.ui.actionClear_extensions.triggered.connect(self.clear_extensions)
        self.ui.actionUpdates.triggered.connect(lambda: check_for_updates(self.app_version, "dreadwrr", "Recentchanges", self))

        self.ui.actionCommands.triggered.connect(lambda: show_cmddoc(self.command_file, self.lclhome, self.default_gpg, self.gpg_path, self.gnupg_home, self.email, self.systimeche, self.ui.hudt))
        self.ui.actionQuick1.triggered.connect(lambda: display(self.dspEDITOR, self.command_file, True, self.dspPATH))
        self.ui.actionDiag1.triggered.connect(self.show_status)
        self.ui.actionWatchdog.triggered.connect(lambda: load_explorer(self.lclscripts))
        self.ui.actionLogging.triggered.connect(lambda: display(self.dspEDITOR, self.log_path, True, self.dspPATH))

        self.ui.actionCalculator.triggered.connect(self.open_calculator)
        self.ui.actionClear_history.triggered.connect(self.clear_history)
        self.ui.actionClear_history.setEnabled(False)  # set in designer <-- ** 07/08/2026
        self.ui.actionHistoryv.triggered.connect(self.show_history)

        self.ui.actionAbout.triggered.connect(lambda: help_about(self.lclhome, self.ui.hudt, self.app_version))
        self.ui.actionResource.triggered.connect(self.open_resource)
        self.ui.actionHelp.triggered.connect(lambda: get_help(self.lclhome, self.ui.hudt))
        # end Menu bar

        # # 1 left <
        self.ui.queryButton.clicked.connect(self.execute_query)
        self.ui.sbasediridx.setMaximum(10)
        self.ui.sbasediridx.valueChanged.connect(self.set_basedir)  # change basedir beside main search .spinner
        # Main window

        # #1 mid ^ Stop Reset defaults button
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
        menu.addAction("Settings", lambda: self.edit_config())  # menu.addAction("Settings", lambda: display(self.dspEDITOR, self.toml_file, True, self.dspPATH))
        """ launch terminal / prompts """
        # menu.addAction("Explorer", lambda: load_explorer(self.lclhome, popPATH=self.popPATH))  # 06/05/2026 removed for exlorer button closer to newer than file
        menu.addAction("Launch Cmd Prompt", lambda: command_prompt(self.lclhome, popPATH=self.popPATH))
        menu.addAction("Launch Powershell", lambda: load_pshell(self.lclhome, popPATH=self.popPATH))
        menu.addSeparator()
        menu.addAction("Filter", lambda: display(self.dspEDITOR, self.filter_file, True, self.dspPATH))
        menu.addAction("Clear Hudt", lambda: self.ui.hudt.clear())
        menu.addAction("List fonts", lambda: available_fonts(self.ui.hudt))
        menu.addAction("Add to path", lambda: set_path(self.lclhome))
        menu.addAction("Rmv from path", lambda: rmv_path(self.lclhome))

        self.ui.tomlb.setMenu(menu)
        self.ui.tomlb.setPopupMode(self.ui.tomlb.ToolButtonPopupMode.InstantPopup)
        # end tomlb

        # bottom right
        self.ui.widget.soundValidated.connect(self.save_sound_to_toml)  # 06/09/2026
        self.ui.textEdit.textChanged.connect(self.set_dirtybit)  # self.ui.textEdit.textChanged.connect(lambda: setattr(self, 'dirtybit', True))  # original
        # Top search
        self.ui.ftimeb.clicked.connect(lambda checked=False, s=self.ui.ftimeb: self.tsearch(s))
        self.ui.ftimebf.clicked.connect(lambda checked=False, s=self.ui.ftimebf: self.tsearch(s, True))
        self.ui.stimeb.clicked.connect(lambda checked=False, s=self.ui.stimeb: self.tsearch(s))
        self.ui.stimebf.clicked.connect(lambda checked=False, s=self.ui.stimebf: self.tsearch(s, True))
        # New than search
        self.ui.ntsb.clicked.connect(self.ntsearch)
        self.ui.ntbrowseb.clicked.connect(self.ntsearch)
        self.ui.ntbrowseb2.clicked.connect(self.ntsearch)
        self.ui.ntbrowseb3.clicked.connect(lambda: load_explorer(self.lclhome, popPATH=self.popPATH))
        # End Top search

        # findfile
        self.ui.ffileb.clicked.connect(lambda: self.ffile(False))

        self.ui.ffileb2.clicked.connect(self.new_extension)
        self.ui.ffilecb.clicked.connect(self.ffcompress)

        self.ui.diffchkc.toggled.connect(self.set_scan)
        # page_2
        self.ui.combdb.currentTextChanged.connect(self.display_db)

        self.ui.dbmainb1.clicked.connect(self.clear_cache)
        self.ui.dbmainb2.clicked.connect(self.super_impose)
        self.ui.dbmainb4.clicked.connect(self.set_hardlinks)
        # refresh button pg2
        self.ui.dbmainb3.setIcon(self.ui.dbmainb3.style().standardIcon(QStyle.StandardPixmap.SP_ArrowLeft))
        self.ui.dbmainb3.setIconSize(QSize(20, 20))
        self.ui.dbmainb3.clicked.connect(self.reload_table)

        self.ui.dbidxb1.clicked.connect(lambda checked: self.clear_sys(self.basedir))

        self.ui.dbidxb2.clicked.connect(self.build_idx)
        self.ui.dbidxb3.clicked.connect(self.scan_idx)

        self.ui.tableView.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.ui.tableView.customContextMenuRequested.connect(self.table_context_menu)
        # nav   # End page_2
        self.ui.toolhomeb_2.clicked.connect(self.show_page)
        self.ui.toolrtb.clicked.connect(self.show_page_2)
        self.ui.toolrtb_2.clicked.connect(self.show_page)
        self.ui.toollftb_2.clicked.connect(self.show_page)
        self.ui.toollftb.clicked.connect(self.show_page_2)
        # End nav

        # mft
        self.ui.mftbrowseb.clicked.connect(self.imprt_mft_brws)
        self.ui.mftsearchb.clicked.connect(self.mftsearch)
        # End Main window

    def update_basedir(self, basedir, suffix, drive, index):

        cache_s = drive.cache_s
        systimeche = drive.systimeche
        driveTYPE = drive.drive_type
        psEXTN = drive.psextn

        self.oldbasedir = self.basedir
        self.basedir = basedir
        self.driveTYPE = driveTYPE
        self.psEXTN = psEXTN
        self.is_exec_profile = ps_profile_type(psEXTN)
        self.cache_s = cache_s
        self.systimeche = systimeche
        self.suffix = suffix
        self.load_drives()  # downloads combo

        sys_tables, self.cache_table, _ = get_idx_tables(basedir, self.cache_s_str, suffix)
        self.sys_a, self.sys_b = sys_tables
        self.reload_database(0, is_remove=True, tables=('logs',))  # sort the dbcomb

        self.basedirs.set_current_index(index)
        self.ui.sbasediridx.blockSignals(True)
        self.ui.sbasediridx.setValue(index)
        self.ui.sbasediridx.blockSignals(False)

    def set_scan(self, checked):
        if checked:
            self.ui.diffchkb.setChecked(True)

    def add_basedir(self, basedir, drive_idx, drive_guid, drive, drive_info):
        r = self.basedirs.add_item((drive_guid, drive, drive_info))
        self.ui.sbasediridx.setMaximum(self.basedirs.items - 1)
        self.update_basedir(basedir, drive_idx, drive, r)  # load the drive

    def rmv_basedir(self, index, current_index):
        r = self.basedirs.remove_item(index, current_index)
        self.ui.sbasediridx.blockSignals(True)
        self.ui.sbasediridx.setValue(r)
        self.ui.sbasediridx.setMaximum(self.basedirs.items - 1)
        self.ui.sbasediridx.blockSignals(False)

    # highlighted basedir button arrows pg_1
    def set_basedir(self):

        if not self.job_running(True):
            return

        y = self.basedirs.current_index
        x = self.ui.sbasediridx.value()

        # currently hasnt change so set drive_suffix for current suffix for debugging
        # current text of drive button parse_drive(self.ui.basedirButton.currentText())
        _, di, _ = self.basedirs.get_current_item()
        suffix = di.suffix  # or self.suffix

        try:

            guid, drive_info, info = self.basedirs.get_item(x)
            suffix = drive_info.suffix
            basedir = drive_info.moi
            if suffix != "c":

                drive_ = get_drive_from_partguid(guid) if guid else None
                if drive_:
                    basedir = drive_
                else:
                    self.rmv_basedir(x, y)
                    self.isexec = False
                    return

            self.update_basedir(basedir, suffix, drive_info, x)

        except Exception as e:
            self.ui.hudt.appendPlainText(f"changing drives. {suffix} going {'down' if y > x else 'up'} on basedir combo err: {e} {type(e).__name__}")
            logging.error("Error switching sbasediridx %s to index %s err: %s", self.basedir, x, e, exc_info=True)
            self.ui.sbasediridx.blockSignals(True)
            self.ui.sbasediridx.setValue(y)
            self.ui.sbasediridx.blockSignals(False)
        self.isexec = False

    def load_basedir_combo(self, a_drives, systimename):

        # add to class basedirs that have profiles for basedirButton

        basedirs = BasedirProfiles(self.ui.basedirButton)
        drive = self.basedir
        drive_info = self.j_settings[drive].copy()
        guid = drive_info.get("drive_partguid")
        moi = self.basedir  # moi = drive_info.get("mount_of_index")
        mtype = drive_info.get("model_type")
        dtype = drive_info.get("drive_type")
        psextn = drive_info.get("proteusEXTN")
        suffix = drive_info.get("idx_suffix")

        basedirs.add_item((guid, BasedirDrive(suffix, guid, moi, mtype, dtype, self.cache_s, self.systimeche, psextn), drive_info))

        # should be C:\\ .. ect
        for drive, suffix in a_drives:
            if drive != self.basedir and drive in self.j_settings and "proteusEXTN" in self.j_settings[drive]:

                cache_s = self.cache_s_str
                systimeche = systimename

                if drive != "C:\\":

                    systimeche = systimename + "_" + suffix
                    cache_s = systimeche + ".gpg"
                    cache_s = os.path.join(self.lclhome, cache_s)

                    # guid
                    # get back drive from guid load or not. Windows is handled in load_saved_indexes ln1500. linux is handled here with part_uuid

                drive_info = self.j_settings[drive].copy()
                guid = drive_info.get("drive_partguid")
                moi = drive  # moi = drive_info.get("mount_of_index")  # a index could not be mounted therefor dont list it
                mtype = drive_info.get("model_type")
                dtype = drive_info.get("drive_type")
                if dtype not in ("HDD", "SSD"):
                    self.ui.hudt.appendPlainText(f"Warning entry for {drive} malformed in json {self.sj} defaulting to HDD.")
                    dtype = "HDD"
                psextn = drive_info.get("proteusEXTN")

                basedirs.add_item((guid, BasedirDrive(suffix, guid, moi, mtype, dtype, cache_s, systimeche, psextn), drive_info))

        self.basedirs = basedirs
        self.ui.sbasediridx.setMaximum(basedirs.items - 1)
        self.ui.sbasediridx.setMinimum(0)
        self.ui.basedirButton.setText(self.basedir)

    def load_find_file_combo(self):

        self.ui.combffileout.clear()
        self.ui.combffileout.addItem("AppData")
        self.ui.combffileout.addItem("Desktop")

        d = self.downloads  # popPATH
        if d.strip():
            self.ui.combffileout.addItem(d)
            ix = self.ui.combffileout.count() - 1
            self.ui.combffileout.setCurrentIndex(ix)

        do = self.j_settings.get("compress_output")
        if do:
            # self.downloads
            ix = self.ui.combffileout.findText(do)
            if ix != -1:
                self.ui.combffileout.setCurrentIndex(ix)
            else:
                update_j_settings(None, self.j_settings, "compress_output", self.sj)
                print(f"Couldnt find downloads {do}")

    def initialize_ui(self, is_startup=False):

        self.change_format(is_startup)  # apply hudt settings

        # fill combos pg_1
        if is_startup:
            if os.path.isfile(self.dbopt):

                QTimer.singleShot(500, lambda: self.load_user_data(True))  # extension combo and the notes from the db
            else:
                fill_extensions(self.ui.combffile, self.extensions)  # extension combo if no db yet

                if not os.path.isfile(self.dbtarget):

                    try:
                        create_db(self.dbopt, (self.sys_a, self.sys_b))
                        if not encr(self.dbopt, self.dbtarget, self.email, no_compression=self.nc, dcr=True):
                            self.ui.hudt.appendPlainText("Unable to create database")
                    except Exception as e:
                        QMessageBox.critical(None, "Error", f"Problem creating database through initializer. Exiting.. {e}")
                        raise  # QApplication.quit()  # .exit(1)

        # find download combo
        a_drives, systimename = self.load_drives()  # if any have a cache file ? loaded it into basedir combo

        # basedir combo. basedirButton + spinner = basedir combo
        self.load_basedir_combo(a_drives, systimename)

        # find file output combo
        self.load_find_file_combo()

        # end fill combos pg_1

    def manage_file_creation_log(self):
        pid = process_by_target(self.search_pattern)
        old_pid_check(self.watchdog_pid_file, pid, "windows")
        if not pid:
            trim_tout(self.inotify_creation_file, self.low_water, self.high_water, self.min_span)

    def remaining_startup(self):

        # on startup user_data_from_database writes app start time so can determine last start time
        # here for xRC option check file_creation_log.txt if its over the cutoff and trim.

        if self.xRC:
            if os.path.isfile(self.inotify_creation_file):
                threading.Thread(target=self.manage_file_creation_log, daemon=True).start()

        # if polkit_check():
        #     self.is_polkit = True
        self.timer.stop()

    # Custom settings for hudt

    # on startup set the alarm formatting
    def change_clock(self):
        self.ui.widget.set_clock_format(self.alarm_24h)  # is it a 24hr clock?
        self.ui.widget.set_format(self.alarmCOLOR)  # digital color or theming

    # after startup change the alarm sounds from the custom button
    def save_sound_to_toml(self, valid_sound, is_set_sound, new_source):
        if new_source:

            sound_file = os.path.basename(new_source)
            # it's a valid sound file save it to .toml
            if valid_sound:

                if is_set_sound:
                    self.alarm_set_soundFILE = sound_file
                    setting = "alarm_set_soundFILE"
                else:
                    self.alarm_soundFILE = sound_file
                    setting = "alarm_soundFILE"

                update_toml_setting("display", setting, sound_file, self.toml_file)
            # dont save to toml
            else:

                print("Invalid media was unable to copy to", new_source)

                if self.new_sound_file:
                    # it was copied to \\Resources\\ and is invalid so delete it
                    sound_file = os.path.join(self.resources, sound_file)
                    removefile(sound_file)
        self.new_sound_file = None

    def set_stylesht(self, f_f, ccolor):
        # print(QFontDatabase().families())
        if not isinstance(self.hudSZE, int):
            self.ui.hudt.appendPlainText(f"Invalid size format hudSZE: {self.hudSZE} defaulting to 12")
            self.hudSZE = 12
            update_toml_values({'display': {'hudSZE': 12}}, self.toml_file)
        else:
            if self.hudSZE == 0:
                self.hudSZE = 12
        qx = ""
        if ccolor:
            qx = f"background-color: black; color: #{ccolor};"

        self.ui.hudt.setStyleSheet(f"""
            QPlainTextEdit {{
                {qx}
                font-family: {f_f};
                font-size: {self.hudSZE}pt;
            }}
        """)

    def change_format(self, is_startup=False):

        if is_startup:

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

        self.proc_timeout_timer = QTimer(self)
        self.proc_timeout_timer.setSingleShot(True)
        self.proc_timeout_timer.timeout.connect(self.proc_timeout)

        self.worker_timeout_sn.connect(self.timeout_timer.stop)
        self.proc_timeout_sn.connect(self.proc_timeout_timer.stop)

        self._status_reset_timer = QTimer(self)
        self._status_reset_timer.setSingleShot(True)
        self._status_reset_timer.timeout.connect(
            lambda: self.ui.dbmainlabel.setText(
                "Status: Connected" if self.tableview_loaded() else "Status: offline"
            )
        )

        self.database_reload_timer = QTimer(self)
        self.database_reload_timer.setSingleShot(True)
        self._reload_connection = None

        # def keyPressEvent(self, event):
        #     if event.key() == Qt.Key.Key_C and event.modifiers() & Qt.KeyboardModifier.ControlModifier:
        #         QApplication.quit()
        #     super().keyPressEvent(event)

    def x_action(self):
        sender = self.sender()
        if self.isexec:
            self.clean_up()
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

    def clean_up(self):
        if getattr(self, "is_user_abort", False):
            return
        self.is_user_abort = True
        for t_name in ['proc_timeout_timer', 'timeout_timer']:
            t = getattr(self, t_name, None)
            if t and t.isActive():
                t.stop()
        if getattr(self, 'worker', None) is not None:
            self.stop_worker_sn.emit()
        thread = getattr(self, 'worker_thread', None)
        try:
            if isinstance(thread, QThread) and thread.isRunning():
                thread.quit()
                if not thread.wait(3000):
                    self.ui.hudt.appendPlainText("Worker thread did not stop cleanly")
        except (RuntimeError, AttributeError) as e:
            logging.debug(f"clean_up couldnt close thread {e}")
            pass
        proc = getattr(self, "proc", None)
        if proc and proc.is_running():
            self.stop_proc_sn.emit()

    # on exit close threads processes and savenotes
    def closeEvent(self, event):

        if self.dirtybit:
            self.dirtybit = False
            uinpt = window_prompt(self, "Save notes", "Save note changes?", "Yes", "No")
            if uinpt:
                self.save_notes(isexit=True)  # self.save_user_data(True)

        if getattr(self, 'isexec', False):
            self.clean_up()
            for x, t in enumerate((getattr(self, 'worker_thread', None), getattr(self, 'worker2', None))):
                try:
                    if isinstance(t, QThread) and t.isRunning():
                        if x == 1:
                            t.requestInterruption()
                            t.wait(3000)
                        else:
                            logging.debug(f"closeEvent couldnt close thread {x}")
                except (RuntimeError, AttributeError) as e:
                    logging.debug(f"closeEvent couldnt close thread. {e}")
                    pass
            proc = getattr(self, 'proc', None)
            if proc:
                try:
                    if hasattr(proc, 'process') and proc.process:
                        proc.process.waitForFinished(10000)
                except (RuntimeError, AttributeError) as e:
                    logging.debug(f"closeEvent couldnt close process {e}")
                    pass

        sys.stdout = self.main_stdout
        sys.stderr = self.main_stderr
        try:
            self.logger.new_message.disconnect(self.ui.hudt.append_colored_output)
        except Exception:
            logging.error("closeEvent couldnt disconnect hudt properly")
            pass

        super().closeEvent(event)
        event.accept()

    def on_exit(self):
        self.setEnabled(True)

    def set_config(self, exit_code):

        # self.on_exit()
        try:
            toml = self.toml_file
            amt = toml.stat().st_mtime_ns
            imt = self.tomldefault_imt
            if amt != imt:
                updated_config = load_toml(toml)
                if not updated_config:
                    raise ConfigurationError

                driveTYPE_frm = updated_config['search']['driveTYPE']  # script entry
                dspEDITOR = updated_config['display']['dspEDITOR']
                popPATH = updated_config['display']['popPATH']
                cachermPATTERNS = updated_config['backend']['cachermPATTERNS']
                cachermPATTERNS = cache_clear_patterns(self.usr, cachermPATTERNS)
                email = updated_config['backend']['email']
                updated_downloads = user_path(updated_config['compress']['downloads'], self.usr)  # end script entry
                analytics = updated_config['analytics']['analytics']
                feedback = updated_config['analytics']['feedback']
                zipPATH = updated_config['compress']['zipPATH']
                zipPROGRAM = updated_config['compress']['zipPROGRAM'].lower()
                checksum = updated_config['diagnostics']['checkSUM']
                hudCOLOR = updated_config['display']['hudCOLOR']
                hudSZE = updated_config['display']['hudSZE']
                hudFNT = updated_config['display']['hudFNT']
                compLVL = updated_config['logs']['compLVL']
                moduleNAME = updated_config['paths']['moduleNAME']
                pwrshell = updated_config['search']['pwrshell']
                exclDIRS = user_path(updated_config['search']['exclDIRS'], self.usr)
                xRC = updated_config['search']['xRC']
                high_water = updated_config['search']['high_water']
                low_water = updated_config['search']['low_water']
                min_span = updated_config['search']['min_span']
                basedir = updated_config['search']['drive']
                extensions = updated_config['search']['extension']
                proteusEXTN = updated_config['shield']['proteusEXTN']
                proteusPATH = updated_config['shield']['proteusPATH']
                proteusSHIELD = updated_config['shield']['proteusSHIELD']
                psEXEC = updated_config['shield']['exec']
                dspPATH_frm = self.config['display']['dspPATH']
                new_dspPATH = updated_config['display']['dspPATH']
                # expansions
                nogo = user_path(self.config['shield']['nogo'], self.usr)
                new_nogo = user_path(updated_config['shield']['nogo'], self.usr)

                suppress_list = user_path(self.config['shield']['filterout'], self.usr)
                new_suppress_list = user_path(updated_config['shield']['filterout'], self.usr)
                # added 06/08/2026
                alarm_24h = updated_config['display']['alarm_24hr']
                alarmCOLOR = updated_config['display']['alarmCOLOR']
                alarm_soundFILE = updated_config['display']['alarm_soundFILE']
                alarm_set_soundFILE = updated_config['display']['alarm_set_soundFILE']

                # added 07/08/2026
                cmode = updated_config['calculator']['mode']
                decimals = updated_config['calculator']['decimals']
                cTHRESHOLD = updated_config['calculator']['scientific_threshold']
                ctheme = updated_config['calculator']['theme']
                chistory = updated_config['calculator']['history']
                randintMAX = updated_config['calculator']['randintMAX']
                randintMIN = updated_config['calculator']['randintMIN']
                clogLEVEL = updated_config['calculator']['logLEVEL']

                ll_level = self.config['logs']['logLEVEL']
                new_ll_level = updated_config['logs']['logLEVEL']

                log_file = self.config['logs']['userLOG']
                new_log_file = updated_config['logs']['userLOG']

                new_log = False
                if ll_level != new_ll_level:
                    new_log = True
                if log_file != new_log_file:
                    new_log = True
                if new_log:
                    new_log_file = os.path.join(self.log_dir, new_log_file)
                    _, log_path = change_logger(new_log_file, new_ll_level, process_label="mainwindow")
                    self.log_path = new_log_file
                    self.ui.hudt.appendPlainText("Log level: " + new_ll_level)
                    self.ui.hudt.appendPlainText("Log file: " + str(log_path))
                new_downloads = updated_downloads != self.downloads

                is_alarm_path = False  # a sound file was changed
                sound_file_path = None
                set_sound_file_path = None
                if self.alarm_soundFILE != alarm_soundFILE:
                    is_alarm_path = True
                    if alarm_soundFILE:
                        sound_file_path = os.path.join(self.resources, alarm_soundFILE)
                elif self.alarm_set_soundFILE != alarm_set_soundFILE:
                    is_alarm_path = True
                    if alarm_set_soundFILE:
                        set_sound_file_path = os.path.join(self.resources, alarm_set_soundFILE)

                for x in (decimals, cTHRESHOLD, randintMAX, randintMIN):
                    if not isinstance(x, int):
                        raise ConfigurationError

                if decimals != self.decimals:
                    if self.calculator:
                        if self.calculator.mode == "scientific":
                            self.calculator.decimal_set(decimals)
                c_change = False
                if ctheme != self.ctheme or chistory != self.chistory or clogLEVEL != self.clogLEVEL or cTHRESHOLD != self.cTHRESHOLD or randintMAX != self.randintMAX or randintMIN != self.randintMIN:
                    c_change = True
                # shutdown the calculator to rebuild the mode
                if cmode != self.cmode:
                    if self.calculator:
                        self.calculator.close()
                    c_change = False  # its shutdown to reset so dont do any other checks

                if zipPATH != self.zipPATH or new_downloads or popPATH != self.popPATH or is_alarm_path:
                    if not check_utility(zipPATH, updated_downloads, popPATH, sound_file_path, set_sound_file_path):
                        raise ConfigurationError

                if new_downloads:
                    self.downloads = updated_downloads
                    self.load_find_file_combo()

                if proteusPATH != self.proteusPATH or new_nogo != nogo or new_suppress_list != suppress_list:
                    if not check_config(proteusPATH, new_nogo, new_suppress_list):
                        raise ConfigurationError

                dspPATH = self.dspPATH
                if dspEDITOR != self.dspEDITOR or new_dspPATH != dspPATH_frm:
                    dspPATH = ""
                    if dspEDITOR:
                        dspEDITOR = multi_value(dspEDITOR)
                        dspEDITOR, dspPATH = resolve_editor(dspEDITOR, new_dspPATH, toml)
                        if dspEDITOR is None:
                            raise ConfigurationError

                is_pwrshell = False
                if pwrshell != self.pwrshell:
                    is_pwrshell = True

                guid = None
                driveTYPE = None
                drive_not_indexed = True
                cache_moved = False

                if basedir != self.basedir:
                    idx = "c"
                    if basedir != "C:\\":
                        idx = parse_drive(basedir)
                        guid = get_mount_partguid(basedir)
                        if not guid:
                            raise DriveLogicError(f"couldnt find guid for {basedir}")

                    ix = self.basedirs.index_by_value(guid)
                    if ix != -1:
                        _, drive_info, _ = self.basedirs.get_item(ix)
                        drive_idx = drive_info.suffix
                        moi = drive_info.moi

                        if idx == drive_idx:
                            drive_not_indexed = False
                            self.update_basedir(basedir, drive_idx, drive_info, ix)  # load the drive

                        else:
                            cache_moved = True
                            self.rmv_basedir(ix, self.basedirs.current_index)  # changed mounts

                    if drive_not_indexed:

                        cache_s, systimeche, drive_idx, driveTYPE = setup_drive_cache(
                            basedir, self.lclhome, self.dbopt, self.dbtarget, self.sj, self.toml_file, self.cache_s_str, driveTYPE,
                            self.usr, self.email, self.compLVL, j_settings=self.j_settings, partguid=guid, iqt=True
                        )
                        if not cache_s or not drive_idx or not self.j_settings:
                            raise DriveLogicError(f"Failed to build cache file for {basedir} in setup_drive_cache")

                        di = self.j_settings.get(basedir, {})
                        if not di:
                            self.ui.hudt.appendPlainText(f"the json in memory wasnt updated for the drive {basedir}")
                            raise DriveLogicError("couldnt apply changes")
                        drive_info = self.j_settings[basedir].copy()

                        if cache_moved:
                            for di in self.j_settings.values():
                                if not isinstance(di, dict):
                                    continue
                                if di.get("idx_suffix") == idx:
                                    self.ui.hudt.appendPlainText(f"drive changed mounts and wasnt properly updated check {self.sj} and set to idx_suffix for drive {basedir}, guid {guid}")
                                    # raise DriveLogicError(f"drive changed mounts and wasnt properly updated check {self.sj} and set to {drive_idx} for guid {guid}")

                        drive_guid = drive_info.get("drive_partguid")
                        moi = basedir
                        mtype = drive_info.get("model_type")
                        dtype = drive_info.get("drive_type")
                        if dtype not in ("HDD", "SSD"):
                            self.ui.hudt.appendPlainText(f"Warning malformed entry for {drive_idx} drive {basedir} in {self.sj}. defaulting to HDD")
                            dtype = "HDD"
                        psEXTN = drive_info.get("proteusEXTN")

                        drive = BasedirDrive(drive_idx, drive_guid, moi, mtype, dtype, cache_s, systimeche, psEXTN)
                        self.add_basedir(basedir, drive_idx, drive_guid, drive, drive_info)

                if hudCOLOR != self.hudCOLOR or hudSZE != self.hudSZE or hudFNT != self.hudFNT:
                    self.hudCOLOR = hudCOLOR
                    self.hudSZE = hudSZE
                    self.hudFNT = hudFNT
                    self.change_format(True)

                alarm_changed = (alarm_24h != self.alarm_24h or alarmCOLOR != self.alarmCOLOR)

                if alarm_changed:
                    self.alarm_24h = alarm_24h
                    self.alarmCOLOR = alarmCOLOR
                    self.change_clock()

                if is_alarm_path:
                    self.alarm_soundFILE = alarm_soundFILE
                    self.alarm_set_soundFILE = alarm_set_soundFILE

                    self.ui.widget.sound_file = sound_file_path
                    self.ui.widget.sound_set_file = set_sound_file_path
                    self.ui.widget.validate_sounds()

                if c_change:
                    if self.calculator:
                        self.calculator.theme = ctheme
                        self.calculator.set_format(ctheme)
                        self.calculator.history_view = chistory
                        self.calculator.log_level = clogLEVEL
                        if 0 <= cTHRESHOLD <= 20:
                            self.calculator.SCI_THRESHOLD = cTHRESHOLD
                        self.calculator.rand_max = randintMAX
                        self.calculator.rand_min = randintMIN

                if extensions != self.extensions:
                    fill_extensions(self.ui.combffile, extensions, prev_extensions=self.user_extensions)

                if not driveTYPE:
                    if driveTYPE_frm != self.driveTYPE:
                        if driveTYPE_frm in ("HDD", "SSD"):

                            _, drive, info = self.basedirs.get_current_item()

                            extn = drive.psextn
                            self.basedirs.update_current_item(extn, driveTYPE_frm, drive_type=driveTYPE_frm)

                            self.driveTYPE = driveTYPE_frm
                            update_j_settings({"drive_type": self.driveTYPE}, self.j_settings, self.basedir, self.sj)  # whatever the user changed update the json
                        else:
                            self.ui.hudt.appendPlainText(f"Incorrect setting for driveTYPE: {driveTYPE_frm} restoring current value.")
                            update_toml_values({'search': {'driveTYPE': self.driveTYPE}}, self.toml_file)

                self.dspEDITOR = dspEDITOR
                self.dspPATH = dspPATH
                self.popPATH = popPATH
                self.cachermPATTERNS = cachermPATTERNS
                self.email = email
                self.analytics = analytics
                self.feedback = feedback
                self.compLVL = compLVL
                self.moduleNAME = moduleNAME

                if is_pwrshell:
                    self.pwrshell = pwrshell

                self.proteusEXTN = ["[no extension]" if p == "" else p for p in proteusEXTN]
                self.proteusPATH = proteusPATH
                self.checksum = checksum
                self.proteusSHIELD = proteusSHIELD
                self.psEXEC = psEXEC
                self.exclDIRS = exclDIRS
                self.xRC = xRC
                self.high_water = high_water
                self.low_water = low_water
                self.min_span = min_span
                self.zipPROGRAM = zipPROGRAM
                self.zipPATH = zipPATH
                self.extensions = extensions

                self.cmode = cmode
                self.decimals = decimals
                self.cTHRESHOLD = cTHRESHOLD
                self.ctheme = ctheme
                self.chistory = chistory
                self.randintMAX = randintMAX
                self.randintMIN = randintMIN
                self.clogLEVEL = clogLEVEL

                # config_changed = (self.config != updated_config)
                # if config_changed:
                self.ui.hudt.appendPlainText("Config changed")   # ctext = cprint.cyan("Config changed") # self.ui.hudt.append_colored_output("\033[1;32mConfig changed\033[0m")  # green

        except ConfigurationError:
            dump_toml(None, self.config, toml)
        except DriveLogicError as e:
            dump_toml(None, self.config, toml)
            self.ui.hudt.appendPlainText(f"{type(e).__name__} {str(e)}")
            self.ui.hudt.appendPlainText("")
        except Exception as e:
            dump_toml(None, self.config, toml)
            if e:
                self.ui.hudt.appendPlainText(
                    f"original settings restored. A backup of initial config was made to {self.tomldefault}. {e} {type(e).__name__}"
                    f"\n error logged {self.lclhome}\\logs"
                )
            logging.error("couldnt change configs", exc_info=True)

        if exit_code != 0:
            self.ui.hudt.appendPlainText(f"Editor exited with exit {exit_code}")

        self.config = None
        self.tomldefault_imt = None
        self.isexec = False
        self.is_user_edit = False

    def edit_config(self):
        if not (self.dspEDITOR and self.dspPATH) or self.is_user_edit:
            return

        toml = self.toml_file
        if os.path.isfile(toml):
            if not self.job_running(True):
                return
            self.is_user_edit = True
            self.config = load_toml(toml)
            if not self.config:
                self.ui.hudt.appendPlainText("failed to store original config unable to continue")
                self.isexec = False
                self.is_user_edit = False
                return

            self.tomldefault_imt = toml.stat().st_mtime_ns
            shutil.copy2(self.toml_file, self.tomldefault)

            # send stdout stderr to hudt from QProcess
            # def handle_stdout(self):
            #     data = self.proc.readAllStandardOutput().data().decode()
            #     self.logger.write(data)
            # def handle_stderr(self):
            #     data = self.proc.readAllStandardError().data().decode()
            #     self.logger.write(data)
            # end stdout stdderr
            # see handle_stdout stderr for QProcess
            # #self.proc = QProcess(self)
            # #self.result = None
            # #self.exit_result = None
            # #self.proc.readyReadStandardOutput.connect(self.handle_stdout)
            # #self.proc.readyReadStandardError.connect(self.handle_stderr)
            # # self.proc_timeout_timer.start(45000)
            # #self.proc.finished.connect(self.shut_proc)
            # #self.proc.finished.connect(self.cleanup_proc)
            # #self.proc.finished.connect(self.on_exit)
            # #self.proc.start(self.dspEDITOR, [str(self.toml_file)])   # subprocess.run([self.dspPATH, toml], capture_output=True, check=True, text=True)

            self.setEnabled(False)
            self.proc = ProcessHandler()  # self.lclhome, self.xdg_runtime, self.ui.dbmainlabel.text(), self.is_polkit
            self.open_proc()
            self.proc.complete.connect(lambda code, _: self.update_ui_sn.emit(code, "editor"))
            self.proc.complete.connect(lambda code, status: QTimer.singleShot(3000, lambda: self.set_config(code)))
            self.proc.start_tomledit(self.dspPATH, [str(self.toml_file)])

        else:
            print(f"{self.dspEDITOR} no such config file: {toml}")

    def postop(self, code, exit_status, show_diff):
        if code == 0:
            if self.proc is not None:
                QTimer.singleShot(
                    500,
                    lambda: self.postop(code, exit_status)
                )
                return
            self.isexec = True
            self.run_scan_idx(show_diff)

    # overview of configuration also debug generalized
    def show_status(self):

        ps = False  # check if profile made
        if table_loaded(self.dbopt, self.sys_a, self.ui.hudt):
            ps = True

        psEXTN = self.j_settings.get(self.suffix, {}).get("proteusEXTN")

        search_count = 0
        unique_files = 0
        lifetime_throughput = 0
        total_scans = 0
        conn = sqlite3.connect(self.dbopt)
        cur = conn.cursor()

        search_count = blank_count(cur)
        if search_count and search_count > 0:
            unique_files = get_unique_files(cur)
            lifetime_throughput = get_lifetime_throughput(cur)
        if ps and psEXTN:
            # cur.execute("SELECT COUNT(*) FROM scans")
            cur.execute("""
                SELECT COUNT(DISTINCT e.scan_id)
                FROM scan_entries e
                WHERE e.basedir = ?
            """, (self.basedir,))
            total_scans = cur.fetchone()[0]
        cur.close()
        conn.close()

        stat_value = {}

        stat_value['Exhibit2'] = "Pwrshell" if self.pwrshell else "os.scandir"
        stat_value['Exhibit'] = self.distro_name

        drive_id = self.j_settings[self.basedir].get("drive_id_model")
        model_type = self.j_settings[self.basedir].get("model_type")
        drive_type = self.j_settings[self.basedir].get("drive_type")

        drive_model = None
        if not drive_id:
            # drive_id = "Unknown"
            drive_info = current_drive_type_model_check(self.basedir)
            if drive_info:
                drive_name, drive_model, _ = drive_info
                if drive_name:
                    drive_id = drive_name
                    update_j_settings({"drive_id_model": drive_id, "model_type": drive_model}, self.j_settings, self.basedir, self.sj)

        if not model_type and drive_model:
            # model_type = "Unknown"
            model_type = drive_model

        if self.calculator:
            print("yes")

        typeModel = f"{drive_id} / {model_type}"
        # log_path = filename_of_handler()
        stat_value.update({
            "Drive or basedir:": self.basedir,
            "Drive name/Type": typeModel,
            "Drive type": drive_type,
            "Empty1":  "",
            "xRC": self.xRC,
            "Proteus Shield active": str(ps),
            "Checksum and Caching": "y" if self.checksum else "n",
            "Empty2a":  "",
            "Empty2":  "",
            "Database": self.dbopt,
            "Last table": self.table,
            "Logfile":  self.log_path,
            "Debug line1": f"self.db is {self.db}",  # debuger
            "Debug line2": f'worker is {"active" if self.worker else ""}'
        })

        hudt = self.ui.hudt.appendPlainText

        # self.ui.hudt.clear()

        for key, value in stat_value.items():
            if not key.startswith("Debug") and not value:
                hudt('')
            elif key == "Exhibit":
                hudt(value)
            elif key == "Exhibit2":
                hudt(value)
            else:
                hudt(f"{key} {value}")

        if self.result is not None:
            hudt(f"Last Return: {self.result}")
            if self.exit_result != -1:
                hudt("QProcess")
                hudt(f"QExitStatus: {self.exit_result}")
            else:
                hudt("Thread")

        if ps:
            if psEXTN:
                hudt('\n')
                is_exec_profile = ps_profile_type(psEXTN)
                extn = profile_to_str(psEXTN, is_exec_profile)
                hudt(f"proteusTYPE: {'exec' if is_exec_profile else 'extn'}")
                hudt("proteusEXTN: " + extn)
                if total_scans:
                    hudt(f"Total scans: {total_scans}")

        if search_count:
            hudt('')
            hudt(f"Searches: {search_count}")
            if lifetime_throughput:
                hudt(f'Lifetime throughput: {lifetime_throughput:.3f} files per second')
            if unique_files:
                hudt("")
                hudt(f"Total unique files in logs: {unique_files}")
        # """ stored drive values """
        # hudt('\n')
        # hudt("mem")

        # guid, drive, keys = self.basedirs.get_current_item()
        # hudt(drive.suffix)
        # hudt(f"drive_partguid: {drive.part_guid}")
        # hudt(f"mount_of_index: {drive.moi}")
        # hudt(f"drive_type: {drive.drive_type}")
        # hudt(drive.cache_s)
        # hudt(drive.systimeche)
        # if drive.psextn:
        #     extn = ', '.join(drive.psextn)
        #     hudt(f"proteusEXTN {extn}")
        # else:
        #     hudt("proteusEXTN: None")
        # """ stored drive info values """
        # hudt("\n")
        # hudt("spinner")

        # for key, v in keys.items():
        #     hudt(f'{key}: {v}')
        # print(f'\nindex: {self.basedirs.current_index}\n')

    def set_dirtybit(self):
        self.dirtybit = True

    def save_notes_history(self, isexit=False):
        notes = self.ui.textEdit.toPlainText()
        self.saved_history = get_history_view(self.saved_history, self.calculator)
        nc = cnc(self.dbopt, self.compLVL)
        user_data_to_database(notes, self.saved_history, self.ui.hudt, self.dbopt, self.dbtarget, self.email, nc, isexit=isexit, parent=self)
        if self.saved_history:
            self.ui.actionClear_history.setEnabled(True)

    def save_user_data(self, isexit=False):

        self.save_notes_history(isexit)

        last_drive = self.ui.combd.currentText()  # what drive is loaded for find downloads combo box on app start
        sr = self.ui.stime.value()  # search time
        ffr = self.ui.sffile.value()  # find file time
        sout_put = self.ui.combftimeout.currentText()  # search destination or output
        ntout_put = self.ui.combt.currentText()  # newer than file destination or output
        compout_put = self.ui.combffileout.currentText()  # compress destination or output
        aw = self.ui.widget
        hour, minute = aw.get_alarm_time()
        alarm_time = f"{hour:02d}:{minute:02d}"
        update_data = {
            "last_drive": last_drive,
            "search_range": sr,
            "find_range": ffr,
            "search_output": sout_put,
            "compress_output": compout_put,
            "newer_output": ntout_put,
            "newer_path": str(self.nt_path).replace('/', '\\'),
            "alarm_time": alarm_time
        }

        # remove from json file as they are default
        if aw.alarm_hour == 7 and aw.alarm_minute == 0:
            update_data["alarm_time"] = None
        if self.nt_path == str(self.lclhome):
            update_data["newer_path"] = None
        if sr == 0:
            update_data["search_range"] = None
        if ffr == 0:
            update_data["find_range"] = None

        update_j_settings(update_data, self.j_settings, None, self.sj)

        # if it didnt change then there is no need to save to toml
        b = self.basedir
        if b != self.oldbasedir:
            kp = {
                "search": {"drive": self.basedir, "driveTYPE": self.driveTYPE}
            }
            update_toml_values(kp, self.toml_file)
            self.oldbasedir = b

        self.lastdrive = last_drive

        self.dirtybit = False

    def load_user_data(self, is_startup=False):

        self.ui.textEdit.blockSignals(True)
        self.user_extensions, self.saved_history = user_data_from_database(self.ui.hudt, self.ui.textEdit, self.ui.combffile, self.extensions, self.dbopt, is_startup, self)
        if self.saved_history:
            self.ui.actionClear_history.setEnabled(True)
        self.ui.textEdit.blockSignals(False)

    def open_calculator(self):
        if self.calculator is None:
            self.calculator = SCalculator(None, self.cmode, self.cTHRESHOLD, self.decimals, self.ctheme, self.chistory,
                                          self.saved_history, self.randintMAX, self.randintMIN, self.ui.hudt,
                                          self.clogLEVEL)
            self.calculator.complete.connect(self.on_calc_closed)
        self.calculator.show()
        self.calculator.raise_()
        self.calculator.activateWindow()

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

    def reset_settings(self, retry=0, max_ret=5):
        if self.isexec:
            if retry < max_ret:
                QTimer.singleShot(100, lambda: self.reset_settings(retry + 1, max_ret))
            return
        self.ui.progressBAR.setValue(0)
        self.ui.dbprogressBAR.setValue(0)

        self.ui.combftimeout.setCurrentIndex(0)  # output
        rng = self.j_settings.get("search_range", 0)
        if rng != 0:
            self.ui.stime.setValue(self.ui.stime.minimum())  # top search time
        rng = self.j_settings.get("find_range", 0)
        if rng != 0:
            self.ui.sffile.setValue(self.ui.sffile.minimum())  # find file time

        self.ui.combt.setCurrentIndex(0)  # ntfilter

        self.ui.ntlineEDIT.clear()
        self.ui.ffilet.clear()
        self.ui.hudt.clear()

        self.ui.diffchka.setChecked(False)
        self.ui.diffchkb.setChecked(False)
        self.ui.diffchkc.setChecked(False)

        # pg_2
        self.ui.dbchka.setChecked(False)
        # disable signals
        # self.ui.combd.setCurrentIndex(0) # signals
        # end pg_2

        self.mft = None
        self.ui.mftrange.setValue(24)
        self.ui.mftchka.setChecked(False)
        self.ui.mftchkb.setDisabled(False)
        self.ui.mftchkb.setChecked(False)
        self.resetMftImport()

        # combo
        self.ui.combffile.setCurrentIndex(0)  # extension

        ix = 0
        if self.downloads.strip():
            ix = self.ui.combffileout.findText(self.downloads)
        if ix != -1:
            self.ui.combffileout.setCurrentIndex(ix)

        # self.ui.combd.setCurrentIndex(0)
        last_drive = self.ui.combd.currentText()
        if last_drive != self.lastdrive:
            idx = self.ui.combd.findText(self.lastdrive)
            if idx != -1:
                self.ui.combd.setCurrentIndex(idx)

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

    """ alarm """
    def copy_sound(self, sound_file):
        new_sound_file = False
        filename = os.path.basename(sound_file)
        dest = os.path.join(self.resources, filename)
        # print(os.path.dirname(sound_file))
        # print(str(self.resources))
        # no need to copy the file to the same location
        if Path(os.path.dirname(sound_file)) != self.resources:
            new_sound_file = True
            shutil.copy(sound_file, dest)
        return dest, new_sound_file

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
            window_message(self, f"Image size must be 250x333 or less.\n\nSelected image: {w}x{h}", "Invalid size")
            return False
        return True

    def load_jpg(self):

        def reset_default(default_path, default_file, target):
            defaultflnm = os.path.join(default_path, default_file)
            shutil.copy(defaultflnm, target)
        # copy .bak to .png

        def select_custom():

            title = "Choose an Option"
            msg = "Please select an option:"

            dlg = QDialog(self)
            dlg.setWindowTitle(title)

            layout = QVBoxLayout(dlg)

            label = QLabel(msg)
            layout.addWidget(label)

            # Jpg
            row1 = QHBoxLayout()
            btn_importjpg = QPushButton("Jpg")
            btn_resetjpg = QPushButton("Reset")
            # Alarm One
            btn_alarmone = QPushButton("Alarm set")
            btn_resetalarmone = QPushButton("Reset")

            # row1.addStretch()  # original
            row1.addWidget(btn_importjpg)
            row1.addWidget(btn_resetjpg)
            row1.addWidget(btn_alarmone)
            row1.addWidget(btn_resetalarmone)
            # row1.addStretch()  # .
            layout.addLayout(row1)

            # Crest
            row2 = QHBoxLayout()
            btn_importcrest = QPushButton("Crest")
            btn_resetcrest = QPushButton("Reset")
            # Alarm Two
            btn_alarmtwo = QPushButton("Alarm sound")
            btn_resetalarmtwo = QPushButton("Reset")

            # row2.addStretch()  # .
            row2.addWidget(btn_importcrest)
            row2.addWidget(btn_resetcrest)
            row2.addWidget(btn_alarmtwo)
            row2.addWidget(btn_resetalarmtwo)
            # row2.addStretch()  # .
            layout.addLayout(row2)

            row3 = QHBoxLayout()
            btn_embosscrest = QPushButton("Emboss Crest")
            # row3.addStretch()  # .
            row3.addWidget(btn_embosscrest)
            # row3.addStretch()  # .
            layout.addLayout(row3)

            result = {"value": None}
            buttons = [btn_importjpg, btn_resetjpg, btn_alarmone, btn_resetalarmone, btn_importcrest, btn_resetcrest, btn_alarmtwo, btn_resetalarmtwo, btn_embosscrest]
            max_w = max(b.sizeHint().width() for b in buttons)
            max_h = max(b.sizeHint().height() for b in buttons)

            for b in buttons:
                b.setFixedSize(max_w, max_h)

            # Connect buttons to results
            btn_importjpg.clicked.connect(lambda: (result.update(value="jpg"), dlg.accept()))
            btn_resetjpg.clicked.connect(lambda: (result.update(value="defjpg"), dlg.accept()))
            btn_alarmone.clicked.connect(lambda: (result.update(value="alarmone"), dlg.accept()))
            btn_resetalarmone.clicked.connect(lambda: (result.update(value="defalarmone"), dlg.accept()))
            btn_importcrest.clicked.connect(lambda: (result.update(value="crest"), dlg.accept()))
            btn_resetcrest.clicked.connect(lambda: (result.update(value="defcrest"), dlg.accept()))
            btn_alarmtwo.clicked.connect(lambda: (result.update(value="alarmtwo"), dlg.accept()))
            btn_resetalarmtwo.clicked.connect(lambda: (result.update(value="defalarmtwo"), dlg.accept()))
            btn_embosscrest.clicked.connect(lambda: (result.update(value="emboss"), dlg.accept()))

            dlg.exec()
            return result["value"]

        res = select_custom()

        # Added 06/09/2026
        if not self.new_sound_file:
            # Added 06/09/2026
            # alarm_set_soundFILE
            if res == "alarmone":
                set_sound = self.open_file_dialog(self.resources)
                if set_sound:
                    ext = os.path.splitext(set_sound)[1].lower()

                    if ext in (".wav", ".mp3", ".ogg"):
                        self.lastdir = Path(os.path.dirname(set_sound))
                        is_set_sound = True

                        # copy the file to .\\Resources\\
                        dest, self.new_sound_file = self.copy_sound(set_sound)

                        # will send a signal if valid sound file and then ok to save filename to .toml
                        # if its not a valid sound file and self.new_sound_file is true delete it as it was copied
                        # to .\\Resources\\
                        self.ui.widget.change_alarm_sound(dest, is_set_sound)
                    else:
                        print(f"Not a valid sound file supported .wav .mp3 .ogg given: {ext}")

            elif res == "defalarmone":
                if self.alarm_set_soundFILE:
                    self.lastdir = None
                    update_toml_values({'display': {'alarm_set_soundFILE': ""}}, self.toml_file)

                    self.alarm_set_soundFILE = None
                    self.ui.widget.sound_set_file = None

            # Added 06/09/2026
            # alarm_soundFILE
            elif res == "alarmtwo":
                alarm_sound = self.open_file_dialog(self.resources)
                if alarm_sound:
                    ext = os.path.splitext(alarm_sound)[1].lower()

                    if ext in (".wav", ".mp3", ".ogg"):
                        self.lastdir = Path(os.path.dirname(alarm_sound))
                        is_set_sound = False

                        dest, self.new_sound_file = self.copy_sound(alarm_sound)

                        self.ui.widget.change_alarm_sound(dest, is_set_sound)
                    else:
                        print(f"Not a valid sound file supported .wav .mp3 .ogg given: {ext}")

            elif res == "defalarmtwo":
                if self.alarm_soundFILE:
                    self.lastdir = None
                    update_toml_values({'display': {'alarm_soundFILE': ""}}, self.toml_file)

                    self.alarm_soundFILE = None
                    self.ui.widget.sound_file = None
                    self.ui.widget.valid_sound = False

            # other logic jpg crest ect
            elif res == "jpg":
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
    def load_last_drive(self, last_drive=None):

        def set_drive(target):
            idx = self.ui.combd.findText(target)
            if idx != -1:
                self.ui.combd.setCurrentIndex(idx)
                self.lastdrive = target
                return True
            return False

        if last_drive:
            return set_drive(last_drive)

        drive = self.j_settings.get("last_drive")

        if not drive or not set_drive(drive):
            self.lastdrive = self.basedir
            self.ui.combd.setCurrentIndex(0)
            update_j_settings({"last_drive": self.basedir}, self.j_settings, None, self.sj)

    # download button combo box
    # return drive cache glob and default name
    def get_cache_pattern(self):
        systimename = name_of(self.cache_s_str)
        systime_pattern = systimename + "*"  # + "_*"

        pattern = os.path.join(self.lclhome, systime_pattern)
        return pattern, systimename

    # values
    def load_saved_indexes(self):
        pattern, systimename = self.get_cache_pattern()

        filepath = glob.glob(pattern)

        a_drives = []

        current_base = self.basedir
        current_suffix = self.suffix

        a_drives.append((current_base, current_suffix))

        for path in filepath:
            fname = name_of(path)  # normalize

            is_base_drive = fname == systimename  # case where the basedir is not /
            if is_base_drive:
                if current_base != "C:\\":
                    a_drives.append(("C:\\", "c"))
                continue
            drive_letter = fname.split("_", 1)[-1]
            if drive_letter != fname and drive_letter != current_suffix:  # our basedir was already added first
                drive_name = drive_letter.upper() + ":\\"
                if os.path.exists(drive_name):
                    a_drives.append((drive_name, drive_letter))
        return a_drives, systimename

    def fill_download_combo(self, drives):  # fill download combo box values
        combo = self.ui.combd
        combo.clear()
        for drive, letter in drives:
            combo.addItem(drive, userData=(drive, letter))
        # combo.addItems(drives)
        combo.setCurrentIndex(0)

    def load_drives(self):
        a_drives, systimename = self.load_saved_indexes()
        self.fill_download_combo(a_drives)
        self.load_last_drive()
        return a_drives, systimename
    # end download button combo box

    def show_history(self):
        """ actionHistoryv show the calculator history on hudt display """

        self.saved_history = get_history_view(self.saved_history, self.calculator)
        if self.saved_history:
            self.ui.hudt.clear()
            self.ui.hudt.appendPlainText(self.saved_history)

    def clear_history(self):
        """ menubar actionClear_history clear history column from extn table """
        if not self.job_running(True):
            return
        if clear_from_extn_tbl(self.dbopt, False, False):
            self.saved_history = ""
            encr(self.dbopt, self.dbtarget, self.email, no_compression=self.nc, dcr=True)
            self.ui.actionClear_history.setEnabled(False)
        self.isexec = False

    def clear_extensions(self):
        """ menubar actionClear_extensions clear all rows other than id 1 from extn tbl """
        if not self.job_running(True):
            return
        if clear_from_extn_tbl(self.dbopt, True, False):
            self.user_extensions = []
            if encr(self.dbopt, self.dbtarget, self.email, no_compression=self.nc, dcr=True):
                fill_extensions(self.ui.combffile, self.extensions)
        self.isexec = False

    def new_extension(self):
        if not self.job_running(True):
            return
        new_extension = add_new_extension(self.extensions, self.ui.hudt, self.ui.combffile, self.dbopt, self.dbtarget, self.email, self.nc, parent=self)
        if new_extension:
            self.user_extensions.append(new_extension)
        self.isexec = False

    def tableview_loaded(self):
        mdl = self.ui.tableView.model()
        if isinstance(mdl, QSortFilterProxyModel):
            mdl = mdl.sourceModel()
        return bool(self.db and mdl and mdl.rowCount())

    def init_page2(self):
        if self.tableview_loaded():  # self.db (first load) and has model rows
            return True
        else:
            if not os.path.isfile(self.dbopt):
                if not load_gpg(self.dbopt, self.dbtarget, self.ui.dbmainlabel):
                    return False
            return self.display_db('logs', False, False)

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

    def open_proc(self, timeout=0):
        self.ui.progressBAR.setValue(0)
        self.result = None
        self.exit_result = None

        if timeout:
            self.proc_timeout_timer.start(timeout)

        self.proc.progress.connect(self.increment_progress)
        self.proc.log.connect(self.append_log)  # self.append_colored_output
        self.proc.error.connect(self.append_log)
        self.stop_proc_sn.connect(self.proc.stop)
        self.proc.complete.connect(self.shut_proc)
        self.proc.complete.connect(self.cleanup_proc)
        self.isexec = True
        self.ui.sbasediridx.setEnabled(False)

    def proc_dbui(self):
        self.ui.dbprogressBAR.setValue(0)
        self.proc.progress.connect(self.increment_db_progress)
        self.proc.status.connect(self.update_db_status)

    @Slot(int, int)
    def shut_proc(self, exit_code, exit_status):
        if exit_code != 4:
            if self.proc_timeout_timer.isActive():
                self.proc_timeout_sn.emit()

        self.ui.sbasediridx.setEnabled(True)
        self.isexec = False

        self.is_user_abort = False

        self.result = exit_code
        self.exit_result = exit_status

        if exit_code != 0:  # and not exit_status != QProcess.NormalExit:
            exit_str = str(exit_status)
            if exit_code == 7:
                self.ui.hudt.appendPlainText(f"QProcess replied to exit request Exit status: {exit_str}")
            else:
                self.ui.hudt.appendPlainText(f"Exit code: {exit_code}, Exit status: {exit_str}")
        self.ui.resetButton.setEnabled(True)

    def proc_timeout(self):
        if getattr(self, "proc", None) and self.proc.is_running():

            self.ui.hudt.appendPlainText("Requesting process stop due to timeout...")
            self.stop_proc_sn.emit()

    #
    # End Process

    ''' Main search recentchangessearch.py'''

    # top search
    def search(self, output, thetime, argf):
        if not self.job_running():
            return

        method = ""
        srcDIR = "noarguser"

        # integrate srcDIR for future expansion. <---
        #
        #

        if output == "AppData":
            argone = thetime
            thetime = srcDIR
            method = "rnt"
        else:
            argone = "search"

        scanidx = self.ui.diffchkb.isChecked()
        postop = self.ui.diffchka.checkState() == Qt.CheckState.Checked
        showDiff = self.ui.diffchkc.isChecked()

        if postop:
            doctrine = os.path.join(self.usrDIR, "doctrine.tsv")
            if os.path.exists(doctrine):
                self.ui.hudt.appendPlainText("A file doctrine already exists skipping")

        self.proc = ProcessHandler()
        self.open_proc(360000)

        ismcore = True
        self.proc.set_mcore(ismcore)  # uses multicore dont cancel while those processes are running   between 21 - 59 % and 66 and 89%

        if postop or scanidx:
            self.proc.complete.connect(lambda code, _: self.update_ui_sn.emit(code, "search"))

        if scanidx:
            self.proc.complete.connect(lambda code, exit_status, showDiff=showDiff: self.postop(code, exit_status, showDiff))

        args = [
            "recentchangessearch.py",
            str(argone),
            str(thetime),
            str(self.usr),
            str(self.pwd),
            str(argf),
            str(method),
            "True",
            str(self.basedir),
            str(self.driveTYPE),
            str(self.dbopt),
            str(self.cache_s),
            str(postop),
            str(self.dspPATH)
        ]
        is_search = True
        if not self.pwrshell:
            is_search = False

        if not self.is_pyinstall:
            args = ["-u", self.app] + args

        self.proc.start_pyprocess(str(self.dispatch), args, dbtarget=self.dbtarget, is_search=is_search, is_postop=postop, is_scanIDX=scanidx)  # self.myapp

    # fork 5 minutebtns ftimeb ftimebf timebtns stimeb stimebf
    # 5 Min, 5 Min Filtered, Search by time and . Filtered
    def tsearch(self, clicked_button, filtered=None):

        argf = ""
        thetime = "noarguser"

        output = self.ui.combftimeout.currentText()  # AppData or Desktop
        if filtered:
            argf = "filtered"

        if clicked_button == self.ui.stimebf or clicked_button == self.ui.stimeb:
            thetime = self.ui.stime.value()
            if thetime == 0:
                self.ui.hudt.appendPlainText("Time cant be 0.")
                return

        self.search(output, thetime, argf)

    # fork
    def ntsearch(self):

        clicked_button = self.sender()
        if clicked_button == self.ui.ntbrowseb:
            fpath = self.open_file_dialog(self.nt_path)

            if fpath:
                self.nt_path = os.path.dirname(fpath)
                self.ui.ntlineEDIT.setText(fpath)
            return
        elif clicked_button == self.ui.ntbrowseb2:

            fpath = QFileDialog.getExistingDirectory(
                self,
                "Select a folder",
                self.nt_path
            )

            if fpath:
                self.nt_path = os.path.dirname(fpath)
                self.ui.ntlineEDIT.setText(fpath)
            return

        fpath = self.ui.ntlineEDIT.text().strip()
        if not fpath:
            window_message(self, "Browse to select a file.", "No target")
            return
        elif not os.path.exists(fpath):
            window_message(self, "please enter valid filename.", "NSF")
            return

        output = "search"
        thetime = fpath

        argf = self.ui.combt.currentText()
        if argf == "Filtered":
            argf = ""
        else:
            argf = "filtered"

        self.search(output, thetime, argf)

    # Find file
    def ffile(self, compress, time_range=0):
        if not self.job_running():
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
            window_message(self, "please enter a filename and or extension")
            return

        self.proc = ProcessHandler()
        self.open_proc(360000)

        if compress:
            downloads = self.ui.combffileout.currentText()
            if downloads == "Desktop":
                downloads = self.usrDIR
            elif downloads == "AppData":
                downloads = self.lclhome
            else:
                downloads = downloads.strip()

            self.proc.set_compress(self.zipPROGRAM, self.zipPATH, str(self.usrDIR), downloads)  # compress button?
        else:
            range_value = self.ui.sffile.value()
            if range_value:
                try:
                    range_float = time_convert(range_value, 60, 2)
                except ValueError:
                    self.ui.hudt.appendPlainText("Invalid number")
                    return
                if range_float:
                    time_range = range_float
                    # elif ok:
                    #     self.ui.hudt.appendPlainText("specify 0 to compress all results. or enter a time range to compress")

        action = "pwsh"
        if not self.pwrshell:
            action = "python"

        self.proc.set_range(str(time_range))
        args = [
            "findfile.py",
            str(self.lclhome),
            action,
            fpath,
            extension,
            self.basedir,
            self.usr,
            self.dspEDITOR,
            self.dspPATH,
            self.tempdir
        ]

        if not self.is_pyinstall:
            args = ["-u", self.app] + args
        self.proc.start_pyprocess(str(self.dispatch), args, dbtarget=self.dbtarget)

    # compress
    def ffcompress(self):
        fpath = self.ui.ffilet.text().strip()
        extension = self.ui.combffile.currentText()
        if not (fpath or extension):
            return

        zip_pth = None
        zipPROGRAM = self.zipPROGRAM
        if not zipPROGRAM:
            zipPROGRAM = "zipfile"
            self.zipPROGRAM = zipPROGRAM
            # self.ui.hudt.appendPlainText("No zipPROGRAM specified")
            # return

        if not self.zipPATH:
            if zipPROGRAM != "zipfile":
                if zipPROGRAM == "winrar":
                    zip_pth = check_installed_app("WinRAR.exe", "WinRAR")
                    if not zip_pth:
                        self.ui.hudt.appendPlainText("Failed to find winrar exe path")
                elif zipPROGRAM == "7zip":
                    zip_pth = check_installed_app("7zFM.exe", "7-Zip")
                    # if not zip_pth:
                    #     zip_pth = self.defaultzipPATH  # 05/31/2026 7-zip not included anymore
                    if zip_pth:
                        seven_path_frm = os.path.dirname(zip_pth)
                        seven_path = os.path.join(seven_path_frm, "7z.exe")
                        if os.path.isfile(seven_path):
                            zip_pth = seven_path
                        else:
                            self.ui.hudt.appendPlainText(f"Failed to find 7z command-line 7z.exe beside 7zFM.exe path: {seven_path_frm}")
                            # self.ui.hudt.appendPlainText("Defaulting to app 7-zip")
                            # zip_pth = self.defaultzipPATH

                if zip_pth:
                    self.zipPATH = zip_pth
                else:
                    window_message(self, "No zipPATH specified and failed to find a path", "Info")
                    return

        time_range = self.ui.sffile.value()
        # range_value
        # if range_value:
        #     time_range = range_value
        # else:
        # time_range, ok = window_input(self, "Enter search time", "Seconds:")
        # if ok and time_range:
        # elif ok:
        #     self.ui.hudt.appendPlainText("specify 0 to compress all results. or enter a time range to compress")
        #     return
        try:
            range_float = time_convert(time_range, 60, 2)
            if range_float == 0:
                uinpt = window_prompt(self, "Compress archive", "You have entered 0. This will compress all file matches. Continue", "Yes", "No")
                if not uinpt:
                    return
            self.ffile(True, range_float)
        except ValueError:
            self.ui.hudt.appendPlainText("Invalid number")
            return
    #
    # End Find file

    def get_ramdrive(self, basedir, cmsg):
        if not isinstance(cmsg, list):
            self.ui.hudt.appendPlainText(f"error get_ramdrive cmsg {cmsg} is not a list")
            return None
        # filter_values = [self.ui.combd.itemText(i) for i in range(self.ui.combd.count())]
        dialog = DriveSelectorDialog(self.basedir, self.j_settings, parent=self)  # , filter_out=filter_values
        if dialog.exec():
            target = dialog.selected_drive()
            if os.path.exists(target):
                self.ramdisk = target
                return target
            else:
                self.ramdisk = None  # no default for index drives applies to Mft
                uinpt = window_prompt(self, *cmsg)
                #
                if uinpt:
                    return basedir
        return None

    def get_newdrive(self):

        filter_values = [self.ui.combd.itemText(i) for i in range(self.ui.combd.count())]
        dialog = DriveSelectorDialog(self.basedir, self.j_settings, idx_drive=True, filter_out=filter_values, parent=self)

        if dialog.exec():
            target, guid = dialog.selected_idx_drive()
            if os.path.exists(target):
                return target, guid
            else:
                window_message(self, f"selected {target} not found.")
        return None

    ''' Proteus Shield / System Profile '''

    # Main db task
    # Set hardlinks
    # also contains Index drive aka Find Downloads
    # System profile / Proteus Shield
    # page_2
    # fork set hardlinks button

    def set_hardlinks(self):
        if not self.job_running(True):
            return
        if not table_loaded(self.dbopt, 'logs', self.ui.hudt) or not has_log_data(self.dbopt, self.ui.hudt, parent=self):
            self.isexec = False
            return

        self.proc = ProcessHandler()
        self.open_proc(120000)
        self.proc_dbui()  # label/pbar
        self.proc.status.connect(self.update_db_status)  # reset label

        self.proc.complete.connect(lambda code, _: self.reload_database_sn.emit(code, False, ("logs",)))

        args = [
            'dirwalker.py',
            'hardlink',
            str(self.lclhome),
            self.dbopt,
            self.dbtarget,
            self.basedir,
            self.usr,
            self.tempdir,
            self.email,
            str(self.compLVL)
        ]

        if not self.is_pyinstall:
            args = ["-u", self.app] + args
        self.proc.start_pyprocess(str(self.dispatch), args, database=self.dbopt, dbtarget=self.dbtarget, status_message="Set hardlinks")

    # Build IDX &
    # Drive index
    #
    # Moved here as database integrated. central hub for core feature system index.   QProcess start logic above. Thread start logic below

    # Main Scan IDX
    def scan_idx(self):
        if not self.job_running():
            return
        self.run_scan_idx()

    def run_scan_idx(self, show_diff=None):
        if not dbtable_has_data(self.dbopt, self.sys_a):
            self.isexec = False
            return  # check if a sys profile exists

        basedir = self.basedir
        email = self.email
        diff_file = get_diff_file(self.lclhome, self.usrDIR, self.moduleNAME)

        showDiff = show_diff
        if not show_diff:
            showDiff = self.ui.dbchka.isChecked()

        self.ui.hudt.append_colored_output("\033[1;32mSystem index scan..\033[0m")

        drive_type = self.j_settings.get(basedir, {}).get("drive_type")
        timeout = 300000
        if drive_type and drive_type == "HDD":
            timeout = timeout * 3
        self.proc = ProcessHandler()
        self.open_proc(timeout)
        self.proc_dbui()
        ismcore = True
        self.proc.set_mcore(ismcore)  # os.scandir workers cant be stopped flag. leave process open until complete
        # self.proc.status.connect(self.update_db_status)

        args = [
            'dirwalker.py',
            'scan',
            str(self.lclhome),
            self.dbopt,
            self.dbtarget,
            basedir,
            self.usr,
            diff_file,
            self.cache_s,
            email,
            str(self.analytics),
            str(showDiff),
            str(self.compLVL),
            'True',
            'True'
        ]
        self.ui.dbmainlabel.setText("Scanning idx")
        if not self.is_pyinstall:
            args = ["-u", self.app] + args
        self.proc.start_pyprocess(str(self.dispatch), args, database=self.dbopt, dbtarget=self.dbtarget, status_message="Index scan")

    # Main Build IDX
    def run_build_idx(self, basedir, cache_s, stsmsg, tables, idx_drive=False, drive_value=None):

        drive_type = self.j_settings.get(basedir, {}).get("drive_type")
        timeout = 300000
        if drive_type and drive_type == "HDD":
            timeout = timeout * 3
        self.proc = ProcessHandler()
        self.open_proc(timeout)

        ismcore = True
        drive_idx = None

        if idx_drive:
            drive_idx = drive_value
            self.proc.complete.connect(lambda code, _, d_value=drive_value: self.reload_drives(code, None, d_value))

        else:  # connect pg_2
            self.proc_dbui()  # label/pbar
            # self.proc.status.connect(self.update_db_status)  # reset label

        self.proc.complete.connect(lambda code, _, table_tuple=tables: self.reload_database_sn.emit(code, False, table_tuple))
        self.proc.complete.connect(lambda code, _: self.reload_sj_sn.emit(code, drive_idx, 'add', False))
        self.proc.set_mcore(ismcore)  # cant stop until some point

        args = [
            'dirwalker.py',
            'build',
            str(self.lclhome),
            self.dbopt,
            self.dbtarget,
            basedir,
            self.usr,
            cache_s,
            self.email,
            str(self.analytics),
            str(idx_drive),
            str(self.gnupg_home),
            str(self.compLVL),
            'True'
        ]
        if not self.is_pyinstall:
            args = ["-u", self.app] + args
        self.proc.start_pyprocess(str(self.dispatch), args, database=self.dbopt, status_message=stsmsg)

    # fork build button pg2
    def build_idx(self):
        if not self.job_running():
            return

        rlt = has_sys_data(self.dbopt, self.ui.hudt, self.sys_a, "Previous sys profile has to be cleared. Continue?", parent=self)  # prompt to delete
        if rlt is None:
            self.isexec = False
            return

        tables = (self.sys_a, self.sys_b, self.cache_table, self.systimeche)

        self.ui.hudt.appendPlainText("Hashing system profile...")
        turbo = 'mc' if self.driveTYPE.lower() == "ssd" else 'default'  # grab it from j_settings?
        self.ui.hudt.appendPlainText(f"Turbo is: {turbo}\n")

        self.ui.dbmainlabel.setText("Building profile")
        self.run_build_idx(self.basedir, self.cache_s, "System profile", tables)

    # fork       pg1
    # add index button page 1 '''
    def idx_drive(self):
        if self.isexec:
            window_message(self, "there is a current job started.", "Execution")
            return
        drive_info = self.get_newdrive()
        if not drive_info:
            return
        drive, drive_guid = drive_info
        if not drive:
            return
        if drive == self.basedir:
            self.ui.hudt.appendPlainText(f"{drive} sys basedir Requires build idx on db page")
            return

        cache_s, systimeche, key = get_cache_s(drive, self.cache_s_str)
        sys_tables, cache_table, _ = get_idx_tables(drive, cache_s, key)

        tables = (*sys_tables, cache_table, systimeche)  # for updating ui elements

        if dbtable_has_data(self.dbopt, sys_tables[0]):
            self.ui.hudt.appendPlainText(f"Drive {drive} has sys profile. switch basedir in config.toml and then clear IDX and rebuild on page2")
            return

        if drive != "C:\\":
            guid = drive_guid
            if self.j_settings.get(drive):
                update_dict(None, self.j_settings, drive)  # remove it conflicting entry
            update_dict({"drive_partguid": guid}, self.j_settings, drive)

        drive_type = setup_drive_settings(drive, key, None, None, user_json=self.sj, j_settings=self.j_settings, idx_drive=True)
        if drive_type is None:
            update_j_settings(None, self.j_settings, drive, self.sj)
            self.ui.hudt.appendPlainText(f"Unable to locate drive. quitting {drive}")
            return

        self.isexec = True
        self.ui.hudt.appendPlainText(f"Indexing {drive}")
        self.run_build_idx(drive, cache_s, f"Drive {drive} profile", tables, True, drive)

    # remove index button page 1 '''
    def rmv_idx_drive(self):
        drive = self.ui.combd.currentText()
        if drive == self.basedir or drive == "C:\\":
            return
        ix = self.ui.combd.currentIndex()

        idx_drive = self.j_settings.get(drive, {})

        cache_s, systimeche_table, key = get_cache_s(drive, self.cache_s_str)

        if idx_drive:

            sys_tables, cache_table, _ = get_idx_tables(drive, idx_suffix=key)
            # remove the drive cache .gpg file
            # call a thread as the database delete can freeze ui
            self.clear_sys(drive, cache_s, sys_tables, cache_table, systimeche_table, ix)
        else:
            self.ui.combd.removeItem(ix)
            systimeche = name_of(self.cache_s_str)
            cache_file = systimeche + f"_{key}.gpg"
            cache_file = os.path.join(self.lclhome, cache_file)
            removefile(cache_file)
            self.load_last_drive()

    # downloads button page 1
    # find downloads or files with the directory cache. os.scandir recursion
    def find_downloads(self, basedir="C:\\"):
        if not self.job_running():
            return

        is_idx = False

        cache_s = self.cache_s
        systimeche = self.systimeche

        drive = self.ui.combd.currentText()  # index selected

        if drive != basedir:

            is_idx = True

            if drive not in self.j_settings:
                self.isexec = False
                self.ui.hudt.appendPlainText(f"Failed to find {drive} in {self.sj}")
                return

            cache_s, systimeche, _ = get_cache_s(drive, self.cache_s_str)
            basedir = "C:\\"
            if drive != "C:\\":
                # result = get_disk_and_volume_for_drive
                # drive_ = self.j_settings.get(drive, {}).get("mount_of_index")
                guid = self.j_settings.get(drive, {}).get("drive_partguid")
                drive_ = get_drive_from_partguid(guid) if guid else None

                if drive_:
                    basedir = drive_  # if the mount point changed you can rename it with a prompt but its easy enough to reindex
                else:
                    self.isexec = False
                    self.ui.hudt.appendPlainText(f"couldnt find drive mount for {drive} partguid: {guid}")
                    return

        if not dbtable_has_data(self.dbopt, systimeche):  # indexed?
            self.isexec = False
            msg = f"{basedir} not indexed."
            if not is_idx:
                self.ui.hudt.appendPlainText(f"{msg} requires Build idx")
            else:
                self.ui.hudt.appendPlainText(msg)
            return

        if not os.path.isfile(cache_s):  # missing cache?
            self.ui.hudt.appendPlainText(f"Error cache not found. {'re index drive' if is_idx else 'requires rebuild IDX'}, file not found: {cache_s}")
            self.isexec = False
            if is_idx:  # dont remove / or basedir
                ix = self.ui.combd.findText(drive)
                if ix != -1:
                    self.ui.combd.removeItem(ix)
                    update_j_settings({"last_drive": self.basedir}, self.j_settings, None, self.sj)
                    if self.ui.combd.count() > 0:
                        self.ui.combd.setCurrentIndex(0)
            return

        drive_type = self.j_settings.get(drive, {}).get("drive_type")
        if drive_type not in ("HDD", "SSD"):
            self.ui.hudt.appendPlainText("json malformed defaulting to HDD")
            drive_type = "HDD"
        else:
            if drive == self.basedir and drive == "C:\\":
                if self.driveTYPE != drive_type:
                    drive_type = self.driveTYPE
                    update_j_settings({"drive_type": self.driveTYPE}, self.j_settings, self.basedir, self.sj)

        self.proc = ProcessHandler()
        self.open_proc(120000)

        # disable stop button ****
        ismcore = True
        self.proc.set_mcore(ismcore)
        args = [
            'dirwalker.py',
            'downloads',
            str(self.lclhome),
            self.dbopt,
            self.dbtarget,
            basedir,
            self.usr, drive_type,
            self.tempdir,
            str(self.gnupg_home),
            cache_s,
            self.dspEDITOR,
            self.dspPATH,
            self.email,
            str(self.analytics),
            str(self.compLVL)
        ]
        if not self.is_pyinstall:
            args = ["-u", self.app] + args
        self.proc.start_pyprocess(str(self.dispatch), args, database=self.dbopt)
        #
        # End Main db task
        #

    ''' Main Mft '''

    # Mft

    # Mft helpers
    def clearmfts(self):

        pattern = os.path.join(self.usrDIR, self.mftflnm + "*")
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

        elif os.path.isfile(self.parsec_command):
            method = "parsec"

        if os.path.isfile(self.icat_command):  # '.\\bin\\icat.exe'

            if os.path.isfile(self.fsstat_command):  # '.\\bin\\fsstat.exe'
                tool = "icat_fsstat"
            else:
                window_message(self, "1.fsstat.exe", f"Cant find executable in {self.exe_path}")
        elif os.path.isfile(self.ntfs_command):  # '.\\bin\\ntfstool.x86.exe'
            tool = "ntfstool"
        else:
            window_message(self, "1.icat/fstat or 2.ntfstools", f"Cant find any executable in {self.exe_path}")

        return method, tool

    def prescreen(self, tool, mmin, method, output_f, csvnm, flnm, oldsort, flnmout, flnmdffout, drive, usrDIR):

        if tool == "icat_fsstat":

            if validmft(str(self.fsstat_command)):  # First check for $MFT
                self.start_mfttrd("Mfticat_fstat", mmin, method, output_f, csvnm, flnm, oldsort, flnmout, flnmdffout, drive, usrDIR)
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
                        self.start_mfttrd("Mftntfs", mmin, method, output_f, csvnm, flnm, oldsort, flnmout, flnmdffout, drive, usrDIR, disk, volume)
                        return True
                        # self.worker.set_task(opt, disk, volume) #pass ins
                    else:
                        self.ui.hudt.appendPlainText("failed to locate mft for C:\\ before running ntfs tools")
                else:
                    self.ui.hudt.appendPlainText("failed volno or volume number for C:\\")
            else:
                self.ui.hudt.appendPlainText("failed diskno unable to find disk number for C:\\")
        return False

    def resetMftImport(self):

        self.ui.mftchkb.setDisabled(False)
        self.ui.mftrange.setDisabled(False)
        self.ui.mftmainlabel.setStyleSheet("")
        self.ui.mftmainlabel.setText("System Mft")
        self.mft = None
    # end Mft helpers

    # Main mft search
    def mftsearch(self):
        if not self.job_running():
            return

        method, tool = self.validaction()
        if tool is None:
            self.isexec = False
            return
        elif self.mft and (method == "mftdump" or method == "parsec"):
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
                    window_message(self, "ramdisk not found", "Error")
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
                window_message(self, f"mft {self.mft} not found.")
                self.isexec = False
                return

        csvnm = "mft.csv"
        mftraw = "Mft.raw"

        flnm = f'{self.mftflnm}{mmin}.txt'
        flnmout = os.path.join(self.usrDIR, flnm)
        flnmdff = f'{self.mftflnm}DiffFromLastSearch{mmin}.txt'
        flnmdffout = os.path.join(self.usrDIR, flnmdff)

        oldsort = None  # find prev search
        testp = os.path.join(self.usrDIR, flnm)
        if os.path.isfile(testp):
            oldsort = os.path.join(drive, flnm)
            try:
                shutil.copy(testp, oldsort)  # copy it to AppData
                self.clearmfts()
            except Exception:
                oldsort = None
                self.ui.hudt.appendPlainText(f'failed to copy old results: {testp}')

        isMftSave = self.ui.mftchkb.isChecked()

        # Imported Mft to csv
        if self.mft:

            self.ui.progressBAR.setValue(15)

            file_name = name_of(mft)
            output_f = f'{file_name}.csv'

            self.start_mfttrd("Mftimport", mmin, method, output_f, csvnm, flnm, oldsort, flnmout, flnmdffout, drive, self.usrDIR, None, None, mft)

            self.worker.set_task(self.parsec_command, self.mftec_command, self.icat_command, self.fsstat_command, self.ntfs_command)
            self.worker_thread.started.connect(self.worker.outputmft)
            self.worker.complete.connect(lambda code: self.update_ui_sn.emit(code, "Import"))
            self.open_trd()

        # Save the system Mft
        elif isMftSave:
            output_f = os.path.join(drive, mftraw)
            if not self.prescreen(tool, mmin, method, output_f, csvnm, flnm, oldsort, flnmout, flnmdffout, drive, self.usrDIR):  # check for NTFS valid $MFT
                self.isexec = False
                return  # optional pass ins
            self.worker.set_task(self.parsec_command, self.mftec_command, self.icat_command, self.fsstat_command, self.ntfs_command)

            self.worker_thread.started.connect(self.worker.save_mft)
            self.open_trd(150000)

        # Default make the search
        else:
            output_f = os.path.join(drive, mftraw)
            self.ui.progressBAR.setValue(5)

            if not self.prescreen(tool, mmin, method, output_f, csvnm, flnm, oldsort, flnmout, flnmdffout, drive, self.usrDIR):  # .
                self.isexec = False
                return
            self.worker.set_task(self.parsec_command, self.mftec_command, self.icat_command, self.fsstat_command, self.ntfs_command)

            self.worker_thread.started.connect(self.worker.run)
            self.open_trd(150000)

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
    def display_db(self, table="logs", refresh=False, only_combo=False):

        sender = self.sender()
        dybit = self.dirtybit

        if sender != self.ui.combdb and self.tableview_loaded() and not refresh:
            return True
        if not self.job_running(database=True, no_popup=True):
            return False
        if not only_combo:
            self.ui.combdb.setEnabled(False)
            # QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)

        def load_combdb():
            cd = self.ui.combdb
            c_text = cd.currentText()
            cd.blockSignals(True)
            cd.clear()
            tbl = sort_right(tables, self.cache_table, self.systimeche, self.suffix)  # self.basedir
            cd.addItems(tbl)

            # ix = cd.findText('extn')  # dont display extn table
            # if ix != -1:
            #     cd.removeItem(ix)

            ix = cd.findText(c_text)  # restore prev setting
            if ix != -1:
                cd.setCurrentIndex(ix)
            else:
                if cd.count() > 0:
                    cd.setCurrentIndex(0)
                    self.table = cd.currentText()
            cd.blockSignals(False)

        # set initial no compression
        if self.nc is None and self.db:
            self.nc = cnc(self.dbopt, self.compLVL)

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
                if table == "sys" or table.startswith("sys_") or table.startswith("cache"):
                    self.sys_step = table
                    self.ui.dbmainb2.setEnabled(True)

                tables = db.tables()
                if tables:
                    tables = [
                        t for t in tables
                        if t not in {"extn", "analytics", "scans", "scan_entries"}
                    ]
                    res = True
                    self.db = True
                    load_combdb()  # Update combobox

                    if not only_combo:
                        self.table = table
                        self.init_table_model_proxy()
                        self.init_dbstreamer(table, batch_size=500)
                        self.worker2.start()

                    if dybit:  # Anything to append?
                        query = QSqlQuery(db)
                        self.dirtybit = False
                        # last_drive = self.ui.combd.currentText()  # just save notes dont overwrite saved settings
                        # update_j_settings({"last_drive": last_drive}, self.j_settings, None, self.sj)

                        notes = self.ui.textEdit.toPlainText()
                        self.saved_history = get_history_view(self.saved_history, self.calculator)

                        self.nc = cnc(self.dbopt, self.compLVL)
                        commit_note_history(self.ui.hudt, notes, self.saved_history, self.email, query)

        except Exception as e:
            res = False
            self.ui.hudt.appendPlainText(f"failure in display_db err: {e} \n {traceback.format_exc()}")

        finally:
            if query:
                del query
            if db:
                db.close()

        if res and dybit:  # Released the connection above to append, otherwise locked out
            if not encr(self.dbopt, self.dbtarget, self.email, no_compression=self.nc, dcr=True):
                self.ui.hudt.appendPlainText("Problem rencrypting notes.")

        if not (refresh or only_combo):  # Only update connection status on combo selection
            if self._status_reset_timer.isActive():
                self._status_reset_timer.stop()
            self.ui.dbmainlabel.setText("Status: Connected" if res else "Status: offline")

        if not (only_combo or res) and getattr(self, 'worker2', None) is None:
            self.isexec = False
            self.ui.combdb.setEnabled(True)
            # QApplication.restoreOverrideCursor()
        elif only_combo:
            self.isexec = False
        return res

    # db and db Sql helpers
    def init_dbstreamer(self, table, sys_tables=None, cache_tables=None, superimpose=False, batch_size=500):

        self.result = None
        self.exit_result = -1
        self.worker2 = DbWorkerIncremental(self.dbopt, table, sys_tables, cache_tables, superimpose=superimpose, batch_size=batch_size)
        self.worker2.log.connect(self.append_log)
        self.worker2.exception.connect(
            lambda t, v, tb: sys.excepthook(t, v, tb)
        )
        self.worker2.headers_ready.connect(lambda headers, tname=table: self.on_header_values(headers, tname))
        self.worker2.batch_ready.connect(self.append_rows_to_model)
        self.worker2.finished_loading.connect(
            lambda code, tname=table: self.on_load_finished(tname, code)
        )
        self.worker2.finished.connect(self.worker2.deleteLater)

    def init_table_model_proxy(self):
        self.model = QStandardItemModel()
        self.proxy = QSortFilterProxyModel()
        self.proxy.setSourceModel(self.model)
        self.proxy.setSortCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.ui.tableView.setModel(self.proxy)
        self.ui.tableView.setSortingEnabled(False)

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

        if table in ('sqlite_sequence', "stats"):
            header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
            for i in range(len(headers)):
                width = header.sectionSize(i)
                if width > 1000:
                    header.resizeSection(i, 1000)
        else:
            systimeche = name_of(self.cache_s_str)
            if 'cache_' in table or systimeche in table:
                column_widths = [60, 135, 1100, 75, 75, 75, 75, 215]

            else:
                # per-table per-column width list
                column_widths = [60, 135, 1100, 135, 130, 135, 215, 75, 50, 115, 115, 50, 50, 135, 135, 70]
            # else:
                # column_widths = [100] * len(headers)
            for i, w in enumerate(column_widths):
                if i < len(headers):
                    header.resizeSection(i, w)

    @Slot(list)
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
        if code != 7:
            # if table == "logs":
            #     self.ui.tableView.sortByColumn(0, Qt.SortOrder.AscendingOrder)
            self.ui.tableView.setSortingEnabled(True)
            self.proxy.sort(-1)
        self.result = code
        self.ui.combdb.setEnabled(True)
        # QApplication.restoreOverrideCursor()
        self.worker2 = None
        self.isexec = False

    def reload_table(self):
        self.display_db(self.table, True, False)

    def super_impose(self):
        if not self.job_running(database=True, no_popup=True):
            return
        if not self.sys_step:
            return

        def parse_suffix(input_text):
            # get the key
            if input_text == "cache_s":
                return input_text, None
            parts = input_text.split('_', 1)
            key = parts[1] if len(parts) > 1 else None
            return parts[0], key

        cache_tables = None
        sys_tables = None

        # self.ui.combdb.setEnabled(False)
        # QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        try:

            self.init_table_model_proxy()

            s = self.sys_step
            prefix, key = parse_suffix(s)

            # cache_s
            #
            #
            if s.startswith("cache"):
                cache_table = s

                systimeche = name_of(self.cache_s_str)
                if key:
                    systimeche = systimeche + "_" + key

                cache_tables = (s, systimeche)
                table = cache_table + "_" + systimeche
            else:
                sys_a = s
                # sys_sda3
                # sys2_sda3
                sys_b = sys_a + "2"
                if key:
                    sys_b = prefix + "2" + "_" + key  # sys_b + "_" + key
                sys_tables = (sys_a, sys_b)
                table = sys_a + "_" + sys_b
            combo = self.ui.combdb
            if table not in [combo.itemText(i) for i in range(combo.count())]:
                combo.blockSignals(True)
                combo.addItem(table)
                ix = combo.findText(table)
                if ix != -1:
                    combo.setCurrentIndex(ix)
                combo.blockSignals(False)
        except Exception:
            logging.error("an error occured in super_impose", exc_info=True)
            self.isexec = False
            self.ui.combdb.setEnabled(True)
            # QApplication.restoreOverrideCursor()
            return

        self.init_dbstreamer(table, sys_tables, cache_tables, superimpose=True, batch_size=500)
        self.worker2.start()

    def table_context_menu(self, pos):
        view = self.ui.tableView
        if not view.selectedIndexes():
            return

        menu = QMenu(view)
        copy_action = menu.addAction("Copy")
        chosen = menu.exec(view.viewport().mapToGlobal(pos))
        if chosen == copy_action:
            self.copy_current_cell()

    def copy_current_cell(self):
        idx = self.ui.tableView.currentIndex()
        if not idx.isValid():
            return
        val = idx.data()
        QApplication.clipboard().setText("" if val is None else str(val))

    # Expansion for database page
    # Live Search
    # on key press search and stop at first match
    # prev button
    # next button
    #
    # or
    #
    # Traditional Search
    # alternative would be search by string box and search by filename box
    #

    # end Sql helpers

    ''' Thread '''

    # Thread
    #
    def job_running(self, database=False, no_popup=False):
        if self.is_user_edit:
            return False
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
            if not no_popup:
                window_message(self, "there is a current job started.", "Execution")
        return False

    def cleanup_thread(self):
        if self.worker_thread:
            self.worker_thread.deleteLater()
            self.worker_thread = None
        if self.worker:
            self.worker = None

    def init_thread(self):
        self.worker.log.connect(self.append_log)
        self.worker.complete.connect(lambda code: self.finalize(code))
        self.worker.complete.connect(self.worker_thread.quit)
        self.worker_thread.finished.connect(self.worker.deleteLater)
        self.worker_thread.finished.connect(self.cleanup_thread)

    def open_trd(self, timeout=60000):
        self.isexec = True
        self.ui.sbasediridx.setEnabled(False)
        self.result = None
        self.exit_result = -1
        self.timeout_timer.start(timeout)

        self.stop_worker_sn.connect(self.worker.stop)  # type: ignore
        self.worker_thread.start()

    @Slot(int)
    def finalize(self, code):
        if code != 4:
            if self.timeout_timer.isActive():
                self.worker_timeout_sn.emit()  # stop the timer from main gui

        try:
            self.stop_worker_sn.disconnect()
        except (TypeError, RuntimeError):
            pass

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
        self.ui.sbasediridx.setEnabled(True)
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
            if isinstance(t, QThread) and t.isRunning():
                self.ui.hudt.appendPlainText("Thread did not quit after timeout")
                QTimer.singleShot(2000, self._fk_thread)
                return

        except (RuntimeError, AttributeError) as e:
            logging.error("Error in _fk_thread on thread timeout: %s", e, exc_info=True)
        self.finalize(4)

    # end Thread

    # Thread types
    # Mft
    def start_mfttrd(self, log_label, mmin, method, output_f, csvnm, flnm, oldsort, flnmout, flnmdffout, drive, usrDIR, disk=None, volume=None, mft=None):
        self.worker_thread = QThread()
        self.worker = MftWorker(self.lclhome, log_label, mmin, method, output_f, csvnm, flnm, oldsort, flnmout, flnmdffout, drive, usrDIR, disk, volume, mft)
        self.worker.progress.connect(self.increment_progress)
        self.ui.progressBAR.setValue(0)
        self.worker.exception.connect(
            lambda t, v, tb: sys.excepthook(t, v, tb)
        )
        self.init_thread()
        self.worker.moveToThread(self.worker_thread)

    # Db
    def start_cleartrd(self, drive):

        self.worker_thread = QThread()

        self.worker = ClearWorker(self.lclhome, self.home_dir, self.dbopt, self.dbtarget, drive, self.usr, self.email, self.flth, self.compLVL)
        self.worker.moveToThread(self.worker_thread)

        self.worker.progress.connect(self.increment_db_progress)
        self.worker.no_compression.connect(self.set_nc)
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
        if not self.job_running(True):
            return
        if self.table == "logs" and not self.tableview_loaded() or not table_loaded(self.dbopt, 'logs', self.ui.hudt):
            self.isexec = False
            return
        self.start_cleartrd(self.basedir)
        self.worker.progress.connect(self.increment_progress)
        self.worker.status.connect(self.update_db_status)
        self.ui.hudt.appendPlainText("\n")
        self._run_clear_task(self.worker.run_query)

    # fork cache clear button
    def clear_cache(self):
        if not self.job_running(True):
            return
        if not table_loaded(self.dbopt, 'logs', self.ui.hudt):
            self.isexec = False
            return
        self.start_cleartrd(self.basedir)
        self.worker.status.connect(self.update_db_status)  # db label pg2
        self.worker.complete.connect(lambda code: self.reload_database_sn.emit(code, False, ("logs",)))  # db reload pg2
        self.worker.set_cache(self.cachermPATTERNS)
        self._run_clear_task(self.worker.run_cacheclr, None)

    # fork clear IDX button
    # From _pg2 or remove index button on page 1. the former is a basedir the latter is a drive index from find downloads
    def clear_sys(self, drive, cache_s=None, sys_tables=None, cache_table=None, systimeche=None, idx=None):
        if not self.job_running(True):
            return False
        prompt_v = "Previous sys profile has to be cleared. Continue?" if not idx else f"drive {drive} has a sys profile and has to be cleared. Continue?"

        cache_s = cache_s or self.cache_s
        sys_tables = sys_tables or (self.sys_a, self.sys_b)
        cache_table = cache_table or self.cache_table
        systimeche = systimeche or self.systimeche

        sys_a = sys_tables[0]
        sys_b = sys_tables[1]

        tables = (sys_a, sys_b, cache_table, systimeche)

        # if it is basedir is there anything to clear?
        if drive == self.basedir:
            if not table_loaded(self.dbopt, sys_tables[0], self.ui.hudt):
                self.isexec = False
                return

        # if it is a drive index that is another basedir it could have a system profile. prompt to delete or exit. if an error exit
        drive_idx = None
        is_ps = False
        if idx:
            drive_idx = drive
            is_ps = has_sys_data(self.dbopt, self.ui.hudt, sys_tables[0], prompt_v, parent=self)
            if is_ps is None:
                self.isexec = False
                return

        self.start_cleartrd(drive)
        self.worker.status.connect(self.update_db_status)  # db label pg2
        self.worker.complete.connect(lambda code, tables=tables: self.reload_database_sn.emit(code, True, tables))

        if idx:
            self.worker.complete.connect(lambda code, idx=idx: self.reload_drives_sn.emit(code, idx, drive))

        self.worker.complete.connect(lambda code: self.reload_sj_sn.emit(code, drive_idx, 'rmv', is_ps))

        self._run_clear_task(self.worker.run_sysclear, [cache_s, sys_tables, cache_table, systimeche])

    #
    # end general tasks db

    # On completion Database Helpers

    # On completion of a db task either update the table or only combo depending if the user is on that page
    @Slot(int, bool, object)
    def reload_database(self, code, is_remove=False, tables=('logs',)):
        if code == 0:

            only_combo = False
            current_table = self.ui.combdb.currentText()  # if the user is on the selected table we want to reload it.

            if is_remove:
                only_combo = True
            else:
                if current_table not in tables:
                    only_combo = True  # the user is not on that table just refresh combobox selector

            drt = self.database_reload_timer
            if not drt.isActive():
                if self._reload_connection is not None:
                    drt.timeout.disconnect(self._reload_connection)

                self._reload_connection = drt.timeout.connect(
                    lambda: self.display_db(current_table, refresh=True, only_combo=only_combo)
                )
                drt.start(5000)

        elif code == 52:
            self.ui.hudt.appendPlainText("A problem saving changes was detected everything preserved. Diagnose if there are any gpg related problems.")
            # loadgpg(self.dbopt, self.dbtarget, self.ui.dbmainlabel) # roll back on failure. database integrity is fine
    # end On completion Database Helpers

    # General Helpers

    def on_calc_closed(self):
        self.calculator.deleteLater()
        self.calculator = None

    # Main search 5 min, search, 5 min filtered, filtered search update ui on completion
    def update_ui_settings(self, code, action_type):

        if action_type == "editor":
            self.setEnabled(True)

        if code == 0:
            if action_type == "search":
                self.ui.diffchka.setChecked(False)
                self.ui.diffchkb.setChecked(False)
                self.ui.diffchkc.setChecked(False)
            elif action_type == "scan":
                self.ui.dbchka.setChecked(False)
            elif action_type == "Import":
                self.resetMftImport()

    # pg 1 index combo
    @Slot(int, int, str)
    def reload_drives(self, code, idx=None, drive=None):
        if code == 0:

            if idx and idx > 0:  # removed index

                self.ui.combd.removeItem(idx)

                self.load_last_drive()

            elif drive:  # added index

                idx_suffix = self.j_settings.get(drive, {})

                if idx_suffix:
                    cache_s, _, _ = get_cache_s(drive, self.cache_s_str)
                    if os.path.isfile(cache_s):
                        self.ui.combd.addItem(drive)
                        ix = self.ui.combd.findText(drive)
                        if ix != -1:
                            self.ui.combd.setCurrentIndex(ix)
                    else:
                        self.ui.hudt.appendPlainText(f"Drive not found {drive}")
                else:
                    self.ui.hudt.appendPlainText(f"Failed to find {drive} in {self.sj}")
            else:
                self.ui.hudt.appendPlainText("Invalid argument no drive specified, reload_drives")

    # manage json
    @Slot(int, str, str, bool)
    def manage_sj(self, code, drive_idx, locale, is_ps):
        if code == 0:
            if drive_idx:  # drive is not basedir
                if locale == 'add':

                    self.lastdrive = drive_idx
                    update_dict({"last_drive": drive_idx}, self.j_settings)

                if locale == 'rmv':

                    # drive index with proteus shield
                    if is_ps:

                        e = self.basedirs.index_by_value(drive_idx)
                        if e != -1:
                            self.ui.sbasediridx.setEnabled(False)  # remove drive from basedirButton combo so it retains only drives with psEXTN
                            r = self.basedirs.remove_item(e)
                            self.ui.sbasediridx.setValue(r)
                            self.ui.sbasediridx.setMaximum(self.basedirs.items - 1)
                            self.ui.sbasediridx.setEnabled(True)

                    update_dict(None, self.j_settings, drive_idx)  # remove drive info for index

                    last_drive_value = self.j_settings.get("last_drive")

                    if last_drive_value:
                        if last_drive_value == drive_idx:
                            update_dict({"last_drive": self.basedir}, self.j_settings)  # set to default as index removed

                dump_j_settings(self.j_settings, self.sj)

            else:  # add or remove proteus EXTN from usrprofile.json for basedir

                if locale == 'add':

                    if self.psEXEC:
                        extn = create_profile_baseline(EXEC_EXTN)
                    else:

                        extn = self.proteusEXTN + self.proteusPATH

                    if extn:

                        self.basedirs.update_current_item(extn, proteusEXTN=extn)
                        update_j_settings({"proteusEXTN": extn}, self.j_settings, self.basedir, self.sj)
                        self.psEXTN = extn
                        self.is_exec_profile = self.psEXEC
                        extn = profile_to_str(extn, self.psEXEC)
                        self.ui.hudt.appendPlainText(f'Profile drive {self.basedir} saved profile: {extn}\n')

                    else:

                        self.ui.hudt.appendPlainText(f'Failed to load proteusEXTN from {self.sj}\n')

                if locale == 'rmv':

                    self.basedirs.update_current_item(None, proteusEXTN=None)
                    update_j_settings({"proteusEXTN": None}, self.j_settings, self.basedir, self.sj)

        else:

            if drive_idx:
                if locale == 'add':
                    update_j_settings(None, self.j_settings, drive_idx, self.sj)  # remove preliminary drive info as add index had failed

            else:
                if code == 52:
                    if locale == 'add':
                        if not table_loaded(self.dbopt, self.sys_a, self.ui.hudt):  # make sure json and db are in sync on failure
                            self.ui.hudt.appendPlainText('profile removed successfully before failure syncing current data to usrprofile.')
                            self.basedirs.update_current_item(None, proteusEXTN=None)
                            update_j_settings({"proteusEXTN": None}, self.j_settings, self.basedir, self.sj)

    # end General Helpers
    #
    # end page_2


def start_main_window():

    # compatible with powershell 5 and 7
    # inst, ver = pwsh_7()
    # if not inst:
    #     if ver is not None:
    #         print(f"PowerShell 7 is required. Installed version: {ver}")
    #     sys.exit(1)

    os.environ["PYTHONUTF8"] = "1"
    os.environ["PYTHONIOENCODING"] = "utf-8"  # for ansi characyers
    os.environ["PYTHONUNBUFFERED"] = "1"

    is_admin()

    appdata_local = Path(sys.argv[0]).resolve().parent  # find_install() # software install aka workdir

    home_dir = Path.home()
    log_dir = appdata_local / "logs"
    resources = appdata_local / "Resources"
    iconPATH = resources / "rntchanges.ico"
    flth = appdata_local / "flth.csv"
    dbtarget_frm = appdata_local / "recent.gpg"
    cache_f_frm = appdata_local / "ctimecache.gpg"
    cache_s_frm = appdata_local / "systimeche.gpg"
    dbtarget = str(dbtarget_frm)
    cache_f = str(cache_f_frm)
    cache_s = str(cache_s_frm)
    cache_s_str = str(cache_s_frm)  # used for reference

    toml_file, json_file, usr = get_config(appdata_local, platform="Windows")

    config = load_toml(toml_file)
    if not config:
        return 1
    email = config['backend']['email']
    email_name = config['backend']['name']
    downloads = user_path(config['compress']['downloads'], usr)
    zipPATH = config['compress']['zipPATH']
    dspEDITOR = config['display']['dspEDITOR']
    dspPATH_frm = config['display']['dspPATH']
    dspPATH = ""
    if dspEDITOR:  # user wants results output in text editor
        dspEDITOR = multi_value(dspEDITOR)
        dspEDITOR, dspPATH = resolve_editor(dspEDITOR, dspPATH_frm, toml_file)  # verify we have a working one
        if dspEDITOR is None:
            return 1
    cachermPATTERNS = config['backend']['cachermPATTERNS']
    alarm_soundFILE = config['display']['alarm_soundFILE']
    alarm_set_soundFILE = config['display']['alarm_set_soundFILE']

    popPATH = config['display']['popPATH']
    basedir = config['search']['drive']
    driveTYPE_frm = config['search']['driveTYPE']
    compLVL = config['logs']['compLVL']
    ll_level = config['logs']['logLEVEL']
    log_file = config['logs']['userLOG']
    proteuspaths = config['shield']['proteusPATH']
    nogo = user_path(config['shield']['nogo'], usr)
    suppress_list = user_path(config['shield']['filterout'], usr)

    cachermPATTERNS = cache_clear_patterns(usr, cachermPATTERNS)
    # startup/initialize

    sound_file_path = None
    set_sound_file_path = None

    if alarm_soundFILE:
        sound_file_path = os.path.join(resources, alarm_soundFILE)
    if alarm_set_soundFILE:
        set_sound_file_path = os.path.join(resources, alarm_set_soundFILE)

    # check ps paths have to be relative. check certain paths exist. check the config file for mismatches.
    if not check_config(proteuspaths, nogo, suppress_list) or not check_utility(zipPATH, downloads, popPATH, sound_file_path, set_sound_file_path):
        return 1

    log_path = log_dir / log_file
    setup_logger(log_path, ll_level, process_label="mainwindow")

    gpg_path = shutil.which("gpg")
    gnupg_home = None
    if gpg_path is None:
        print("a")
        gpg_path, gnupg_home = set_gpg(appdata_local, "gpg")
    else:
        gpg_path = Path(gpg_path).resolve()

    if not check_for_gpg():
        QMessageBox.critical(None, "Error", "Unable to verify gpg in path. Likely path was partially initialized. quitting")  # QMessageBox.warning(None, "")
        return 1

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
            # subprocess.run(["icacls", tempdir, "/grant", "NT AUTHORITY\\SYSTEM:(OI)(CI)F"], check=True)  # add admins back
            # subprocess.run(["icacls", tempdir, "/grant", "BUILTIN\\Administrators:(OI)(CI)F"], check=True)

            app = QApplication(sys.argv)

            # experiment with default theming kind of basic?
            # import qdarktheme
            # # qdarktheme.setup_theme("auto")
            # palette = qdarktheme.load_palette(theme="dark")
            # app.setPalette(palette)
            # # stylesheet = qdarktheme.load_stylesheet(theme="dark")
            # # app.setStyleSheet(stylesheet)

            is_key, err = iskey(email)
            if is_key is False:

                # from PySide6.QtWidgets import QInputDialog, QLineEdit
                # pawd, ok = QInputDialog.getText(None, "Enter new GPG Password", "Password:", QLineEdit.EchoMode.Password)
                icon = str(appdata_local / "Resources" / "gnupg-streamline.png")
                key_error = res = False
                dlg = PassphraseDialog(icon_path=icon)
                if not dlg.exec():
                    key_error = True
                else:
                    pawd = dlg.get_password()

                    res = genkey(appdata_local, usr, email, email_name, dbtarget, cache_f, cache_s, flth, tempdir, pawd)
                    if res:

                        print("Got password (hidden):", "*" * len(pawd) + "\n")
                if key_error or not res:
                    QMessageBox.critical(None, "Error", "Failed to generate key")
                    return 1
            elif is_key is None:
                QMessageBox.critical(None, "Error", err)
                return 1

            output = os.path.splitext(os.path.basename(dbtarget))[0]
            dbopt = os.path.join(tempdir, output + '.db')

            if os.path.isfile(dbtarget):
                res = decr(dbtarget, dbopt)
                if not res:
                    if res is None:
                        QMessageBox.critical(None, "Error", f"There is no key for {dbtarget}.")
                    else:
                        QMessageBox.critical(None, "Error", "Decryption failed .gpg could be corrupt. exitting.")
                    return 1

            # if drive is not "C:\\" resolve partguid. store info in json under suffix ie s for S:\\ drive
            j_settings = {}  # load it once. dump often to avoid desync but saves on unecessary reads

            cache_s, systimeche, suffix, driveTYPE = setup_drive_cache(
                basedir, appdata_local, dbopt, dbtarget, json_file, toml_file, cache_s_str,
                driveTYPE_frm, usr, email, compLVL, j_settings=j_settings, iqt=True
            )
            if not cache_s or not suffix or not j_settings:
                return 1

            # config changes
            json_dump = False

            # from user install
            if not gnupg_home:
                gnupg_home = os.environ.get("GNUPGHOME")
                gpg_home = j_settings.get("gnupghome")
                if gnupg_home:
                    if gnupg_home != gpg_home:
                        j_settings["gnupghome"] = gnupg_home
                        json_dump = True
                elif not gpg_home:
                    gnupg_home = find_gnupg_home(json_file, j_settings, iqt=True)  # detect and save
                    if gnupg_home:
                        json_dump = True

            distro_name = j_settings.get("distro_name")
            if not distro_name:
                _, distro_name = windows_version()
                if distro_name:
                    j_settings["distro_name"] = distro_name
                    json_dump = True
            # end confnig changes

            # end startup/initialize

            if json_dump:
                dump_j_settings(j_settings, json_file)

            print("Qt database in ", tempdir)

            icon_path = str(iconPATH)

            def excepthook(exc_type, exc_value, exc_traceback):
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                logging.error("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))

                app_inst = QApplication.instance()
                if app_inst is not None:
                    msg = QMessageBox()
                    msg.setIcon(QMessageBox.Icon.Critical)
                    msg.setWindowTitle("Error")
                    msg.setText(f"An unexpected error occurred:\n{exc_value}")
                    msg.setStandardButtons(QMessageBox.StandardButton.Ok)
                    msg.exec()
                # else:
                    # app_inst = QApplication(sys.argv)

                print(f"Unhandled exception {exc_type.__name__} stack trace logged to: {log_path}")
                sys.exit(1)
            sys.excepthook = excepthook

            exit_code = 0
            window = MainWindow(
                appdata_local, home_dir, config, j_settings, toml_file, json_file, log_dir, log_path,
                driveTYPE, distro_name, dbopt, dbtarget, cache_s, cache_s_str, systimeche, suffix,
                gpg_path, gnupg_home, dspEDITOR, dspPATH, popPATH, alarm_soundFILE,
                alarm_set_soundFILE, downloads, email, usr, cachermPATTERNS,
                tempdir
            )
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

    caller = os.environ.get("CMD_LINE")
    multiprocessing.freeze_support()
    if caller or len(sys.argv) >= 2:
        dispatcher(sys.argv)
    sys.exit(start_main_window())
