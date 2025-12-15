# 12/09/2025           developer buddy core
import csv
import ctypes
import getpass
import glob
import importlib.util
import logging
import magic
import os
import pandas as pd
import platform
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import traceback
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
import tomlkit
import winreg
from .fsearch import process_find_lines as pl1
from .fsearchmft import process_find_lines as pl2
from .fsearchps1 import process_find_lines as pl3
from .pyfunctions import cprint
# from .pyfunctions import epoch_to_date
from .pyfunctions import sbwr
from .pyfunctions import clear_conn
# sys.path.append(str(Path(__file__).resolve().parent)) original add  / or app root to path. see rntchangesfunctions.py for importing filter.py
# sys.path.append(os.path.dirname(os.path.dirname(__file__)))
# script_path = os.path.abspath(__file__)  # // filter.py originally beside main.py
# script_dir = os.path.dirname(script_path)
# parent_dir = os.path.dirname(script_dir)
# sys.path.insert(0, parent_dir)
# import filter as user_filter
myapp = Path(__file__).resolve().parent.parent
filter_patterns_path = myapp / "filter.py"
spec = importlib.util.spec_from_file_location("user_filter", filter_patterns_path)
user_filter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(user_filter)

# Note: For database cacheclear / terminal supression see pyfunctions.py


# inclusions from this script and the tempdir the script uses. db_output is the tempdir for the qt app parsed from the database file that is going to pstsrg or ha

def get_runtime_exclude_list(dbtarget, appdata_local, tempdir, MODULENAME, USRDIR, flth, CACHE_F, CACHE_S, db_output=None):

    dir_pth = os.path.join(appdata_local, "MDY_*")
    folders = glob.glob(dir_pth)
    old_searches = [os.path.join(fld, MODULENAME) for fld in folders]

    ad = os.path.join(appdata_local, MODULENAME[:3])
    usrd = os.path.join(USRDIR, MODULENAME[:3])

    excluded_list = [
        flth,
        tempdir,
        ad,
        usrd,
        dbtarget,
        CACHE_F,
        CACHE_S
    ]

    for entry in old_searches:
        excluded_list.append(entry)

    if db_output:
        excluded_list += [os.path.dirname(db_output)]

    return excluded_list

    # old_sort = os.path.join(appdata_local, "MDY_") # excludes entire folder
    # old_sort,


def intst(target_file, compLVL):
    CSZE = 1024*1024
    if os.path.isfile(target_file):
        _, ext = os.path.splitext(target_file)
        try:
            file_size = os.stat(target_file).st_size
            size = file_size
            if ext == ".gpg":
                size = file_size // 2

            return size // CSZE >= compLVL  # no compression
        except Exception as e:
            print(f"Error setting compression of {target_file}: {e}")
    return False


# term output
def logic(syschg, nodiff, diffrlt, validrlt, appdata, MODULENAME, THETIME, argone, argf, filename, flsrh, method):

    if syschg:
        if method == "rnt":
            if validrlt == "prev":
                print("Refer to \\AppData\\Local\\save-changesnew\\rntfiles_MDY folder for the previous search")
                print()
            elif validrlt == "nofiles":
                cprint.cyan('There were no files to grab.')
            cprint.cyan(f'Search results in: {appdata}')
            if THETIME != "noarguser" and syschg:
                cprint.cyan(f'All system files in the last {argone} seconds are included')

            elif syschg:
                cprint.cyan("All system files in the last 5 minutes are included")

            cprint.cyan(f'\n{MODULENAME}xSystemchanges{argone}')

        else:
            if flsrh:
                cprint.cyan(f'All files newer than {filename} on Desktop')
            elif argf:
                cprint.cyan('All new filtered files are listed on Desktop')
            else:
                cprint.cyan('All new system files are listed on Desktop')
    else:
        cprint.cyan('No sys files to report')
    if not diffrlt and nodiff:
        cprint.green('Nothing in the sys diff file. That is the results themselves are true.')


# open text editor
def display(dspEDITOR, filepath, syschg, dspPATH):
    if not (dspEDITOR and dspPATH):
        return
    if not syschg:
        # print(f"No file to open with {dspEDITOR}: {filepath}")
        return

    if os.path.isfile(filepath) and os.path.getsize(filepath) != 0:
        try:
            subprocess.Popen([dspPATH, filepath], shell=True)
        except subprocess.CalledProcessError as e:
            print(f"{dspEDITOR} failed. Try setting abs editor path (dspPATH). Error: {e}")


def resolve_editor(dspEDITOR, dspPATH, toml_file):

    EDITOR_MAP = {
        "notepad": r"C:\Windows\System32\notepad.exe",
        "notepad++": r"C:\Program Files\Notepad++\notepad++.exe"
    }

    display_editor = dspEDITOR

    def get_editor_path(editor_key, dspPATH):
        if dspPATH:
            return dspPATH
        return EDITOR_MAP.get(editor_key.lower())

    def validate_editor(editor_path, editor_key, dspPATH):
        if os.path.isfile(editor_path):
            return True
        if dspPATH:
            print(f"{editor_key} dspPATH incorrect: {dspPATH}")
        elif editor_path is not None:
            print(f"{editor_key} not installed (expected: {editor_path})")
        elif not editor_path:
            print(f"Invalid value for dspEDITOR {dspEDITOR}")
        return False

    editor_key = dspEDITOR.lower()
    editor_path = None

    if editor_key == "notepad++" and not dspPATH:
        editor_path = check_installed_app("notepad++", "Notepad++")
    elif editor_key == "notepad" and not dspPATH:
        editor_path = shutil.which("notepad.exe")

    if not editor_path:

        editor_path = get_editor_path(editor_key, dspPATH)
        if not editor_path:
            if dspPATH:
                print(f"Invalid path {dspPATH} for setting dspPATH")
                sys.exit(1)
            print(f"{dspEDITOR} not found please specify a dspPATH or path to an editor in settings")

        if not validate_editor(editor_path, editor_key, dspPATH):
            display_editor = False
            print(f"Couldnt find {dspEDITOR} in path. continuing without editor")
            update_toml_setting('display', 'dspEDITOR', False, toml_file)
            editor_path = ""

    return display_editor, editor_path


# scr / cerr logic
def filter_output(filepath, escaped_user, filtername, critical, pricolor, seccolor, typ, supbrwr=True, supress=False):
    webb = sbwr(escaped_user)
    flg = False
    with open(filepath, 'r') as f:
        for file_line in f:

            file_line = file_line.strip()
            ck = False

            if file_line.startswith(filtername):
                if supbrwr:
                    for item in webb:
                        if re.search(item, file_line):
                            ck = True
                            break
                if not ck and not supress and not flg:
                    getattr(cprint, pricolor, lambda msg: print(msg))(f"{file_line} {typ}")
            else:
                if critical != "no":
                    if file_line.startswith(critical) or file_line.startswith("COLLISION"):
                        getattr(cprint, seccolor, lambda msg: print(msg))(f'{file_line} {typ} Critical')
                        flg = True
                else:
                    getattr(cprint, seccolor, lambda msg: print(msg))(f"{file_line} {typ}")
    return flg


# WSL
#
# update the toml to disable wsl
def update_toml_setting(keyName, settingName, newValue, filePath):
    try:
        # config = toml.load(file_path)    removes commenting **      tomblib
        # if keyf in config and stng in config[keyf]:
        #     config[keyf][stng] = False
        #     with open(file_path, 'w') as file:
        #         toml.dump(config, file)
        with open(filePath, "r", encoding="utf-8") as f:
            doc = tomlkit.parse(f.read())

        doc[keyName][settingName] = newValue

        with open(filePath, "w", encoding="utf-8") as f:
            f.write(tomlkit.dumps(doc))
    except Exception as e:
        print(f"Failed to update toml {filePath} setting. check key value pair {type(e).__name__} {e}")
        raise


def get_linux_distro():
    os_release_path = "/etc/os-release"
    distro_info = {}
    try:
        with open(os_release_path, "r") as file:
            for line in file:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    value = value.strip('"')
                    distro_info[key] = value
        distro_id = distro_info.get("ID", "").lower()
        distro_name = distro_info.get("NAME", "").lower()
        for target in ("porteus", "artix"):
            if target in distro_id or target in distro_name:
                return True
        return False
    except FileNotFoundError:
        print("The file /etc/os-release was not found.")
    except Exception as e:
        print(f'An error occurred: {type(e).__name__} {e}')
    return False


def findwsl(toml):
    iswsl = is_wsl()
    if iswsl:
        default = get_default_distro()
        res = get_version1()
        if default and not res:
            question = "WSL installed. it is required to change to WSL1 continue?"
            while True:
                user_input = input(f"{question} (Y/N): ").strip().lower()
                if user_input == 'y':
                    iswsl = set_to_wsl1(default)
                    if iswsl:
                        return True
                elif user_input == 'n':
                    update_toml_setting('search', 'wsl', False, toml)
                    break
                else:
                    print("Invalid input, please enter 'Y' or 'N'.")
        elif not default and not res:
            print("Unable to get default distro for wsl..")
        else:
            return True
    else:
        print("WSL not installed setting changed to off")
    update_toml_setting('search', 'wsl', False, toml)
    return False


def is_wsl():
    uname = platform.uname()
    if "microsoft" in uname.release.lower() or "microsoft" in uname.version.lower():
        return True
    # try:
    #     with open("/proc/version", "r") as f:
    #         version = f.read().lower()
    #     if "microsoft" in version or "wsl" in version:
    #         return True
    # except FileNotFoundError:
    #     pass
    try:
        result = subprocess.run(
            ["wsl", "cat", "/proc/version"],
            capture_output=True,
            text=True,
            check=True
        )
        version = result.stdout.lower()
        return "microsoft" in version or "wsl" in version
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    if "WSL_DISTRO_NAME" in os.environ:
        return True
    return False


def set_to_wsl1(distro):
    proc2 = subprocess.run(["wsl", "--set-version", distro, "1"], capture_output=True, text=True)
    if proc2.returncode == 0:
        print(f"Successfully set {distro} to WSL 1")
        return True
    else:
        print(f"Error setting version for distro {distro} {proc2.stderr}")
        return False


# Parse the default distro for ver
def get_version1():
    proc = subprocess.run(["wsl", "--list", "--verbose"], capture_output=True, text=True, encoding="utf-16le")
    lines = proc.stdout.splitlines()
    for line in lines:
        line = line.strip()
        if line.startswith("*"):

            parts = re.split(r"\s+", line)
            if len(parts) >= 4:
                distro_name = parts[1]
                version_str = parts[-1]
                try:
                    version = int(version_str)
                except ValueError:
                    version = None
                if version == 1:
                    return distro_name
    return None


# find default wsl
def get_default_distro():

    proc = subprocess.run(["wsl", "--list", "--verbose"], capture_output=True, text=True, encoding="utf-16le")
    output = proc.stdout.splitlines()

    for line in output:
        line = line.strip()
        if line.startswith("*"):
            parts = re.split(r"\s+", line)
            if len(parts) >= 2:
                return parts[1]
    return None


# convert find command paths
def wsl_to_windows_path(wsl_path: str) -> str:
    if wsl_path.startswith("/mnt/"):
        driv = wsl_path[5]
        windows_path = wsl_path.replace(f"/mnt/{driv}/", f"{driv.upper()}:\\")
        windows_path = windows_path.replace("/", "\\")
        return windows_path
    return wsl_path


def conv_cdrv(file_entries):
    result = []
    for entry in file_entries:
        fields = entry.split(maxsplit=8)
        if len(fields) >= 9:
            wsl_path = fields[8]
            fields[8] = wsl_to_windows_path(wsl_path)
            result.append(fields)
    return result
# end convert find command paths

# end WSL


# find command search helper use powershell for find_files() for files the find command cant reach
# , file_type
def find_cmdhelp(s_path, mmin, USR):

    command = [
        "powershell.exe",
        "-ExecutionPolicy", "Bypass",
        "-File", str(s_path),
        "-cutoffMinutes", str(mmin),
        "-userName", str(USR)
    ]

    # if file_type == "ctime":
    #     command += ["-cmin", "True"]
    # else:
    #     command += ["-mmin", "True"]

    # print('Running command:', ' '.join(command)) debug

    # file_entries = []
    mmin_files = []
    cmin_files = []
    try:

        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, ermsg = proc.communicate()

        if proc.returncode not in (0, 1):
            print(proc.stdout)
            print()
            print(f"Err: {ermsg.decode(errors='backslashreplace')}")
            print("Powershell failure for find command helper.")
            return [], []

        recent_files = output.decode(errors='backslashreplace').splitlines()
        for record in recent_files:
            if len(record) >= 9:
                fields = record.split(maxsplit=8)

                if fields[2] > fields[0]:
                    cmin_files.append(fields)
                else:
                    mmin_files.append(fields)
                # file_entries.append(fields)
        return mmin_files, cmin_files

    except (FileNotFoundError, PermissionError) as e:
        print(f"find_cmdhelp unable to locate script {s_path} or access denied: {e}")
    except Exception as e:
        print(f"Unexpected error running powershell find helper find_cmdhelp in rntchangesfunctions: {type(e).__name__} {e}")
    return [], []

# find command search using WSL. One search ctime > mtime for downloaded, copied or preserved metadata files. cmin. Main search for mtime newer than mmin.
# ported from linux
# amin is used in place of cmin as cmin isnt updated the same as on linux. amin can be used for cmin loop to check if creation time is greater than mtime to find
# downloaded or copied files with preserved metadata.


def find_files(find_command, DRIVETYPE, usr_areas, file_type, RECENT, COMPLETE, init, checksum, updatehlinks, cfr, FEEDBACK, logging_values, end, cstart, search_start_dt, iqt=False, strt=20, endp=60):

    file_entries = []
    try:
        print('Running command:', ' '.join(find_command))
        proc = subprocess.Popen(find_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  # stderr=subprocess.DEVNULL
        output, err = proc.communicate()

        if proc.returncode not in (0, 1):
            stderr_str = err.decode("utf-8")
            print(stderr_str)
            print("Find command failed, unable to continue. Quitting.")
            sys.exit(1)

    except (FileNotFoundError, PermissionError) as e:
        print(f"Error running WSL find in find_files rntchangesfunctions.py: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error running WSL. command: {find_command} \nfind_files func rntchangesfunctions.py: {type(e).__name__} {e}")
        sys.exit(1)

    if file_type == "main":
        end = time.time()

    file_entries = [entry.decode(errors='backslashreplace') for entry in output.split(b'\0') if entry]
    file_entries = conv_cdrv(file_entries)  # /mnt/c to C:\

    if usr_areas:
        file_entries += usr_areas  # add user dirs for full accuracy

    if file_type == "main":
        if FEEDBACK:  # scrolling terminal look
            for entry in file_entries:
                if len(entry) >= 9:
                    file_path = entry[8]
                    print(file_path, flush=True)
            print()

    elif file_type != "ctime":
        raise ValueError(f"Invalid search type: {file_type}")

    if init and checksum:
        cprint.cyan('Running checksum.')
        cstart = time.time()
    RECENT, COMPLETE = pl1(file_entries, DRIVETYPE, checksum, updatehlinks, file_type, search_start_dt, logging_values, cfr, iqt, strt, endp)
    return RECENT, COMPLETE, end, cstart


# Main windows search. creation time or modified > cutoff time - default. uses powershell

def find_ps1(command, DRIVETYPE, RECENT, COMPLETE, mergeddb, init, checksum, updatehlinks, cfr, FEEDBACK, logging_values, end, cstart, iqt=False, strt=20, endp=60):  # FEEDBACK,  handled in .ps1 script

    def get_recent_changes(cursor, table):
        allowed_tables = ('files')
        if table not in allowed_tables:
            return None
        query = f'''
            SELECT timestamp, filename, creationtime, inode, accesstime, checksum, filesize, symlink, owner, domain, mode
            FROM {table}
            ORDER BY timestamp DESC
        '''
        cursor.execute(query)
        reslt = cursor.fetchall()
        return reslt

    file_entries = []

    print("Launching powershell.", flush=True)

    result, end = run_pwsh(command)
    if result is None and end is None:
        sys.exit(1)
    if not result and isinstance(end, int) and not os.path.isfile(mergeddb):
        print("No new files reported from scanline.ps1. exiting")
        sys.exit(1)

    # retrieve results from merged database to prepare for multiprocessing
    try:
        with sqlite3.connect(mergeddb) as conn:
            cur = conn.cursor()

            # if filename:  # any removals here
            #    cur.execute("DELETE from files WHERE filename = ?", (filename))
            #    conn.commit()

            file_entries = get_recent_changes(cur, "files")

    except (sqlite3.Error, Exception) as e:
        print(f"find_ps1 rntchangesfunctions Error getting results from \\scripts\\scanline.ps1 couldnt connect to {mergeddb} quitting err: {type(e).__name__} : {e}")
        sys.exit(1)
    finally:
        clear_conn(conn, cur)

    if not file_entries:
        print(f"No new files to report. powershell results empty in {mergeddb}.")
        sys.exit(0)

    if init and checksum:
        out_text = "Running checksum."
        if FEEDBACK:
            out_text = "\n" + out_text
        cprint.cyan(out_text)
        cstart = time.time()
    RECENT, COMPLETE = pl3(file_entries, DRIVETYPE, checksum, updatehlinks, logging_values, cfr, iqt, strt, endp)
    return RECENT, COMPLETE, end, cstart


# calibrate search using Mft

def find_mft(DRIVETYPE, RECENT, COMPLETE, init, checksum, cfr, FEEDBACK, logging_values, end, cstart, search_time, table='logs', iqt=False, strt=20, endp=60):

    p = search_time * 60

    compt = (datetime.now(timezone.utc) - timedelta(seconds=p))

    delta_value = (endp - strt)
    endval = strt + (delta_value / 2)
    logger = logging.getLogger("search_Mft")

    exec_path = logging_values[0] / "bin" / "MFTECmd.exe"

    csv_data = read_mftmem(str(exec_path), 'C:\\$MFT', compt, iqt, strt, endval)  # search
    if csv_data is None:
        print("Error read Mft data in IOString from MFTECmd.exe. exiting.")
        sys.exit(1)
    if len(csv_data.getvalue()) == 0:
        print("No files returned from read_mftmem from reading Mft. exiting.")
        sys.exit(1)

    prog_v = endval

    file_entries = search_Mft(csv_data, compt, logger)  # convert csv to list of tuples
    end = time.time()
    if not file_entries:
        print(f"No new files from results of search in Mft search time {p} seconds. exiting")
        sys.exit(1)

    if FEEDBACK:
        for entry in file_entries:
            if len(entry) >= 10:
                file_path = entry[9]
                print(file_path, flush=True)
    if init and checksum:
        cprint.cyan('\nRunning checksum.')
        cstart = time.time()
    RECENT, COMPLETE = pl2(file_entries, DRIVETYPE, checksum, table, logging_values, cfr, iqt, prog_v, endp)  # multiprocess
    return RECENT, COMPLETE, end, cstart


def get_full_path(df):

    df = df[df['ParentPath'].notna() & df['FileName'].notna()].copy()  # get rid of warnings by making a copy

    df['ParentPath'] = df['ParentPath'].fillna('').astype(str).str.replace(r'^\.(\\)?', r'C:\\', regex=True)
    df['FileName'] = df['FileName'].fillna('').astype(str)

    df['FullPath'] = df['ParentPath'].str.rstrip('\\') + '\\' + df['FileName'].str.lstrip('\\')

    return df


"""
 Read a parsed mft csv into pandas to diff and process. Return list for recentchangessearch
 MFTECmd
"""


def str_to_bool(x):
    return str(x).strip().lower() in ("true", "1")


def search_Mft(csv_p, compt, logger, iqt=False):  # tmn  csv            dec13/2025

    time_field = "LastModified0x10"
    ctime_field = "Created0x10"
    atime_field = "LastAccess0x10"

    bool_columns = ["InUse", "IsDirectory", "IsAds"]

    local_tz = datetime.now().astimezone().tzinfo

    columns = [
        "EntryNumber", "SequenceNumber", "InUse", "ParentEntryNumber", "ParentSequenceNumber", "ParentPath", "FileName", "Extension",
        "FileSize", "ReferenceCount", "ReparseTarget", "IsDirectory", "HasAds", "IsAds", "SI<FN", "uSecZeros", "Copied", "SiFlags", "NameType",
        "Created0x10", "Created0x30", "LastModified0x10", "LastModified0x30", "LastRecordChange0x10", "LastRecordChange0x30", "LastAccess0x10",
        "LastAccess0x30", "UpdateSequenceNumber", "LogfileSequenceNumber", "SecurityId", "ObjectIdFileDroid", "LoggedUtilStream", "ZoneIdContents",
        "SourceFile"
    ]
    try:

        csv_p.seek(0)
        df = pd.read_csv(
            csv_p, names=columns,
            low_memory=False,
            converters={col: str_to_bool for col in bool_columns}
        )

        dt_cols = [time_field, ctime_field, atime_field]

        for col in dt_cols:  # convert utc aware object
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize('UTC')

        df = df.dropna(subset=[time_field])

        #  ~ is not
        recent_files = df[
            (df['InUse']) &
            (~df['IsDirectory']) &
            (~df['IsAds']) &
            ((df[time_field] >= compt) | (df[ctime_field] >= compt))
        ].copy()

        recent_files['cam'] = None
        mask_cam = recent_files[ctime_field] > recent_files[time_field]
        recent_files.loc[mask_cam, 'cam'] = 'y'
        recent_files['LastModified'] = None
        recent_files.loc[mask_cam, 'LastModified'] = recent_files.loc[mask_cam, time_field]
        recent_files.loc[mask_cam, time_field] = recent_files.loc[mask_cam, ctime_field]

        recent_files[time_field] = recent_files[time_field].dt.tz_convert(local_tz).dt.tz_localize(None).dt.to_pydatetime()
        # recent_files[col] = recent_files[col].dt.floor('s')

        recent_files['EntryNumber'] = pd.to_numeric(recent_files['EntryNumber'], errors='coerce')
        recent_files['SequenceNumber'] = pd.to_numeric(recent_files['SequenceNumber'], errors='coerce')

        recent_files = recent_files.dropna(subset=['EntryNumber', 'SequenceNumber'])

        recent_files['EntryNumber'] = recent_files['EntryNumber'].astype('int64')
        recent_files['SequenceNumber'] = recent_files['SequenceNumber'].astype('int64')

        recent_files['inode'] = (recent_files['SequenceNumber'] << 48) | recent_files['EntryNumber']
        # recent_files['inode'] = np.left_shift(recent_files['SequenceNumber'], 48) | recent_files['EntryNumber']  # frn

        recent_files = recent_files.dropna(subset=['ParentPath', 'FileName'])
        recent_files = get_full_path(recent_files)

        dt_cols = [ctime_field, atime_field]
        for col in dt_cols + ['LastModified']:
            recent_files[col] = pd.to_datetime(recent_files[col], errors='coerce')  # ensure datetime
            recent_files[col] = recent_files[col].dt.tz_convert(local_tz).dt.tz_localize(None)
            recent_files[col] = recent_files[col].dt.strftime("%Y-%m-%d %H:%M:%S")
            recent_files[col] = recent_files[col].where(recent_files[col].notna(), None)

        recent_files['ReferenceCount'] = pd.to_numeric(recent_files['ReferenceCount'], errors='coerce')
        recent_files['ReferenceCount'] = recent_files['ReferenceCount'].apply(
            lambda x: x - 1 if x is not None else None
        )

        recent_files['FileSize'] = pd.to_numeric(recent_files['FileSize'], errors='coerce').astype('Int64')

        recent_files = recent_files.replace({pd.NA: None})

        SORTCOMPLETE = list(zip(
            recent_files[time_field],
            recent_files[ctime_field],
            recent_files[atime_field],
            recent_files['FileSize'],
            recent_files['LastModified'],
            recent_files['SiFlags'],
            recent_files['ReferenceCount'],
            recent_files['inode'],
            recent_files['cam'],
            recent_files['FullPath']
        ))
        return SORTCOMPLETE
    except Exception as e:
        print("Error reading converting data frame to tuple list search_Mft rntchangesfunctions. quitting")
        print(f"Error processing MFT data in search_Mft func rntchangesfunctions.py: {type(e).__name__} :{e}")
        logger.error(f"Error processing MFT data: {type(e).__name__} {e}", exc_info=True)
        sys.exit(1)


def mft_entrycount():
    KB = 1024
    MB = KB**2
    GB = KB**3
    byte_s = None
    cmd = ['fsutil', 'fsinfo', 'ntfsinfo', 'C:']
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = proc.communicate()
        output = (stdout + stderr).lower()
        if "access is denied" in output:
            print("Error: Access denied. Please run as administrator.")
            return None
        elif proc.returncode != 0:
            print("Command failed with return code", proc.returncode)
            print("err", stderr.strip())
            return None
        else:
            for line in stdout.splitlines():
                line = line.strip()
                if line.startswith("Mft Valid Data Length"):
                    match = re.search(r"([\d\.]+)\s*(GB|MB|KB|bytes)", line, re.IGNORECASE)
                    if match:
                        value = float(match.group(1))
                        unit = match.group(2).upper()
                        if unit == "GB":
                            byte_s = value * GB
                        elif unit == "MB":
                            byte_s = value * MB
                        elif unit == "KB":
                            byte_s = value * KB
                        else:
                            byte_s = value
        if byte_s is None:
            print("Unable to read MFT entry count")
        return byte_s
    except subprocess.SubprocessError as e:
        print(f"Error in subprocess execution mft_entrycount: {type(e).__name__}: {e}")
        print(traceback.format_exc())
        return None


def read_mft_progress(cmd, csv_data, byte_s, strt, endp, logger=None):

    total_e = (int(byte_s) // 1024)

    num_steps = 32
    step_size = total_e / (num_steps - 1)
    steps = [int(round(step_size * i)) for i in range(num_steps)]
    current_step_index = 0

    csv_started = False
    x = 0

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
    for line in iter(proc.stdout.readline, ''):
        if not line.strip():
            continue
        x += 1
        if current_step_index < len(steps) and x >= steps[current_step_index]:
            progress = float(current_step_index) / max(num_steps - 1, 1) * 100
            progress = round(strt + (endp - strt) * (progress / 100), 2)

            if logger:

                logger(float(progress))
            else:
                print(f'Progress: {progress}%')

            current_step_index += 1

        if ',' not in line:
            continue
        if not csv_started:
            if "EntryNumber,SequenceNumber,InUse" in line:  # if line.startswith("EntryNumber,SequenceNumber,InUse"): weird char at start BOM character discard header and rebuild later
                csv_started = True
                continue
            else:
                continue

        csv_data.write(line)

    proc.stdout.close()
    rlt = proc.wait()
    err_output = proc.stderr.read()
    return rlt, err_output


def read_mft_default(cmd, csv_data):

    csv_started = False

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
    for line in iter(proc.stdout.readline, ''):
        if not line.strip():
            continue

        if ',' not in line:
            continue
        if not csv_started:
            if "EntryNumber,SequenceNumber,InUse" in line:
                csv_started = True
                continue
            else:
                continue

        csv_data.write(line)

    proc.stdout.close()
    rlt = proc.wait()
    err_output = proc.stderr.read()
    return rlt, err_output


def read_mftmem(exec_path, mft, compt, iqt=False, strt=0, endp=100):  # mft='C:\\$MFT'

    cutoff = compt.replace(microsecond=0)
    cutoff = cutoff.isoformat().replace("+00:00", "Z")

    cmd = [exec_path, '-f', mft, '--dt', 'yyyy-MM-dd HH:mm:ss.ffffff', '--cutoff', cutoff, '--csv', 'C:\\', '--csvf', 'myfile2.csv']  # '.\\bin\\MFTECmd.exe'
    # print('Running command:', ' '.join(cmd))
    # print('Running command:' + ' '.join(f'"{c}"' for c in cmd))

    byte_s = mft_entrycount()

    csv_data = StringIO()
    try:
        if byte_s:
            rlt, std_err = read_mft_progress(cmd, csv_data, byte_s, strt, endp)
        else:
            print("Couldnt get mft entry count in read_mftmem")

            rlt, std_err = read_mft_default(cmd, csv_data)

        if rlt == 0:
            if len(csv_data.getvalue()) != 0:

                print(f'Progress: {float(endp):.2f}')
                return csv_data

            else:
                print("No csv_data in read_mftmem read_mftmem rntchangesfunctions")
        else:
            # for err_line in iter(proc.stderr.readline, ''):   #     print("ERR:", err_line.strip())
            if std_err:
                print(f'Failed. Unable to output csv with mftecmd.exe: {std_err}')
    except (FileNotFoundError, PermissionError):
        print(f'Unable to find MFTECmd.exe {exec_path} or permission error \\bin')
    except Exception as e:
        emesg = f'error running cmd {cmd} {type(e).__name__} {e}'
        print(f"{emesg} \n {traceback.format_exc()}")
        logging.error(f"{emesg} traceback: ", exc_info=True)
    return None


# after checking for a previous search it is required to remove all old searches to prevent write problems of the results
# also keeping the workspace clean as its important to have the exact number of files. This will erase all types and
# achieve this result. Also copy the old search to the MDY folder in app install for later diff retention
def clear_logs(USRDIR, DIRSRC, appdata_local, MODULENAME, method, archivesrh):

    FLBRAND = datetime.now().strftime("MDY_%m-%d-%y-TIME_%H_%M_%S")  # %y-%m-%d better sorting?
    validrlt = ""

    # Archive last search to AppData
    keep = [
        "xSystemchanges",
        "xSystemDiffFromLastSearch"
    ]

    new_folder = None
    for suffix in keep:
        pattern = os.path.join(DIRSRC, f"{MODULENAME}{suffix}*")
        matches = glob.glob(pattern)
        for fp in matches:
            validrlt = "prev"  # mark as not first srh for logic
            if not new_folder:
                new_folder = os.path.join(appdata_local, FLBRAND)
                Path(new_folder).mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(fp, new_folder)
            except Exception as e:
                print(f'clear_logs func Failed to move {fp} to appdata: {e}')

    if validrlt == "prev":
        # Delete oldest dir
        pattern = os.path.join(appdata_local, "MDY_*")

        dirs = glob.glob(pattern)
        dirs = [d for d in dirs if os.path.isdir(d)]

        dirs.sort()
        while len(dirs) >= archivesrh:
            oldest = dirs.pop(0)
            try:
                shutil.rmtree(oldest)
            except Exception as e:
                print(f"Error deleting {oldest}: {e}")
        # End Delete

    if method != "rnt":  # Clear all searches on desktop
        suffixes = [
            "xSystemDiffFromLastSearch",
            "xFltDiffFromLastSearch",
            "xFltchanges",
            "xFltTmp",
            "xSystemchanges",
            "xSystemTmp",
            "xNewerThan",
            "xDiffFromLast"
        ]

        for suffix in suffixes:

            pattern = os.path.join(USRDIR, f"{MODULENAME}{suffix}*")

            for filepath in glob.glob(pattern):
                try:
                    os.remove(filepath)
                except FileNotFoundError:
                    pass
    return validrlt


def check_utility(zipPATH=None, downloads=None, popPATH=None):
    if downloads:
        if not os.path.isdir(downloads):
            print(f"setting downloads path: {downloads} does not exist. exiting.")
            sys.exit(1)
    if zipPATH:
        if not os.path.isfile(zipPATH):
            print(f"setting zipPATH {zipPATH} does not exist. check setting")
            sys.exit(1)
    if popPATH:
        if not os.path.isdir(popPATH):
            print(f"setting popPATH {popPATH} does not exist. check setting")
            sys.exit(1)


# apply filter.py filter to TMPOPT
def filter_lines_from_list(lines, escaped_user):

    regexes = [re.compile(p.replace("{{user}}", escaped_user)) for p in user_filter.get_exclude_patterns()]
    filtered = [line for line in lines if not any(r.search(line[1]) for r in regexes)]
    return filtered


def to_bool(val):
    return val.lower() == "true" if isinstance(val, str) else bool(val)


def multi_value(arg_string):
    return False if isinstance(arg_string, str) and arg_string.strip().lower() == "false" else arg_string


# convert s to min
def convertn(quot, divis, decm):
    tmn = round(quot / divis, decm)
    if quot % divis == 0:
        tmn = quot // divis
    return tmn


def get_diffFile(lclhome, USRDIR, MODULENAME):

    defaultdiff = os.path.join(lclhome, f"{MODULENAME}xDiffFromLastSearch300.txt")

    # Try to find a difference file
    patterns = [
        os.path.join(lclhome, f"{MODULENAME}*DiffFromLast*"),
        os.path.join(USRDIR, f"{MODULENAME}*DiffFromLast*")
    ]

    difffile = None

    for pattern in patterns:
        matches = glob.glob(pattern)
        if matches:
            difffile = sorted(matches, key=os.path.getmtime, reverse=True)[0]
            break

    if not difffile:
        difffile = defaultdiff

    return difffile


# return base filename or base filename a new extension
def getnm(locale, ext=''):
    root = os.path.basename(locale)
    root, _ = os.path.splitext(root)
    return root + ext


def get_spt_path(sub_dir):
    # mdl_dir = Path(__file__).resolve().parent
    # os.path.dirname(os.path.abspath(os.sys.argv[0]))

    script_dir = Path(sys.argv[0]).resolve().parent
    tgt_path = script_dir / sub_dir
    return tgt_path


# UTC join
def timestamp_from_line(line):
    parts = line.split()
    return " ".join(parts[:2])


def line_included(line, patterns):
    return not any(p in line for p in patterns)


# prev search?
def hsearch(OLDSORT, appdata, MODULENAME, argone):

    dir_pth = os.path.join(appdata, "MDY_*")
    folders = sorted(glob.glob(dir_pth), reverse=True)

    for folder in folders:
        pattern = os.path.join(folder, f"{MODULENAME}xSystemchanges{argone}*")
        matching_files = sorted(glob.glob(pattern), reverse=True)

        for file in matching_files:
            if os.path.isfile(file):
                with open(file, 'r') as f:
                    OLDSORT.clear()
                    OLDSORT.extend(f.readlines())
                break

        if OLDSORT:
            break


def removefile(fpath):
    try:
        if os.path.isfile(fpath):
            os.remove(fpath)
            return True
    except (TypeError, FileNotFoundError):
        pass
    except Exception:
        pass
    return False


def is_admin():
    res = ctypes.windll.shell32.IsUserAnAdmin()
    if not res:
        script = os.path.abspath(sys.argv[0])
        params = " ".join(f'"{arg}"' for arg in sys.argv[1:])
        try:
            ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, f'"{script}" {params}', None, 1)
        except Exception as e:
            print(f"Failed to elevate: {e}")
            sys.exit(1)
        sys.exit(0)


def get_usr():
    USR = None
    try:
        USR = getpass.getuser()
    except OSError:
        print("unable to get username attempting fallback")
    if USR:
        return USR
    else:
        USR = Path.home().parts[-1]
        if USR:
            return USR
    return None


def pwsh_7():
    c_ver = None
    pwsh_path = shutil.which("pwsh")
    if not pwsh_path:
        print("Powershell not installed please install Powershell 7")
        return False, None
    try:
        result = subprocess.run([pwsh_path, "--version"], capture_output=True, text=True, check=True)
        version_str = result.stdout.strip()
        c_ver = int(version_str.split()[1].split(".")[0])
        if c_ver >= 7:
            is_correct = True
        else:
            is_correct = False

        return is_correct, c_ver
    except Exception as e:
        print(f"PowerShell incompatible{c_ver if c_ver is not None else ""} required: 7 {e}")
        return False, None


# Used for calibrate mftec check
# See if its the right version .NET 9 check version from stdout
def mftec_is_cutoff(lclappdata):  # recentchangessearch

    exec_path = os.path.join(lclappdata, "bin", "MFTECmd.exe")
    cmd = [exec_path, '-f', 'C:\\$MFT', '--dt', 'yyyy-MM-dd HH:mm:ss.ffffff', '--cutoff', '2025-11-10T07:48:46Z']  # , '--csv', 'C:\\', '--csvf', 'myfile2.csv' # '.\\bin\\MFTECmd.exe'
    # print('Running command:', ' '.join(cmd))
    # mesg = 'Running command:' + ' '.join(f'"{c}"' for c in cmd)
    try:
        cver = False
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for line in iter(proc.stdout.readline, ''):
            if line.strip():
                if "--cutoff" in line:
                    cver = True
                    break

        proc.stdout.close()
        rlt = proc.wait()
        proc_stderr = proc.stderr.read()
        if rlt != 0:
            if proc_stderr:
                print("MFTECmd stderr:")
                print(proc_stderr)
            return False

        return cver

    except FileNotFoundError:
        print(f"MFTECmd {exec_path} not found")
    except Exception as e:
        print(f"Failed to verify MFTECmd version mftec_cutoff function. {type(e).__name__} {e} \n{traceback.format_exc()}")


# Used in Qt for mftec check
# See if its the right version .NET 9 check version from file
# same as above but print the arg list to a file only works in certain environments.
def mftec_version(exe_path, tempdir):  # Qt

    fn = "cutoff"
    c_args = "--" + fn
    temp_path = Path(tempdir)
    version_file = temp_path / "version.txt"

    try:

        result = "mftec"

        # Run MFTECmd and redirect the output to version.txt
        subprocess.run(rf'"{exe_path}" > {version_file}', shell=True)     # .\bin\MFTECmd.exe

        if not version_file.is_file():
            return None

        with version_file.open("r", encoding="utf-8") as f:
            for line in f:
                if c_args in line:
                    result = "mftec_cutoff"
        removefile(version_file)

    except FileNotFoundError:
        result = None
        print(f"{exe_path} not found")
    except Exception as e:
        result = None
        print(f"mftec_ver exception {type(e).__name__} {e} \n {traceback.format_exc()}")

    return result


# size and owner. smallest size first and alphabetically by owner
def tsv_sort_by(row):
    parts = row.split("\t")
    owner = parts[8].lower() if len(parts) > 8 else ""
    try:
        size = float(parts[2])
    except (ValueError, TypeError):
        size = float("inf")
    return (owner, size)


# An overview of the files for a specified search. stat the file to give feedback if its accessable and
# not deleted. Magic gives accurate file description by reading file content (alternative to mimetypes which
# is by extension) cam field indicates changed time as modified time (dt). last modified time is the modified
# time from the download or copy which could be from 2021 for example. Also by checking the database a copy
# can also be detected by having the same checksum and a diffrent filename or inode. Sorted by above.
#
def build_tsv(RECENT, rout, outpath):
    fmt = "%Y-%m-%d %H:%M:%S"
    header = "Datetime\tFile\tSize(kb)\tType\tSymlink\tCreation\tcam\tAccessed\tOwner\tStatable\tCopy"
    tsv_files = []
    mtyp = ""
    is_copy = ""
    is_statable = False
    st = None
    try:
        copy_paths = set()

        if rout:
            for line in rout:
                parts = line.strip().split()
                if len(parts) < 6:
                    continue
                action = parts[0]
                if action in ("Deleted", "Nosuchfile"):
                    continue
                if action == "Copy":
                    full_path = ' '.join(parts[5:])
                    copy_paths.add(full_path)

        for entry in RECENT:
            if len(entry) < 12:
                continue
            dt = entry[0]
            fpath = entry[1]

            if not fpath:
                continue

            try:
                st = Path(fpath).stat()
                mtyp = magic.from_file(fpath, mime=True)  # mimetypes.guess_type(fpath)[0] or "" less detailed
                is_statable = True
            except Exception:
                pass

            sym_frm = entry[7]
            sym = sym_frm if sym_frm is not None else ""
            stat_bool = "y" if is_statable else ""  # originally was "" as statable but could be confusing

            onr = entry[8]
            if is_statable:
                sz = round(st.st_size / 1024, 2)
                # md = epoch_to_date(st.st_mtime)  # epoch_to_str(st.st_mtime)
            else:
                sz = entry[6]
                # md = dt

            ae = entry[4]
            creation_time = entry[2]
            cam = entry[11]

            if fpath in copy_paths:
                is_copy = "y"

            # explored changing or expanding on cam but it complicates and is inaccurate
            # original drafting phase
            # last_modified = entry[12]
            # if md and md > dt:
            #     dt = md
            #     if cam == "y":
            #         last_modified_frm = md
            #         last_modified = last_modified_frm.strftime(fmt)
            #     cam = ""

            row = (
                f"{dt.strftime(fmt) if dt else ''}\t"
                f"{fpath}\t"
                f"{sz}\t"
                f"{mtyp}\t"
                f"{sym}\t"
                f"{creation_time or ''}\t"
                f"{cam or ''}\t"
                f"{ae or ''}\t"
                f"{onr}\t"
                f"{stat_bool}\t"
                f"{is_copy}"
            )

            tsv_files.append(row)

        tsv_files.sort(key=tsv_sort_by)

        with open(outpath, "w", encoding="utf-8", newline='') as f:
            f.write(header + "\n")
            for row in tsv_files:
                f.write(row + "\n")
    except Exception as e:
        print(f"Error building TSV data in build_tsv func rntchangesfunctions: {type(e).__name__} {e}")
        return False
    return True


def check_for_gpg():
    try:
        result = subprocess.run(
            ["gpg", "--list-keys"],
            capture_output=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def iskey(email):
    try:
        result = subprocess.run(
            ["gpg", "--list-secret-keys"],
            capture_output=True,
            text=True,
            check=True
        )
        return (email in result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error running gpg:", e)
    return False


def genkey(email, name, TEMPD, pass_Word=None):
    if not pass_Word:
        p = getpass.getpass("Enter passphrase for new GPG key: ")
    else:
        p = pass_Word
    params = f"""%echo Generating a GPG key
Key-Type: RSA
Key-Length: 4096
Subkey-Type: RSA
Subkey-Length: 4096
Name-Real: {name}
Name-Email: {email}
Expire-Date: 0
Passphrase: {p}
%commit
%echo done
"""
    with tempfile.TemporaryDirectory(dir=TEMPD) as kp:
        try:
            ftarget = os.path.join(kp, 'keyparams.conf')
            # ftarget=r"C:\AMD\myfile.txt"
            # with open(ftarget, 'a'):
            #     os.utime(ftarget, None)

            # os.chmod(ftarget, 0o600)
            with open(ftarget, "w", encoding="utf-8") as f:
                f.write(params)

            cmd = [
                "gpg",
                "--batch",
                "--pinentry-mode", "loopback",
                "--passphrase", p,  # p
                "--generate-key",
                ftarget,
            ]

            subprocess.run(cmd, check=True)
            os.remove(ftarget)
            print(f"GPG key generated for {email}.")
            return True
        except subprocess.CalledProcessError as e:
            print("failed to make key params:", e)
        except Exception as e:
            print(f'Unable to make gpg key: {e}')
        finally:
            if os.path.isfile(ftarget):
                os.remove(ftarget)
    return False


def find_user_folder(folder_name="Desktop"):
    home = Path.home()
    onedrive_envs = ["OneDrive", "OneDriveConsumer"]
    for env in onedrive_envs:
        onedrive_path = os.environ.get(env)
        if onedrive_path:
            folder = Path(onedrive_path) / folder_name
            if folder.exists():
                return folder
    onedrive_paths = list(home.glob("OneDrive*/" + folder_name))
    if onedrive_paths:
        return onedrive_paths[0]
    fallback = home / folder_name
    if fallback.exists():
        return fallback
    return None


def res_path(settingName, theusr):
    if isinstance(settingName, list):
        return [s.replace("{{user}}", theusr) for s in settingName]
    elif isinstance(settingName, str):
        return settingName.replace("{{user}}", theusr)
    else:
        raise ValueError(f"Invalid type for settingName: {type(settingName).__name__}, expected str or list")


# get the single drive letter in lower
def parse_drive(basedir):
    return basedir.split(":", 1)[0].lower()


# c:\ has systimeche.gpg, systimeche table
# any other has systimeche_n.gpg, systimeche_n table
def get_cache_s(basedir, cache_file):  # get_cache_s
    prefix = getnm(cache_file)
    CACHE_S = cache_file
    systimeche = prefix
    if basedir != "C:\\":
        cd = parse_drive(basedir)
        CACHE_S = prefix + f"_{cd}.gpg"
        systimeche = prefix + f"_{cd}"
    return CACHE_S, systimeche

# cache_s/cache_s2/cache_n table has the directory
# structure at the time of the system profile

# c:\ has sys, cache_s
# any other has sys_n, sys2_n and cache_n
# eg drive is s:\ its sys_s, sys2_s and cache_s2
# eg drive r:\ sys_r, sys2_r, cache_r


# returns the profile table
# as well as the profile changes table
def get_idx_tables(basedir):
    sys_a = ""
    cache_table = "cache_s"
    if basedir != "C:\\":
        delim = ""
        cd = parse_drive(basedir)
        if cd == "s":
            delim = "2"
        sys_a = f"_{cd}"
        cache_table = f"cache_{cd}{delim}"
    sys_b = "sys2" + sys_a
    sys_a = "sys" + sys_a
    return (sys_a, sys_b), cache_table


# Database section new for dirwalker.py and Encryption functions file and memory below
# python .db merging incase problems with powershell do it in python
def mergedb(dbopt):

    merge_path = os.path.join(dbopt, "recent_merged.db")  # AppData\Local
    ppath = os.path.join(dbopt, "recent_part")
    part_db = glob.glob(f"{ppath}*")

    with sqlite3.connect(merge_path) as merged_conn:
        cur = merged_conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS files (
            timestamp TEXT,
            filename TEXT,
            creationtime TEXT,
            inode TEXT,
            accesstime TEXT,
            checksum TEXT,
            filesize INTEGER,
            symlink TEXT,
            owner TEXT,
            domain TEXT,
            mode TEXT,
            casmod TEXT,
            lastmodified TEXT,
            hardlinks TEXT
        );
        """)

        for part in part_db:
            if not os.path.isfile(part):
                continue

            try:
                with sqlite3.connect(part) as part_conn:
                    part_cur = part_conn.cursor()
                    part_cur.execute("SELECT * FROM files")
                    rows = part_cur.fetchall()

                    for row in rows:
                        cur.execute("""
                        INSERT INTO files (
                            timestamp, filename, creationtime, inode,
                            accesstime, checksum, filesize, symlink,
                            owner, domain, mode, casmod, lastmodified,
                            hardlinks
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, row)

            except sqlite3.Error as e:
                print(f"Error reading from {part}: {e}")
                return False
        merged_conn.commit()
    return merge_path


# enc mem
def encrm(c_data: str, opt: str, r_email: str, compress: bool = True, armor: bool = False) -> bool:
    try:
        cmd = [
            "gpg",
            "--batch",
            "--yes",
            "--encrypt",
            "-r", r_email,
            "-o", opt
        ]

        if not compress:
            cmd.extend(["--compress-level", "0"])

        if armor:
            cmd.append("--armor")

        subprocess.run(
            cmd,
            input=c_data.encode("utf-8"),
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return True

    except subprocess.CalledProcessError as e:
        err_msg = e.stderr.decode().strip() if e.stderr else str(e)
        print(f"[ERROR] Encryption failed: {err_msg}")
    return False


# dec mem
def decrm(src, quiet=False):
    if os.path.isfile(src):
        try:
            cmd = [
                "gpg",
                "--quiet",
                "--batch",
                "--yes",
                "--decrypt",
                src
            ]

            result = subprocess.run(cmd, check=True, capture_output=True, text=True, encoding="utf-8")

            return result.stdout  # decrypted CSV content
        except subprocess.CalledProcessError as e:
            if not quiet:
                print(f"[ERROR] Decryption failed: {e}")
            return None
    else:
        print('No such .gpg file')
        return None


def encr(database, opt, email, nc, md):
    try:
        cmd = [
            "gpg",
            "--yes",
            "--encrypt",
            "-r", email,
            "-o", opt,
        ]
        if nc:
            cmd.extend(["--compress-level", "0"])
        cmd.append(database)
        subprocess.run(cmd, capture_output=True, text=True, check=True)
        if md:
            os.remove(database)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Encryption failed: {e},")
        # print(f"stderr: {e.stderr}")
    except Exception as e:
        print(f'general exc encr: {e}')
    return False


def decr(src, opt):
    if os.path.isfile(src):
        try:
            cmd = [
                "gpg",
                "--yes",
                "--decrypt",
                "-o", opt,
                src
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            if result.returncode == 0:
                return True
            else:
                print(f"[ERROR] Decryption failed with exit code {result.returncode}")
                print(f"stderr: {result.stderr}")
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Decryption failed: {e}")
            print(f"stderr: {e.stderr}")
        except FileNotFoundError:
            print("GPG not found. Please ensure GPG is installed.")
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}")
    else:
        print(f"[ERROR] File {src} not found. Ensure the .gpg file exists.")

    return False


def set_gpg(lclapp_data, sub_dir='gpg'):
    # script_dir = Path(__file__).resolve().parent   the location of this script rntchangesfunctions.py
    # Path(sys.argv[0]).resolve().parent # caller path

    gpg_p = lclapp_data / sub_dir
    gnupg_home = gpg_p / "gnupghome"
    os.environ["PATH"] = str(gpg_p) + ";" + os.environ["PATH"]
    os.environ["GNUPGHOME"] = str(gnupg_home)
    return gnupg_home
# End Database section


def decr_ctime(CACHE_F):
    if not CACHE_F or not os.path.isfile(CACHE_F):
        return {}

    csv_path = decrm(CACHE_F)
    if not csv_path:
        print(f"Unable to retrieve cache file {CACHE_F} quitting.")
        return 1

    cfr_src = {}
    reader = csv.DictReader(StringIO(csv_path), delimiter='|')

    for row in reader:
        root = row.get('root')
        if not root:
            continue

        modified_ep = row.get('modified_ep') or ''
        cfr_src.setdefault(root, {})[modified_ep] = {
            "checksum": row.get('checksum') or '',
            "size": row.get('size') or '',
            "modified_time": row.get('modified_time') or '',
            "owner": row.get('owner') or '',
            "domain": row.get('domain') or ''
        }

    return cfr_src


def run_pwsh(command):
    try:
        validrlt = False

        print('Running command:', ' '.join(command))
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as proc:  # stderr=subprocess.STDOUT

            assert proc.stdout is not None
            for line in proc.stdout:
                if "Merge complete:" in line:
                    validrlt = True
                    break
                print(line, end="")

            err_output = proc.stderr.read()
            res = proc.wait()

            if res == 0:
                end = time.time()
                return validrlt, end
            else:
                if err_output:
                    print(err_output)
                print("Command failed subprocess fault or script error scanline.ps1")
                sys.exit(res)

    except (FileNotFoundError, PermissionError) as e:
        print(f"run_pwsh rntchangesfunctions Error launching PowerShell {command} unable to find powershell script or access denied : {type(e).__name__} {e}")
    except Exception as e:
        print(f"run_pwsh Unexpected error: {command} : {type(e).__name__} {e}")
    return None, None


def check_installed_app(exe_name, product_key=None):
    try:
        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            fr"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\{exe_name}"
        ) as key:
            value, _ = winreg.QueryValueEx(key, None)
            if os.path.isfile(value):
                return value
    except (FileNotFoundError):
        pass
    if product_key:
        try:
            with winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE,
                fr"SOFTWARE\{product_key}"
            ) as key:
                value, _ = winreg.QueryValueEx(key, "exe")
                if os.path.isfile(value):
                    return value
        except (FileNotFoundError):
            pass
    return None


def output_results_exit(RECENT, argone, is_calibrate, iswsl, fmt):
    file_nm = f"PwshOutput{argone}.txt"
    if is_calibrate:
        file_nm = f"MftOutput{argone}.txt"
    elif iswsl:
        file_nm = f"WSLOutput{argone}.txt"

    flnm_frm, ext = os.path.splitext(file_nm)
    outpath = flnm_frm + "_sample" + ext
    i = 1
    while os.path.exists(outpath):
        outpath = f"{flnm_frm}_sample_{i}{ext}"
        i += 1

    with open(outpath, 'w') as f:
        for entry in RECENT:
            if entry[1].startswith("C:\\Windows"):
                continue
            tss = entry[0].strftime(fmt)
            fp = entry[1]
            f.write(f'{tss} {fp}\n')
    print("\n Sample output complete:", outpath)
    sys.exit(1)
