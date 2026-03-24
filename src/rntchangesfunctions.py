# 03/22/2026           developer buddy core
import csv
import ctypes
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
import time
import traceback
import winreg
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from .config import update_toml_values
from .configfunctions import find_install
from .ctime import init_recentchanges
from .fsearch import process_line
from .fsearchfunctions import set_excl_dirs
from .fsearchmft import process_mft
from .fsearchps1 import process_ps1
from .fsearchparallel import process_lines
from .pyfunctions import cprint
from .pyfunctions import suppress_list
from .pysql import clear_conn
install_root = find_install()
filter_patterns_path = install_root / "filter.py"
spec = importlib.util.spec_from_file_location("user_filter", filter_patterns_path)
user_filter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(user_filter)
# Note: For database cacheclear / terminal supression see config.toml


def reset_csvliteral(csv_file):

    patterns_to_reset = user_filter._filterhitRESET

    try:
        with open(csv_file, newline='') as f:
            reader = csv.reader(f)
            rows = list(reader)
        for row in rows[1:]:
            if row[0] in patterns_to_reset:
                row[1] = '0'
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(rows)
    except (FileNotFoundError, PermissionError):
        print(f"nfs permission error on {csv_file} reset_csvliteral.")
        pass


# return base filename or base filename a new extension
def name_of(locale, ext=''):
    root = os.path.basename(locale)
    root, _ = os.path.splitext(root)
    return root + ext


# MEIPATH  the location is different. this is not used currently
def get_script_path(sub_dir):
    # mdl_dir = Path(__file__).resolve().parent
    # os.path.dirname(os.path.abspath(os.sys.argv[0]))
    script_dir = Path(sys.argv[0]).resolve().parent
    tgt_path = script_dir / sub_dir
    return tgt_path


def check_script_path(script, appdata_local=None):
    script_path = os.path.join(appdata_local, script) if appdata_local else script
    return script_path


# inclusions from this script. temp_dir is the temp_dir for the qt app
def get_runtime_exclude_list(appdata_local, USRDIR, MODULENAME, flth, dbtarget, CACHE_F, CACHE_S, log_path, dbopt=None, temp_dir=None):

    dir_pth = os.path.join(appdata_local, f"{MODULENAME}_MDY_*")
    folders = glob.glob(dir_pth)
    old_searches = [os.path.join(fld, MODULENAME) for fld in folders]

    ad_results = os.path.join(appdata_local, f'{MODULENAME}x')
    download_results = os.path.join(USRDIR, f'{MODULENAME}x')
    # gnupg_one = f"/home/{user}/.gnupg/random_seed"
    # gnupg_two = "/root/.gnupg/random_seed"

    excluded_list = [
        ad_results,
        download_results,
        flth,
        dbtarget,
        CACHE_F,
        CACHE_S,
        log_path
    ]

    for entry in old_searches:
        excluded_list.append(entry)

    if dbopt:
        excluded_list += [dbopt]
    if temp_dir:
        excluded_list += [temp_dir]

    return [e.lower() for e in excluded_list if e]


# term output
def logic(syschg, nodiff, diffrlt, validrlt, THETIME, argone, argf, result_output, filename, flsrh, method):

    if syschg:
        # if validrlt == "prev":
        #     print("Refer to \\AppData\\Local\\save-changesnew\\rntfiles_MDY folder for the previous search")
        #     print()

        if method == "rnt":

            if validrlt == "nofiles":
                cprint.cyan('There were no files to grab.')
                print()

            if THETIME != "noarguser":
                cprint.cyan(f'All system files in the last {argone} seconds are included')

            else:
                cprint.cyan("All system files in the last 5 minutes are included")

        else:

            if flsrh:
                cprint.cyan(f'All files newer than {filename} on Desktop')
            elif argf:
                cprint.cyan('All new filtered files are listed on Desktop')
            else:
                cprint.cyan('All new system files are listed on Desktop')
        cprint.cyan(result_output)

    else:
        cprint.cyan('No sys files to report')
    if not diffrlt and nodiff:
        cprint.green('Nothing in the sys diff file. That is the results themselves are true.')


# open text editor   # Resource leaks   wait() commun
def display(dspEDITOR, filepath, syschg, dspPATH):
    if not (dspEDITOR and dspPATH):
        return
    if not syschg:
        # print(f"No file to open with {dspEDITOR}: {filepath}")
        return

    if os.path.isfile(filepath):  # and os.path.getsize(filepath) != 0:
        try:
            subprocess.Popen([dspPATH, filepath])  # , shell=True windows **
        except Exception as e:
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
                return None, None
            print(f"{dspEDITOR} not found please specify a dspPATH or path to an editor in settings")

        if not validate_editor(editor_path, editor_key, dspPATH):
            display_editor = False
            print(f"Couldnt find {dspEDITOR} in path. continuing without editor")
            # update_config(toml_file, "dspEDITOR", "true")  # python version
            update_toml_values({'display': {'dspEDITOR': False}}, toml_file)
            editor_path = ""

    return display_editor, editor_path


def is_excluded(web_list, file_line):
    return any(re.search(pat, file_line) for pat in web_list)


def is_supressed(web_list, file_line, flag, suppress_browser, suppress):
    if flag or suppress:
        return True
    if suppress_browser and web_list:
        return is_excluded(web_list, file_line)
    return False


# scr / cerr logic
def filter_output(filepath, escaped_user, filtername, critical, pricolor, seccolor, typ, supbrwLIST, suppress_browser=True, suppress=False):
    web_list = suppress_list(escaped_user, supbrwLIST)
    flag = False
    with open(filepath, 'r', encoding='utf-8') as f:
        for file_line in f:

            file_line = file_line.strip()
            if file_line.startswith(filtername):

                if not is_supressed(web_list, file_line, flag, suppress_browser, suppress):
                    getattr(cprint, pricolor, lambda msg: print(msg))(f"{file_line} {typ}")
            else:
                if critical != "no":
                    if file_line.startswith(critical) or file_line.startswith("COLLISION"):
                        getattr(cprint, seccolor, lambda msg: print(msg))(f'{file_line} {typ} Critical')
                        flag = True
                else:
                    if not is_supressed(web_list, file_line, flag, suppress_browser, suppress):
                        getattr(cprint, seccolor, lambda msg: print(msg))(f"{file_line} {typ}")


def windows_version():
    import platform
    try:
        # Example: ("10", "10.0.22631", "SP0", "Multiprocessor Free")
        release, version, *_ = platform.win32_ver()

        # Match your Linux-style return: (id, name)
        windows_id = f"windows-{release}".lower() if release else "windows"
        windows_name = f"Microsoft Windows {release} ({version})".lower() if (release or version) else "windows"

        return windows_id, windows_name
    except Exception as e:
        print(f"An error occurred: {e}")
    return None, None

# WSL


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


def find_wsl(toml_file):
    dm = "switching to powershell"
    if is_wsl():
        default = get_default_distro()
        res = get_version1()
        if default and not res:

            question = "WSL installed. it is required to change to WSL1 continue?"
            while True:
                user_input = input(f"{question} (Y/N): ").strip().lower()
                if user_input == 'y':
                    if set_to_wsl1(default):
                        return True
                    else:
                        print(f"Unable to set wsl1. {dm}")

                elif user_input == 'n':
                    update_toml_values({'search': {'wsl': False}}, toml_file)
                    break
                else:
                    print("Invalid input, please enter 'Y' or 'N'.")

        elif not default:  # and not res:
            print(f"Unable to get default distro for wsl.. {dm}")
        else:
            return True
    else:
        print("WSL not installed setting changed to off")
    update_toml_values({'search': {'wsl': False}}, toml_file)
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
    print("converting...")
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
        fields = entry.split(maxsplit=10)
        if len(fields) >= 11:
            wsl_path = fields[10]
            fields[10] = wsl_to_windows_path(wsl_path)
            result.append(fields)
    return result
# end convert find command paths

# end WSL


def get_powershell_script(basedir, script_dir, EXCLDIRS, excl_file, tempwork, search_time, proval, endval, iqt):
    excl_path = os.path.join(tempwork, excl_file)
    set_excl_dirs(basedir, excl_path, EXCLDIRS)

    s_path = os.path.join(script_dir, "ctime.ps1")

    find_command_cmin = [
        "powershell.exe",
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", str(s_path),
        "-rootPath", basedir,
        "-cutoffMinutes", str(search_time),
        "-excluded", excl_path,
        "-StartR", str(proval),
        "-EndR", str(endval)
    ]
    if iqt:
        find_command_cmin += ["-progress"]
    # if FEEDBACK:
    #     find_command_cmin += ["-feedback"]
    return find_command_cmin


# find command search helper use
# powershell for find_files() for all created files
def find_cmdcreated(command, s_path, search_start_dt):

    try:

        # proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # output, ermsg = proc.communicate()

        # if proc.returncode not in (0, 1):
        #     print(proc.stdout)
        #     print()
        #     print(f"Err: {ermsg.decode(errors='backslashreplace')}")
        #     print("Powershell failure for find command helper.")
        #     return []

        # recent_files = output.decode(errors='backslashreplace').splitlines()
        # for record in recent_files:
        #     if not record:
        #         continue
        #     fields = record.split(maxsplit=10)
        #     if len(fields) >= 11:  # 11
        #         m_time = int(fields[0])
        #         c_time = int(fields[2])

        #         fields[0] = str(m_time / 1_000_000)
        #         fields[2] = str(c_time / 1_000_000)
        #         file_entries.append(fields)
        # return file_entries

        file_entries = []
        is_error = True

        print(f"\nCutoff {search_start_dt.replace(microsecond=0).isoformat()} \n")
        print('Running command:', ' '.join(command), flush=True)
        print()
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as proc:  # stderr=subprocess.STDOUT

            assert proc.stdout is not None
            for line in proc.stdout:
                if line.startswith("Merge complete:"):
                    is_error = False
                    break
                elif line.startswith("RESULT:"):
                    value_str = line.split("RESULT:")[1].strip()
                    print(value_str)
                elif line.startswith("Progress: "):
                    print(line, end="", flush=True)
                else:
                    fields = line.split(maxsplit=10)

                    if len(fields) >= 11:
                        m_time = int(fields[0])
                        c_time = int(fields[2])

                        fields[0] = str(m_time / 1_000_000)
                        fields[2] = str(c_time / 1_000_000)
                        file_entries.append(fields)

            err_output = proc.stderr.read()
            res = proc.wait()

            if res != 0:

                if err_output:
                    print(err_output)
                print("Command failed subprocess fault or script error scanline.ps1")
                is_error = True
                file_entries = []

        return file_entries, is_error

    except (FileNotFoundError, PermissionError) as e:
        print(f"find_created unable to locate script {s_path} or access denied: {e}")
    except Exception as e:
        print(f"Unexpected error running powershell find helper find_created in rntchangesfunctions: {type(e).__name__} {e}")
    return None, None


# find command search helper use
# powershell for find_files() for modified files the find command cant reach.
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
            if not record:
                continue
            fields = record.split(maxsplit=10)
            if len(fields) >= 11:
                m_time = int(fields[0])
                c_time = int(fields[2])

                fields[0] = str(m_time / 1_000_000)
                fields[2] = str(c_time / 1_000_000)
                # original
                # if fields[2] > fields[0]:
                #     cmin_files.append(fields)
                # else:
                #     mmin_files.append(fields)

                mmin_files.append(fields)

        # note cmin_files are disabled as handled by ctime.py
        return mmin_files, cmin_files

    except (FileNotFoundError, PermissionError) as e:
        print(f"find_cmdhelp unable to locate script {s_path} or access denied: {e}")
    except Exception as e:
        print(f"Unexpected error running powershell find helper find_cmdhelp in rntchangesfunctions: {type(e).__name__} {e}")
    return [], []

# find command search using WSL. One search ctime > mtime for downloaded, copied or preserved metadata files. cmin. Main search for mtime newer than mmin.
# ported from linux
# amin was used in place of cmin as cmin isnt updated the same as on linux. amin can be used for cmin loop to check if creation time is greater than mtime to find
# downloaded or copied files with preserved metadata.


def find_files(find_command, usr_areas, file_type, RECENT, COMPLETE, init, cfr, search_start_dt, user_setting, logging_values, end, cstart, search_time, EXCLDIRS, excl_file, toml_file, iqt=False, strt=20, endp=60, logger=None):

    # file_entries = []
    records = []

    if file_type == "ctime":
        # xRC tout bypass for created files to only run 1 loop
        if user_setting['xRC']:
            # try xRC
            records = init_recentchanges(search_time, search_start_dt, logging_values, search=True)
            if records not in (None, "db_error", "usn_error"):
                strt += 10
                if iqt:
                    print(f"Progress: {strt}%", flush=True)
                endp += 10

            # normal execution
            else:
                print("init_recentchanges returned ", records)
                print("\nSomething went wrong xRC is set to disabled. resuming")
                print("logfile", logging_values[0])
                records = []

                appdata_local = logging_values[2]
                tempwork = logging_values[3]
                basedir = user_setting['basedir']

                script_dir = os.path.join(appdata_local, "scripts")

                find_command = get_powershell_script(basedir, script_dir, EXCLDIRS, excl_file, tempwork, search_time, strt, endp, iqt)

                s_path = find_command[5]

                records, is_error = find_cmdcreated(find_command, s_path, search_start_dt)
                if records is None or is_error:
                    records = []
                strt += 10
                # if iqt:
                #     print(f"Progress: {strt}%", flush=True)
                endp += 10

                # disable the setting until the problem is found
                update_toml_values({'search': {'xRC': False}}, toml_file)

        # normal execution
        else:
            # -cmin doesnt update properly on wsl. when a file moves its ctime should change but doesnt

            # use powershell for just the created files which is fast
            # appdata_local = logging_values[2]
            # tempwork = logging_values[3]
            # s_path = os.path.join(appdata_local, "scripts", "ctime.ps1")
            s_path = find_command[5]

            records, is_error = find_cmdcreated(find_command, s_path, search_start_dt)
            if records is None or is_error:
                records = []
            strt += 10
            # if iqt:
            #     print(f"Progress: {strt}%", flush=True)
            endp += 10
    elif file_type == "main":

        try:
            print('Running command:', ' '.join(find_command), flush=True)
            proc = subprocess.Popen(find_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)  # stderr=subprocess.DEVNULL

            # output, err = proc.communicate()  # if buffered
            # if proc.returncode not in (0, 1):
            #     stderr_str = err.decode("utf-8")
            #     print(stderr_str)
            #     print("Find command failed, unable to continue. Quitting.")
            #     sys.exit(1)

            for line in proc.stdout:
                line = line.rstrip()
                fields = line.split(maxsplit=10)
                if len(fields) >= 11:
                    wsl_path = fields[10]
                    fields[10] = wsl_to_windows_path(wsl_path)
                    if file_type == "main" and user_setting['FEEDBACK']:
                        print(fields[10], flush=True)
                    records.append(fields)

            _, err = proc.communicate()
            rlt = proc.returncode

            if rlt not in (0, 1):
                print(err)
                print("Find command failed, unable to continue. Quitting.")
                sys.exit(1)

        except (FileNotFoundError, PermissionError) as e:
            print(f"Error running WSL find in find_files rntchangesfunctions.py: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"Unexpected error running WSL. command: {find_command} \nfind_files func rntchangesfunctions.py: {type(e).__name__} {e}")
            sys.exit(1)

    else:
        raise ValueError(f"Invalid search type: {file_type}")

    if file_type == "main":
        end = time.time()

    # file_entries = [entry.decode('utf-8', errors='backslashreplace') for entry in output.split(b'\0') if entry]  # if buffered
    # file_entries = conv_cdrv(file_entries)  # /mnt/c to C:\

    if usr_areas:
        records += usr_areas  # add user dirs for full accuracy

    # records = []  # if buffered
    # for fields in file_entries:
    #     if len(fields) >= 11:
    #         if file_type == "main" and user_setting['FEEDBACK']:  # scrolling terminal look
    #             print(fields[10], flush=True)
    #         records.append(fields)

    if init and user_setting['checksum']:
        cstart = time.time()
        cprint.cyan("Running checksum")
    # print(len(records))
    RECENT, COMPLETE = process_lines(process_line, records, file_type, search_start_dt, 'FSEARCH', user_setting, logging_values, cfr, iqt, strt, endp)
    return RECENT, COMPLETE, end, cstart


# Main windows search. creation time or modified > cutoff time - default. uses powershell

def find_ps1(command, RECENT, COMPLETE, mergeddb, init, cfr, search_start_dt, user_setting, logging_values, end, cstart, iqt=False, strt=20, endp=60):

    def get_recent_changes(cursor, table):
        allowed_tables = ('files',)
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

    print("Launching powershell.", flush=True)

    validrlt = False
    try:

        print('Running command:', ' '.join(command))
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as proc:  # stderr=subprocess.STDOUT

            assert proc.stdout is not None
            for line in proc.stdout:
                if "Merge complete:" in line:
                    validrlt = True
                    break
                print(line, end="", flush=True)

            err_output = proc.stderr.read()
            res = proc.wait()

            if res == 0:
                end = time.time()
            else:
                if err_output:
                    print(err_output)
                print("Command failed subprocess fault or script error scanline.ps1")
                sys.exit(res)

    except FileNotFoundError as e:
        print(f"powershell_run Error launching PowerShell file not found {command} err: {e}")
        validrlt = None
        end = None
    except Exception as e:
        print(f"powershell_run Unexpected error: {command} : {type(e).__name__} {e}")
        validrlt = None
        end = None

    if validrlt is None and end is None:
        sys.exit(1)
    if not validrlt and not os.path.isfile(mergeddb):
        print("No new files reported from scanline.ps1. exiting")
        sys.exit(1)

    # retrieve results from merged database to prepare for multiprocessing
    file_entries = []
    conn = cur = None
    try:
        conn = sqlite3.connect(mergeddb)

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

    if init and user_setting['checksum']:
        out_text = "Running checksum."
        if user_setting['FEEDBACK']:
            out_text = "\n" + out_text
        cprint.cyan(out_text)
        cstart = time.time()

    filetype = None
    RECENT, COMPLETE = process_lines(process_ps1, file_entries, filetype, search_start_dt, "FSEARCHPS1", user_setting, logging_values, cfr, iqt, strt, endp)
    return RECENT, COMPLETE, end, cstart


# calibrate search using Mft

def find_mft(RECENT, COMPLETE, init, cfr, search_start_dt, user_setting, logging_values, end, cstart, search_time, iqt=False, strt=20, endp=60):

    p = search_time * 60

    # compt = (datetime.now(timezone.utc) - timedelta(seconds=p))
    compt = search_start_dt.astimezone(timezone.utc)

    delta_value = (endp - strt)
    endval = strt + (delta_value / 2)
    logger = logging.getLogger("search_Mft")

    exec_path = logging_values[2] / "bin" / "MFTECmd.exe"

    csv_data = read_mftmem(str(exec_path), 'C:\\$MFT', compt, search_start_dt, iqt, strt, endval)  # search
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

    if user_setting["FEEDBACK"]:
        for entry in file_entries:
            if len(entry) >= 11:
                file_path = entry[10]
                print(file_path, flush=True)
    if init and user_setting["checksum"]:
        cprint.cyan('\nRunning checksum.')
        cstart = time.time()

    filetype = None
    RECENT, COMPLETE = process_lines(process_mft, file_entries, filetype, search_start_dt, "FSEARCHMFT", user_setting, logging_values, cfr, iqt, prog_v, endp)  # multiprocess
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
    lmtime_field = "LastModified"

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

        for col in dt_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize('UTC')

        df = df.dropna(subset=[time_field])

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

        original_mtime = recent_files.loc[mask_cam, time_field].copy()
        recent_files.loc[mask_cam, time_field] = recent_files.loc[mask_cam, ctime_field]
        recent_files.loc[mask_cam, ctime_field] = original_mtime

        recent_files["mtime_us"] = (recent_files[time_field].astype("int64") // 1_000).astype("int64")

        seq = pd.to_numeric(recent_files["SequenceNumber"], errors="coerce")
        ent = pd.to_numeric(recent_files["EntryNumber"], errors="coerce")
        mask = seq.notna() & ent.notna()
        recent_files = recent_files.loc[mask].copy()

        recent_files["inode"] = (
            seq[mask].astype(object) * (1 << 48) + ent[mask].astype(object)
        )

        recent_files = recent_files.dropna(subset=['ParentPath', 'FileName'])
        recent_files = get_full_path(recent_files)

        for col in dt_cols + [lmtime_field]:
            recent_files[col] = pd.to_datetime(recent_files[col], errors="coerce", utc=True).dt.tz_convert(local_tz)

        recent_files[time_field] = (
            recent_files[time_field].dt.tz_convert(local_tz).dt.tz_localize(None).to_numpy(dtype="datetime64[us]").astype(object)
        )

        for col in (ctime_field, atime_field, lmtime_field):
            recent_files[col] = recent_files[col].dt.strftime("%Y-%m-%d %H:%M:%S")
            recent_files[col] = recent_files[col].where(recent_files[col].notna(), None)

        recent_files['ReferenceCount'] = pd.to_numeric(recent_files['ReferenceCount'], errors='coerce')
        recent_files["FileSize"] = pd.to_numeric(recent_files["FileSize"], errors="coerce").map(
            lambda v: None if pd.isna(v) else int(v)
        )

        remaining_col = [
            "mtime_us", "FileSize", "SiFlags", "ReferenceCount", "inode", "cam", "FullPath"
        ]

        for col in remaining_col:
            recent_files[col] = recent_files[col].astype(object).where(pd.notna(recent_files[col]), None)

        result = list(zip(
            recent_files[time_field],
            recent_files['mtime_us'],
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
        return result
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

# to possiblly increase efficiency but overhead is not an issue. maybe if an error shows up
# this is for below read_mft
# proc = subprocess.Popen(
#     cmd,
#     stdout=subprocess.PIPE,
#     stderr=subprocess.PIPE,
#     bufsize=1024*1024,
#     text=True,
#     encoding="utf-8",
#     errors="replace"
# )
#
# buffer = ""
#
# while True:
#     chunk = proc.stdout.read(1024*1024)   # 1MB
#     if not chunk:
#         break
#
#     buffer += chunk
#     lines = buffer.split("\n")
#     buffer = lines.pop()
#
#     for line in lines:
#         process_line(line)


def read_mft_progress(cmd, csv_data, byte_s, strt, endp, show_progress=False, logger=None):

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
        if show_progress:

            #
            #

            if current_step_index < len(steps) and x >= steps[current_step_index]:
                progress = float(current_step_index) / max(num_steps - 1, 1) * 100
                progress = round(strt + (endp - strt) * (progress / 100), 2)

                if logger:

                    logger(int(progress))
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


def read_mftmem(exec_path, mft, compt, search_start_dt, iqt=False, strt=0, endp=100):
    # exec_path = '.\\bin\\MFTECmd.exe'
    # mft='C:\\$MFT'

    cutoff = compt.replace(microsecond=0)
    cutoff = cutoff.isoformat().replace("+00:00", "Z")

    cmd = [exec_path, '-f', mft, '--dt', 'yyyy-MM-dd HH:mm:ss.ffffff', '--cutoff', cutoff, '--csv', 'C:\\', '--csvf', 'myfile2.csv']
    print('Running command:', ' '.join(cmd))

    print(f"\nCutoff {search_start_dt.replace(microsecond=0).isoformat()} \n")
    # print('Running command:' + ' '.join(f'"{c}"' for c in cmd))

    byte_s = mft_entrycount()

    csv_data = StringIO()
    try:
        show_progress = False
        if byte_s:
            show_progress = True

        rlt, std_err = read_mft_progress(cmd, csv_data, byte_s, strt, endp, show_progress)

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


# recentchanges search
# after checking for a previous search it is required to remove all old searches to keep the workspace clean and avoid write problems later.
#  Also copy the old search to the MDY folder in /tmpfor later diff retention

def clear_logs(USRDIR, DIRSRC, method, appdata_local, MODULENAME, archivesrh):

    FLBRAND = datetime.now().strftime("MDY_%m-%d-%y-TIME_%H_%M_%S")  # %y-%m-%d better sorting?
    validrlt = ""

    # Archive last search to /tmp
    keep = [
        "xSystemchanges",
        "xSystemDiffFromLastSearch"
    ]

    new_folder = None
    for suffix in keep:
        pattern = os.path.join(DIRSRC, f"{MODULENAME}{suffix}*")
        matches = glob.glob(pattern)
        for fp in matches:
            if not new_folder:
                validrlt = "prev"  # mark as not first time search
                new_folder = os.path.join(appdata_local, f"{MODULENAME}_{FLBRAND}")
                Path(new_folder).mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(fp, new_folder)
            except Exception as e:
                print(f'clear_logs func Failed to move {fp} to appdata: {e}')

    if validrlt == "prev":
        # Delete oldest dir
        pattern = os.path.join(appdata_local, f"{MODULENAME}_MDY_*")

        dirs = glob.glob(pattern)
        dirs = [d for d in dirs if os.path.isdir(d)]

        dirs.sort()
        while len(dirs) > archivesrh:
            oldest = dirs.pop(0)
            try:
                shutil.rmtree(oldest)
            except Exception as e:
                print(f"Error deleting {oldest}: {e}")
        # End Delete

    if method != 'rnt':
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
    res = True
    if downloads:
        if not os.path.isdir(downloads):
            print(f"setting downloads path: {downloads} does not exist. exiting.")
            res = False
    if zipPATH:
        if not os.path.isfile(zipPATH):
            print(f"setting zipPATH {zipPATH} does not exist. check setting")
            res = False
    if popPATH:
        if not os.path.isdir(popPATH):
            print(f"setting popPATH {popPATH} does not exist. check setting")
            res = False
    return res


def filter_lines_from_list(lines, escaped_user, idx=1):
    if user_filter is None:
        print("Error unable to load filter filter.py")
        return None
    regexes = [re.compile(p.replace("{{user}}", escaped_user)) for p in user_filter._filter]

    filtered = []
    for line in lines:
        if not line or len(line) <= idx:
            continue
        value = line[idx]
        if not value:
            continue

        if not any(r.search(value) for r in regexes):
            filtered.append(line)
    return filtered


# def str_to_bool(x):
#     return str(x).strip().lower() in ("true", "1")
def to_bool(val):
    return val.lower() == "true" if isinstance(val, str) else bool(val)


def multi_value(arg_string):
    return False if isinstance(arg_string, str) and arg_string.strip().lower() == "false" else arg_string


# convert s to min
def time_convert(quot, divis, decm):
    tmn = round(quot / divis, decm)
    if quot % divis == 0:
        tmn = quot // divis
    return tmn


def get_diff_file(lclhome, USRDIR, MODULENAME):

    default_diff = os.path.join(lclhome, f"{MODULENAME}xDiffFromLastSearch300.txt")

    # Try to find a difference file
    patterns = [
        os.path.join(lclhome, f"{MODULENAME}*DiffFromLast*"),
        os.path.join(USRDIR, f"{MODULENAME}*DiffFromLast*")
    ]

    diff_file = None

    for pattern in patterns:
        matches = glob.glob(pattern)
        if matches:
            diff_file = sorted(matches, key=os.path.getmtime, reverse=True)[0]
            break

    if not diff_file:
        diff_file = default_diff

    return diff_file


# UTC join
def timestamp_from_line(line):
    parts = line.split()
    return " ".join(parts[:2])


def line_included(line, patterns):
    return not any(p in line for p in patterns)


# prev search?
def hsearch(OLDSORT, appdata, MODULENAME, argone):

    dir_pth = os.path.join(appdata, f"{MODULENAME}_MDY_*")
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
def tsv_sort_by(row, is_link=False):
    parts = row.split("\t")
    if not is_link:
        owner = parts[8].lower() if len(parts) > 8 else ""
    else:
        owner = parts[9].lower() if len(parts) > 9 else ""
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
def build_tsv(SORTCOMPLETE, TMPOPT, logf, rout, escaped_user, outpath, method, fmt):

    if method != "rnt":
        if logf is TMPOPT:
            SORTCOMPLETE = filter_lines_from_list(SORTCOMPLETE, escaped_user)

    tsv_files = []

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

        is_link = any(len(row) > 7 and row[7] == 'y' for row in SORTCOMPLETE)
        header = "Datetime\tFile\tSize(kb)\tType\tSymlink" + ("\tTarget" if is_link else "") + "\tCreation\tcam\tAccessed\tOwner\tStatable\tCopy"

        for entry in SORTCOMPLETE:
            if len(entry) < 13:
                continue

            is_statable = st = None
            mtyp = is_copy = ""

            dt = entry[0]
            fpath = entry[1]

            if not fpath:
                continue
            is_statable = False
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
            target = entry[12] if entry[12] else ""

            if fpath in copy_paths:
                is_copy = "y"

            row = (
                f"{dt.strftime(fmt) if dt else ''}\t"
                f"{fpath}\t"
                f"{sz}\t"
                f"{mtyp}\t"
                f"{sym}\t"
            )
            if is_link:
                row += f"{target}\t"
            row += (
                f"{creation_time or ''}\t"
                f"{cam or ''}\t"
                f"{ae or ''}\t"
                f"{onr}\t"
                f"{stat_bool}\t"
                f"{is_copy}"
            )

            tsv_files.append(row)

        tsv_files.sort(key=lambda row: tsv_sort_by(row, is_link=is_link))
        # tsv_files.sort(key=tsv_sort_by)

        with open(outpath, "w", encoding="utf-8", newline='') as f:
            f.write(header + "\n")
            for row in tsv_files:
                f.write(row + "\n")
    except Exception as e:
        print(f"Error building TSV data in build_tsv func rntchangesfunctions: {type(e).__name__} {e}")
        return False
    return True


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

# End Database section


def parse_search(command, args):
    """ findfile.py powershell stream """
    try:
        target_files = []
        cmd = command + args

        is_error = True

        print('Running command:', ' '.join(cmd), flush=True)
        print()
        with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as proc:  # stderr=subprocess.STDOUT

            assert proc.stdout is not None
            for line in proc.stdout:
                if "Merge complete:" in line:
                    is_error = False
                    break
                if "RESULT:" in line:
                    value_str = line.split("RESULT:")[1].strip()
                    print(value_str)
                    target_files.append(value_str)
                else:
                    print(line, end="", flush=True)

            err_output = proc.stderr.read()
            res = proc.wait()

            if res != 0:

                if err_output:
                    print(err_output)
                print("Command failed subprocess fault or script error scanline.ps1")
                is_error = True
                target_files = []

            return target_files, is_error
    except FileNotFoundError as e:
        print(f"powershell_run Error launching PowerShell file not found {command} err: {e}")
    except Exception as e:
        print(f"powershell_run Unexpected error: {command} : {type(e).__name__} {e}")
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
    """ calibrate powershell and find command and see what the mft says """
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
