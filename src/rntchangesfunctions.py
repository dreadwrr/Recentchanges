# 05/03/2026           developer buddy core
import csv
import ctypes
import glob
import importlib.util
import magic
import os
import re
import shutil
import subprocess
import sys
import time
import winreg
from datetime import datetime
from pathlib import Path
from .config import update_toml_values
from .configfunctions import find_install
from .dirwalkerfunctions import files_search
from .fsearchps1 import process_ps1
from .fsearchparallel import process_lines
from .fsearchscan import process_scan
from .pyfunctions import cprint
from .pyfunctions import suppress_list

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
def get_runtime_exclude_list(appdata_local, usrDIR, moduleNAME, flth, dbtarget, cache_f, cache_s, gnupg_home, log_path, dbopt=None, temp_dir=None):

    dir_pth = os.path.join(appdata_local, f"{moduleNAME}_MDY_*")
    folders = glob.glob(dir_pth)
    old_searches = [os.path.join(fld, moduleNAME) for fld in folders]

    ad_results = os.path.join(appdata_local, f'{moduleNAME}x')
    download_results = os.path.join(usrDIR, f'{moduleNAME}x')

    if not gnupg_home:
        gnupg_home = os.environ.get("GNUPGHOME")
    # gnupg_one = f"/home/{user}/.gnupg/random_seed"
    # gnupg_two = "/root/.gnupg/random_seed"

    excluded_list = [
        ad_results,
        download_results,
        flth,
        dbtarget,
        cache_f,
        cache_s,
        log_path
    ]
    if gnupg_home:
        excluded_list += [str(gnupg_home)]
    for entry in old_searches:
        excluded_list.append(entry)

    if dbopt:
        excluded_list += [dbopt]
    if temp_dir:
        excluded_list += [temp_dir]

    return [e.lower() for e in excluded_list if e]


# term output
def logic(syschg, nodiff, diffrlt, validrlt, thetime, argone, argf, result_output, filename, flsrh, method):

    if syschg:
        # if validrlt == "prev":
        #     print("Refer to \\AppData\\Local\\save-changesnew\\rntfiles_MDY folder for the previous search")
        #     print()

        if method == "rnt":

            if validrlt == "nofiles":
                cprint.cyan('There were no files to grab.')
                print()

            if thetime != "noarguser":
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


def find_scan(recent, complete, init, cfr, search_start_dt, user_setting, logging_values, end, cstart, exclDIRS, iqt=False, strt=20, endp=60, logger=None):

    records = []

    basedir = user_setting['basedir']
    feedback = user_setting['feedback']

    # normal execution

    records, _ = files_search(basedir, search_start_dt, feedback, exclDIRS, logger, iqt=iqt, strt=strt, endp=endp)
    strt += 15
    end = time.time()
    if records is None:
        return None, [], end, cstart
    endp += 30

    if init and user_setting['checksum']:
        out_text = "Running checksum."
        if user_setting['feedback']:
            out_text = "\n" + out_text
        cprint.cyan(out_text)
        cstart = time.time()

    file_type = None
    recent, complete = process_lines(process_scan, records, file_type, search_start_dt, 'FSEARCH', user_setting, logging_values, cfr, iqt, strt, endp)

    return recent, complete, end, cstart


# Main windows search. creation time or modified > cutoff time - default. uses powershell

def find_ps1(command, recent, complete, init, cfr, search_start_dt, user_setting, logging_values, end, cstart, iqt=False, strt=20, endp=60):

    feedback = user_setting['feedback']
    script_path = command[4]
    file_entries = []
    validrlt = False

    print("Launching powershell.", flush=True)

    try:
        print('Running command:', ' '.join(command), flush=True)
        print()

        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as proc:  # stderr=subprocess.STDOUT

            assert proc.stdout is not None
            for line in iter(proc.stdout.readline, ''):
                if not line:
                    continue
                if line.startswith("Merge complete:"):
                    validrlt = True
                    break
                elif line.startswith("RESULT:"):
                    value_str = line.split("RESULT:")[1].strip()
                    fields = value_str.split(maxsplit=10)

                    if len(fields) >= 11:
                        fields[7] = None if fields[7] == "None" else fields[7]
                        fields[8] = None if fields[8] == "None" else fields[8]
                        file_entries.append(fields)
                else:
                    print(line, end="", flush=True)

            end = time.time()
            err_output = proc.stderr.read()
            res = proc.wait()

            if res != 0:
                if err_output:
                    print(err_output)
                print(f"Command failed subprocess fault or script error {script_path}")
                sys.exit(1)

    except FileNotFoundError as e:
        print(f"powershell_run Error launching PowerShell file not found {command} err: {e}")
    except Exception as e:
        print(f"powershell_run Unexpected error: {command} : {type(e).__name__} {e}")

    if not validrlt or not end:
        sys.exit(1)
    if not file_entries:
        print("No new files to report. powershell results empty")
        sys.exit(0)

    if init and user_setting['checksum']:
        out_text = "Running checksum."
        if feedback:
            out_text = "\n" + out_text
        cprint.cyan(out_text)
        cstart = time.time()

    file_type = None
    recent, complete = process_lines(process_ps1, file_entries, file_type, search_start_dt, "FSEARCHPS1", user_setting, logging_values, cfr, iqt, strt, endp)
    return recent, complete, end, cstart


# recentchanges search
# after checking for a previous search it is required to remove all old searches to keep the workspace clean and avoid write problems later.
# Also copy the old search to the MDY folder in appinstall for later diff retention

def clear_logs(dirSRC, method, appdata_local, moduleNAME, archivesrh):

    FLBRAND = datetime.now().strftime("MDY_%m-%d-%y-TIME_%H_%M_%S")  # %y-%m-%d better sorting?
    validrlt = ""

    # Archive last search to appinstall
    keep = [
        "xSystemchanges",
        "xSystemDiffFromLastSearch"
    ]

    new_folder = None
    for suffix in keep:
        pattern = os.path.join(dirSRC, f"{moduleNAME}{suffix}*")
        matches = glob.glob(pattern)
        for fp in matches:
            if not new_folder:
                validrlt = "prev"  # mark as not first time search
                new_folder = os.path.join(appdata_local, f"{moduleNAME}_{FLBRAND}")
                Path(new_folder).mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(fp, new_folder)
            except Exception as e:
                print(f'clear_logs func Failed to move {fp} to appdata: {e}')

    if validrlt == "prev":
        # Delete oldest dir
        pattern = os.path.join(appdata_local, f"{moduleNAME}_MDY_*")

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

    suffixes = [
        "xSystemDiffFromLastSearch",
        "xFltDiffFromLastSearch",
        "xFltchanges",
        "xFltTmp",
        "xSystemchanges",
        "xSystemTmp",
    ]

    if method != 'rnt':
        suffixes += [
            "xNewerThan",
            "xDiffFromLast"
        ]

    for suffix in suffixes:

        pattern = os.path.join(dirSRC, f"{moduleNAME}{suffix}*")

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


def str_to_bool(x):
    return str(x).strip().lower() in ("true", "1")


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


def get_diff_file(lclhome, usrDIR, moduleNAME):

    default_diff = os.path.join(lclhome, f"{moduleNAME}xDiffFromLastSearch300.txt")

    # Try to find a difference file
    patterns = [
        os.path.join(lclhome, f"{moduleNAME}*DiffFromLast*"),
        os.path.join(usrDIR, f"{moduleNAME}*DiffFromLast*")
    ]

    diff_file = None

    all_matches = []
    for pattern in patterns:
        all_matches.extend(glob.glob(pattern))

    if all_matches:
        diff_file = max(all_matches, key=os.path.getmtime)

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
def hsearch(oldsort, appdata, moduleNAME, flnm):

    dir_pth = os.path.join(appdata, f"{moduleNAME}_MDY_*")
    folders = sorted(glob.glob(dir_pth), reverse=True)

    # glob changes based on flnm

    for folder in folders:
        pattern = os.path.join(folder, f"{moduleNAME}{flnm}*")  # pattern = os.path.join(folder, f"{moduleNAME}xSystemchanges{argone}*")
        matching_files = sorted(glob.glob(pattern), reverse=True)

        for file in matching_files:
            if os.path.isfile(file):
                with open(file, 'r') as f:
                    oldsort.clear()
                    oldsort.extend(f.readlines())
                break

        if oldsort:
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

        is_correct = c_ver >= 7
        return is_correct, c_ver
    except Exception as e:
        print(f"PowerShell incompatible{c_ver if c_ver is not None else ""} required: 7 {e}")
        return None, None


def search_pwsh(command, args, script_path, feedback=True):
    """ powershell stream """
    try:
        target_files = []
        validrlt = False

        cmd = command
        if args:
            cmd = command + args

        output = ' '.join(cmd)
        print('Running command:', output, flush=True)
        cprint.cyan('')

        with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as proc:  # stderr=subprocess.STDOUT

            assert proc.stdout is not None
            for line in proc.stdout:
                if "Merge complete:" in line:
                    validrlt = True
                    break
                if "RESULT:" in line:
                    value_str = line.split("RESULT:")[1].strip()
                    if feedback:
                        print(value_str)
                    target_files.append(value_str)
                else:
                    print(line, end="", flush=True)

            err_output = proc.stderr.read()
            res = proc.wait()

            if res != 0:

                if err_output:
                    print(err_output)
                print(f"Command failed subprocess fault or script error {script_path}")
                return None, None

            return target_files, validrlt
    except FileNotFoundError as e:
        print(f"powershell_run Error launching PowerShell file not found {command} err: {e}")
    except Exception as e:
        print(f"powershell_run Unexpected error: {command} : {type(e).__name__} {e}")
    return None, None


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
def build_tsv(sortcomplete, tmpopt, logf, rout, escaped_user, outpath, method, fmt):

    if method != "rnt":
        if logf is tmpopt:
            sortcomplete = filter_lines_from_list(sortcomplete, escaped_user)

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

        is_link = any(len(row) > 7 and row[7] == 'y' for row in sortcomplete)
        header = "Datetime\tFile\tSize(kb)\tType\tSymlink" + ("\tTarget" if is_link else "") + "\tCreation\tcam\tAccessed\tOwner\tStatable\tCopy"

        for entry in sortcomplete:
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
