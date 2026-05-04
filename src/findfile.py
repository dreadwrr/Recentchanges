import fnmatch
import os
import shutil
import subprocess
import sys
import tempfile
import traceback
import zipfile
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from .config import load_toml
from .configfunctions import get_config
from .dirwalkerfunctions import files_search
from .findfileparser import build_parser
from .fsearchfunctions import set_excl_dirs
from .logs import setup_logger
from .pyfunctions import cprint
from .pyfunctions import user_path
from .rntchangesfunctions import display
from .rntchangesfunctions import filter_lines_from_list
from .rntchangesfunctions import get_runtime_exclude_list
from .rntchangesfunctions import search_pwsh
from .rntchangesfunctions import removefile
# 05/02/2026


def archive_failure_blk(result, file_list):
    rlt = result.returncode
    if rlt != 0:
        # stdout = result.stdout.decode("utf-8", errors="replace")
        # stderr = result.stderr.decode("utf-8", errors="replace")
        stdout = result.stdout
        stderr = result.stderr
        missing = [f for f in file_list.keys() if not os.path.isfile(f)]
        if missing:
            print(f"debug: {len(missing)}/{len(file_list)} target files were not found")
            for item in missing:
                print(item)
        print('\n')
        err = stdout + "\nstderr: " + stderr
        print(err)
        return 1
    return 0


# all in one for .zip using standard library. if strip duplicate retain absolute path
def zip_(complete, xdata, zipcmode, ziplevel, strip, zip_name="archive.zip"):

    modes = [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED, zipfile.ZIP_BZIP2, zipfile.ZIP_LZMA]

    def comp_chart(user_choice):
        user_choice = max(1, min(user_choice, 9))
        index = int(round((user_choice - 1) / 8 * (len(modes) - 1)))
        return modes[index]

    compression = comp_chart(zipcmode)

    with zipfile.ZipFile(zip_name, "w", compression=compression, compresslevel=ziplevel) as zipf:

        if strip:
            for src, arcname in complete.items():
                if os.path.isfile(src):
                    zipf.write(src, arcname=arcname)
                else:
                    print(f"target skipped (not a file): {src}")
        else:
            for src in xdata:
                if os.path.isfile(src):
                    zipf.write(src, arcname=src)
                else:
                    print(f"target skipped (not a file): {src}")

    return zip_name


def has_content(recent_files):
    try:
        with open(recent_files, 'r', encoding='utf-8') as f:
            return f.readline().strip() != ''
    except OSError:
        return False


# add " " to the paths
def encase_line(target_files, target_f):

    try:
        with open(target_f, "w", encoding='utf-8') as f:
            for line in target_files:

                encased_line = f'"{line}"'
                f.write(encased_line + "\n")
        return True
    except Exception as e:
        print(f"problem in encase_line {e} {type(e).__name__}")
        return None


# apply filter.py, filter out inclusions
def filter_list(target_files, arch_exclude, usr, moduleNAME):

    # exclude any searches results from this app using fn match
    search_files = f"*{moduleNAME}*.txt"

    # apply user filter
    rows = [(p,) for p in target_files]

    n_line = filter_lines_from_list(rows, usr, idx=0)
    if n_line is None:
        return None

    recent_files = []

    for row in n_line:
        line = row[0]

        if not line:
            continue

        line_path = line.lower()

        # filter out inclusions from the app
        if any(line_path.startswith(excl) for excl in arch_exclude):
            continue

        if fnmatch.fnmatch(line, search_files):
            continue

        recent_files.append(line)

    return recent_files


def comp_archive(target_f, target_files, archive, temp_dir, downloads, arch_exclude, usr, moduleNAME, zipPROGRAM, zipPATH, zipcmode, ziplevel, _7zipcmode, winrarcmode, strip):

    res = 1
    complvl = None
    archflnm = archive + ".zip"
    if zipPROGRAM == "7zip":
        complvl = "-mx=" + str(_7zipcmode)
        relative_flg = "-spf"  # use absolute paths
    elif zipPROGRAM == "winrar":
        complvl = "-m" + str(winrarcmode)
        relative_flg = "-ep1"  # use relative
        archflnm = archive + ".rar"
    elif zipPROGRAM != "zipfile":
        print("Unrecognized zip program skipping archive.")
        return 1

    xdata = filter_list(target_files, arch_exclude, usr, moduleNAME)
    if xdata is None:
        return 1
    elif not xdata:
        return 0

    out_file = os.path.join(downloads, archflnm)
    removefile(out_file)

    try:
        # if there is a newline in filename or too many args use zipfile
        # if strip use zipfile. any duplicate uses absolute path
        # for tar find the duplicates and create a second .tar file
        duplicates = []
        uniques = []
        complete = {}

        bases = [os.path.basename(p) for p in xdata]
        counts = Counter(bases)

        for filepath, base in zip(xdata, bases):
            if counts[base] == 1:
                uniques.append(filepath)
                complete[filepath] = base
            else:
                duplicates.append(filepath)
                complete[filepath] = filepath

        if zipPROGRAM == "zipfile":
            print("using zipfile")
            zip_(complete, xdata, zipcmode, ziplevel, strip, zip_name=out_file)
            res = 0

        else:

            cmd = [zipPATH, "a", complvl]

            if strip:
                # tempdir = tempfile.gettempdir()
                with tempfile.TemporaryDirectory() as tempdir:

                    for src, arcname in complete.items():
                        drive, tail = os.path.splitdrive(arcname)
                        rel = tail.lstrip("\\/")
                        dst = os.path.join(tempdir, rel)
                        if drive:
                            os.makedirs(os.path.dirname(dst), exist_ok=True)
                        shutil.copy2(src, dst)

                    cmd += ["-r", out_file, r".\*"]
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        cwd=tempdir,
                        text=True
                    )
                    if archive_failure_blk(result, complete) == 0:
                        res = 0
            else:

                result = encase_line(xdata, target_f)
                if not result:
                    return 1

                if zipPROGRAM == "7zip":
                    cmd += [relative_flg]

                cmd += [out_file, f"@{target_f}"]
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True
                )
                if archive_failure_blk(result, complete) == 0:
                    res = 0

        if res == 0:
            if os.path.isfile(out_file) and os.path.getsize(out_file) > 0:
                cprint.cyan(f"Archive created in: {out_file}")
    except FileNotFoundError as e:
        print(f"The file list for the archive is missing {target_f}. Error running zip program {zipPROGRAM} at {zipPATH} err: {e}")
    except Exception as e:
        print(f"An unexpected error happened while trying to compress {out_file}. {type(e).__name__} {e} traceback:\n {traceback.format_exc()}")

    return res


def main(localappdata, action, filename, extension, basedir, usr, dspEDITOR, dspPATH, temp_dir, cutoffTIME=None, zipPROGRAM=None, zipPATH=None, usrDIR=None, downloads=None):

    if not (filename or extension):
        print("Invalid input. exiting.")
        return 1

    current_time = datetime.now()

    localappdata = Path(localappdata)

    log_file = localappdata / "logs" / "errs.log"

    toml_file, json_file, usr = get_config(localappdata, usr, platform="Windows")
    config = load_toml(toml_file)
    if not config:
        return 1
    exclDIRS = user_path(config['search']['exclDIRS'], usr)
    moduleNAME = config['paths']['moduleNAME']
    ll_level = config['logs']['logLEVEL']
    zipcmode = config['compress']['zipcmode']
    ziplevel = config['compress']['ziplevel']
    _7zipcmode = config['compress']['_7zipcmode']
    winrarcmode = config['compress']['winrarcmode']

    strip = config['compress']['strip']

    archive = moduleNAME
    tgt_file = archive + 'xfindfiles.txt'

    recent_files = os.path.join(temp_dir, tgt_file)

    target_f = archive + ".txt"
    target_f = os.path.join(temp_dir, target_f)

    target_files = []

    tmn = str(cutoffTIME)

    res = 0

    try:

        # os.scandir
        if action == "python":

            out_text = ""

            logging_values = (localappdata, ll_level)
            logger = setup_logger(log_file, logging_values[1], "FINDFILE")

            search_start_dt = None
            if cutoffTIME is not None:
                if cutoffTIME != '0':
                    search_start_dt = (current_time - timedelta(minutes=tmn))  # if zero is specified means to compress all results dont filter by time if zero

            if filename and not extension:
                mode = 1
                out_text = f"filename: {filename}"
            elif not filename and extension:
                mode = 2
                out_text = f"extn: {extension}"
            if filename and extension:
                out_text = filename + extension
                has_ext = bool(os.path.splitext(filename)[1])
                if has_ext:
                    print(f"Searching for {out_text}\n")
                mode = 3
            print(f'Running os.scandir for {out_text}', flush=True)
            feedback = True
            iqt = True

            target_files, _ = files_search(basedir, search_start_dt, feedback, exclDIRS, logger, filename, extension, mode, iqt, strt=0, endp=100)

            if target_files:
                with open(recent_files, "w", encoding="utf-8") as f1:
                    for otline in target_files:
                        print(otline, file=f1)

        elif action == "pwsh":

            excl_file = 'excluded.txt'
            excl_path = os.path.join(temp_dir, excl_file)
            set_excl_dirs(basedir, excl_path, exclDIRS)  # send excluded list to file in temp dir

            s_path = localappdata / "scripts" / "ffsearch.ps1"

            pwsh_path = shutil.which("pwsh")
            if not pwsh_path:
                pwsh_path = "powershell.exe"

            cmd = [
                pwsh_path,
                "-ExecutionPolicy", "Bypass",
                "-File", str(s_path),
            ]
            args = ['-rootPath', basedir]
            if cutoffTIME is not None:
                if cutoffTIME != '0':
                    args.extend(['-cutoffMinutes', tmn])
                args.extend(['-archiveRs', target_f])
            args.extend(['-mergedRs', recent_files, '-excluded', excl_path, '-feedback'])
            # '-MaxParallel', str(os.cpu_count()), original
            if filename and extension:
                has_ext = bool(os.path.splitext(filename)[1])
                if has_ext:
                    print(f"Searching for {filename}{extension}\n")
                args.extend(['-fileName', filename, '-extension', extension])

            elif filename:
                args.extend(['-fileName', filename])
            else:
                args.extend(['-extension', extension])
            feedback = True
            target_files, validrlt = search_pwsh(cmd, args, feedback)
            if validrlt is None:
                return 1
            if not validrlt:
                print("\nNo return from powershell script. quitting")
                return 1
            if not os.path.isfile(target_f):
                print(f"no archive path list: {target_f} couldnt compress")
                return 1

        if target_files is None:
            return 1
        elif target_files and os.path.isfile(recent_files) and os.path.getsize(recent_files) != 0:
            print()

            if cutoffTIME is not None and downloads:

                flth_frm = localappdata / "flth.csv"  # filter hits
                dbtarget_frm = localappdata / "recent.gpg"  # database
                cache_f_frm = localappdata / "ctimecache.gpg"
                cache_s_frm = localappdata / "systimeche.gpg"
                flth = str(flth_frm)
                dbtarget = str(dbtarget_frm)
                cache_f = str(cache_f_frm)
                cache_s = str(cache_s_frm)
                cache_s, _ = os.path.splitext(cache_s)  # to match index drives as well as cache_s it is `systimeche`

                gnupg_home = None
                # exclude certain files from .rar/.zip. app inclusions and temp work area

                # def get_runtime_exclude_list(appdata_local, usrDIR, moduleNAME, flth, dbtarget, cache_f, cache_s, gnupg_home, log_path, dbopt=None, temp_dir=None):
                arch_exclude = get_runtime_exclude_list(
                    localappdata, usrDIR, moduleNAME, flth, dbtarget, cache_f, cache_s,
                    gnupg_home, str(log_file), recent_files, temp_dir=temp_dir
                )

                res = comp_archive(
                    target_f, target_files, archive, temp_dir, downloads, arch_exclude, usr,
                    moduleNAME, zipPROGRAM, zipPATH, zipcmode, ziplevel, _7zipcmode,
                    winrarcmode, strip
                )

            display(dspEDITOR, recent_files, True, dspPATH)
            if res != 0:
                pass
                # return 1
        print("Progress: 100%")
        return 0

    except Exception as e:
        print(f'An error occurred in ffsearch: {type(e).__name__} err: {e} \n {traceback.format_exc()}')
        return 1


def main_entry(argv):
    parser = build_parser()
    args = parser.parse_args(argv)

    calling_args = [
        args.appdata,
        args.action,
        args.filename,
        args.extension,
        args.basedir,
        args.user,
        args.dspEDITOR,
        args.dspPATH,
        args.tempdir,
        args.cutoffTIME,
        args.zipPROGRAM,
        args.zipPATH,
        args.usrDIR,
        args.downloads
    ]
    result = main(*calling_args)
    sys.exit(result)


if __name__ == "__main__":
    main_entry(sys.argv[1:])
