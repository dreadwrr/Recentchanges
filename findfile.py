import fnmatch
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import traceback
import zipfile
from collections import Counter
from pathlib import Path
from .config import load_toml
from .configfunctions import get_config
from .findfileparser import build_parser
from .fsearchfunctions import set_excl_dirs
from .pyfunctions import cprint
from .rntchangesfunctions import display
from .rntchangesfunctions import get_runtime_exclude_list
from .rntchangesfunctions import removefile
from .rntchangesfunctions import parse_search
from .rntchangesfunctions import wsl_to_windows_path
from .rntchangesfunctions import filter_lines_from_list
from .qtdrivefunctions import parse_drive
# 03/08/2026


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
def filter_list(target_files, arch_exclude, USR, MODULENAME):

    # exclude any searches results from this app using fn match
    search_files = f"*{MODULENAME}*.txt"

    # apply user filter
    rows = [(p,) for p in target_files]

    n_line = filter_lines_from_list(rows, USR, idx=0)
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


def comp_archive(target_f, target_files, archive, temp_dir, downloads, arch_exclude, USR, MODULENAME, zipPROGRAM, zipPATH, zipcmode, ziplevel, _7zipcmode, winrarcmode, strip):

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

    xdata = filter_list(target_files, arch_exclude, USR, MODULENAME)
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
                # TEMPDIR = tempfile.gettempdir()
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


def main(localappdata, action, filename, extension, basedir, USR, dspEDITOR, dspPATH, temp_dir, cutoffTIME=None, zipPROGRAM=None, zipPATH=None, USRDIR=None, downloads=None):

    if not (filename or extension):
        print("Invalid input. exiting.")
        return 1

    # localappdata = find_install()
    localappdata = Path(localappdata)
    log_path = localappdata / "logs" / "errs.log"

    toml_file, json_file, USR = get_config(localappdata, USR, platform="Windows")
    config = load_toml(toml_file)
    if not config:
        return 1
    EXCLDIRS = config['search']['EXCLDIRS']
    MODULENAME = config['paths']['MODULENAME']
    zipcmode = config['compress']['zipcmode']
    ziplevel = config['compress']['ziplevel']
    _7zipcmode = config['compress']['_7zipcmode']
    winrarcmode = config['compress']['winrarcmode']

    strip = config['compress']['strip']

    archive = MODULENAME
    tgt_file = archive + 'xfindfiles.txt'

    recent_files = os.path.join(temp_dir, tgt_file)

    target_f = archive + ".txt"
    target_f = os.path.join(temp_dir, target_f)

    target_files = []

    tmn = str(cutoffTIME)

    res = 1

    try:
        if action == "wsl":
            arge = []

            drv_letter = parse_drive(basedir)
            ch = f"/mnt/{drv_letter}"

            F = ["wsl", "find", ch]

            PRUNE = ["\\("]
            for i, d in enumerate(EXCLDIRS):
                PRUNE += ["-path", f"/mnt/{drv_letter}/{d}"]
                if i < len(EXCLDIRS) - 1:
                    PRUNE.append("-o")
            PRUNE += ["\\)", "-prune", "-o"]

            TAIL = ["-not", "-type", "d"]

            find_command = F + PRUNE  # + TAIL + arge

            if cutoffTIME is not None:
                if cutoffTIME != '0':
                    # if zero is specified it means to compress all results
                    find_command += ["-mmin", f"-{tmn}"]  # dont filter by time if zero

            if filename and not extension:
                arge = ["-iname", filename, "-print0"]
            elif not filename and extension:
                arge = ["-iname", "'*" + extension + "'", "-print0"]
            elif filename and extension:
                has_ext = bool(os.path.splitext(filename)[1])
                if has_ext:
                    print(f"Searching for {filename}{extension}\n")
                arge = ["-iname", f"{filename}*{extension}", "-print0"]  # arge = ["-iname", filename + "'*" + extension + "'", "-print"] original

            find_command += TAIL
            find_command += arge

            result_inclusion = ".txt" in filename or ".txt" in extension

            print('Running command:', ' '.join(find_command), flush=True)
            proc = subprocess.Popen(find_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            is_progress = True
            base_folder_paths = []
            try:
                for item in os.listdir(basedir):
                    b_path = os.path.join(basedir, item)
                    if os.path.isdir(b_path):
                        base_folder_paths.append(b_path)
            except (OSError, PermissionError):
                is_progress = False

            y = len(base_folder_paths)
            is_progress = y > 0

            stderr_thread = None
            stderr_output = []
            proc = subprocess.Popen(find_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            def process_stderr(stderr_pipe, sink):
                try:
                    for raw in iter(stderr_pipe.readline, b''):
                        text = raw.decode("utf-8", errors="replace").strip()
                        if text:
                            sink.append(text)
                finally:
                    stderr_pipe.close()

            if proc.stderr is not None:
                stderr_thread = threading.Thread(target=process_stderr, args=(proc.stderr, stderr_output), daemon=True)
                stderr_thread.start()

            buffer = b''

            emitted = set()
            with open(recent_files, "w", encoding="utf-8") as f1:
                while True:
                    if proc.stdout is None:
                        break
                    chunk = proc.stdout.read(8192)
                    if not chunk:
                        break
                    buffer += chunk
                    while b'\0' in buffer:
                        part, buffer = buffer.split(b'\0', 1)
                        if part.strip():

                            line = part.decode("utf-8", errors="replace")
                            if line:
                                otline = wsl_to_windows_path(line)

                                for i, prefix in enumerate(base_folder_paths, start=1):
                                    if otline.startswith(prefix):
                                        if is_progress and i not in emitted:
                                            print(f"Progress: {round((i / y) * 100, 2)}", flush=True)
                                            emitted.add(i)
                                        break

                                if not (result_inclusion and otline == recent_files):
                                    if downloads is not None:
                                        target_files.append(otline)
                                    print(otline, file=f1)
                                    print(otline, flush=True)

                if buffer.strip():
                    try:
                        otline = buffer.decode('utf-8', errors='replace')
                        if os.path.isfile(otline):
                            if downloads is not None:
                                target_files.append(otline)
                            print(otline)
                    except Exception as e:
                        print(f"fault in trailing buffer ignored. {type(e).__name__} {e}")
                        pass

            if proc.stdout is not None:
                proc.stdout.close()
            proc.wait()
            if stderr_thread is not None:
                stderr_thread.join()

            if proc.returncode not in (0, 1):
                errors = "\n".join(stderr_output)
                if errors:
                    print(errors)
                print()
                print("Find failed unable to continue. quitting")
                return proc.returncode

            res = 0

        elif action == "pwsh":

            excl_file = 'excluded.txt'
            excl_path = os.path.join(temp_dir, excl_file)
            set_excl_dirs(basedir, excl_path, EXCLDIRS)  # send excluded list to file in temp dir

            s_path = localappdata / "scripts" / "ffsearch.ps1"

            cmd = [
                "powershell.exe",
                "-ExecutionPolicy", "Bypass",
                "-File", str(s_path),
            ]
            args = ['-rootPath', basedir]
            if cutoffTIME is not None:
                if cutoffTIME != '0':
                    args.extend(['-cutoffMinutes', tmn])
                args.extend(['-archiveRs', target_f])
            args.extend(['-mergedRs', recent_files, '-excluded', excl_path, '-feedback'])  # .ps1 resolves path from tgt_file or excl_file
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

            target_files, err = parse_search(cmd, args)
            if err:
                return 1
            res = 0

        if res == 0 and os.path.isfile(recent_files) and os.path.getsize(recent_files) != 0:
            print()
            if target_files and downloads is not None:
                flth_frm = localappdata / "flth.csv"  # filter hits
                dbtarget_frm = localappdata / "recent.gpg"  # database
                CACHE_F_frm = localappdata / "ctimecache.gpg"
                CACHE_S_frm = localappdata / "systimeche.gpg"
                flth = str(flth_frm)
                dbtarget = str(dbtarget_frm)
                CACHE_F = str(CACHE_F_frm)
                CACHE_S = str(CACHE_S_frm)
                CACHE_S, _ = os.path.splitext(CACHE_S)  # to match index drives as well as CACHE_S it is `systimeche`

                # exclude certain files from .rar/.zip. app inclusions and temp work area

                arch_exclude = get_runtime_exclude_list(
                    localappdata, USRDIR, MODULENAME, flth, dbtarget, CACHE_F,
                    CACHE_S, str(log_path), recent_files, temp_dir=temp_dir
                )

                res = comp_archive(
                    target_f, target_files, archive, temp_dir, downloads, arch_exclude, USR,
                    MODULENAME, zipPROGRAM, zipPATH, zipcmode, ziplevel, _7zipcmode,
                    winrarcmode, strip
                )

            elif downloads and cutoffTIME is not None:
                res = 1
                print(f"no archive path list: {target_f} couldnt compress")
                if action == "pwsh" and target_files is None:
                    print("\nNo return from powershell script. quitting")

            display(dspEDITOR, recent_files, True, dspPATH)

        if res == 0:
            print("Progress: 100%")
        return res

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
        args.USRDIR,
        args.downloads
    ]
    result = main(*calling_args)
    sys.exit(result)


if __name__ == "__main__":
    main_entry(sys.argv[1:])
