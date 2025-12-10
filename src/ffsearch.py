import fnmatch
import os
import re
import shutil
import subprocess
import sys
import traceback
from .ffsearchparser import build_parser
from .fsearchfnts import set_excl_dirs
from .pyfunctions import cprint
from .pyfunctions import get_wdir
from .pyfunctions import load_config
from .rntchangesfunctions import display
from .rntchangesfunctions import get_runtime_exclude_list
from .rntchangesfunctions import removefile
from .rntchangesfunctions import run_pwsh
from .rntchangesfunctions import wsl_to_windows_path
from .rntchangesfunctions import filter_lines_from_list
# 12/08/2025


def has_content(recent_files):
    try:
        with open(recent_files, 'r', encoding='utf-8') as f:
            return f.readline().strip() != ''
    except OSError:
        return False


# add " " to the paths
def encase_line(recent_files, arch_exclude, USR, MODULENAME):

    # exclude any searches results from this app using fn match
    search_files = f"*{MODULENAME}*.txt"

    with open(recent_files, 'r', encoding='utf-8') as f:
        n_line = [line.strip() for line in f if line.strip()]

    # apply user filter
    escaped_user = re.escape(USR)
    n_line = filter_lines_from_list(n_line, escaped_user)

    with open(recent_files, "w", encoding='utf-8') as f:
        for line in n_line:

            line_path = line.lower()

            # filter out inclusions from the app
            if any(line_path.startswith(excl.lower()) for excl in arch_exclude):
                continue

            if fnmatch.fnmatch(line, search_files):
                continue

            encased_line = f'"{line}"'
            f.write(encased_line + "\n")


def comp_archive(recent_files, archive, arch_exclude, USR, MODULENAME, zipPROGRAM, zipPATH, downloads):

    res = 1

    if zipPROGRAM == "7zip":
        relative_flg = "-spf"
        archflnm = archive + ".zip"
    elif zipPROGRAM == "winrar":
        # relative_flg = "-ep1"
        archflnm = archive + ".rar"
    else:
        print("Unrecognized zip program skipping archive.")
        return 1

    encase_line(recent_files, arch_exclude, USR, MODULENAME)

    out_file = os.path.join(downloads, archflnm)
    removefile(out_file)
    try:
        cmd = [zipPATH, "a"]
        if zipPROGRAM == "7zip":
            cmd += [relative_flg]
        cmd += [out_file, f"@{recent_files}"]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True
        )
        rlt = result.returncode
        if rlt != 0:
            print(result.stdout)
            print(result.stderr)
        if rlt == 0:
            res = 0
            cprint.cyan(f"Archive created in: {out_file}")
    except FileNotFoundError as e:
        print(f"The file list for the archive is missing {recent_files}. Error running zip program {zipPROGRAM} at {zipPATH} err: {e}")
    except Exception as e:
        print(f"An unexpected error happened while trying to compress {out_file}. {type(e).__name__} {e} traceback:\n {traceback.format_exc()}")

    return res


def main(action, filename, extension, basedir, USR, dspEDITOR, dspPATH, tempdir, cutoffTIME=None, zipPROGRAM=None, zipPATH=None, USRDIR=None, downloads=None):

    res = 1

    if not (filename or extension):
        print("Invalid input. exiting.")
        return 1

    localappdata = get_wdir()
    toml = localappdata / "config" / "config.toml"
    config = load_config(toml)
    EXCLDIRS = config['search']['EXCLDIRS']
    MODULENAME = config['paths']['MODULENAME']

    archive = MODULENAME
    tgt_file = archive + 'xfindfiles.txt'

    recent_files = os.path.join(tempdir, tgt_file)

    target_f = archive + ".txt"
    target_f = os.path.join(tempdir, target_f)

    tmn = str(cutoffTIME)

    pwsh_result = None

    try:
        if action == "wsl":
            arge = []

            drv_letter = basedir.split(":", 1)[0].lower()
            ch = f"/mnt/{drv_letter}"

            F = ["wsl", "find", ch]

            PRUNE = ["\\("]
            for i, d in enumerate(EXCLDIRS):
                PRUNE += ["-path", f"/mnt/{drv_letter}/{d.replace('$', '\\$')}"]
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
                arge = ["-iname", filename, "-print"]
            elif not filename and extension:
                arge = ["-iname", "'*" + extension + "'", "-print"]
            elif filename and extension:
                has_ext = bool(os.path.splitext(filename)[1])
                if has_ext:
                    print(f"Searching for {filename}{extension}\n")
                arge = ["-iname", f"{filename}*{extension}", "-print"]  # arge = ["-iname", filename + "'*" + extension + "'", "-print"] original

            find_command += TAIL
            find_command += arge

            print('Running command:', ' '.join(find_command))
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

            with open(recent_files, "w", encoding="utf-8") as f:
                for line in iter(proc.stdout.readline, b''):

                    line = line.decode('utf-8').strip()
                    if line:
                        otline = wsl_to_windows_path(line)

                        for i, prefix in enumerate(base_folder_paths, start=1):
                            if otline.startswith(prefix):
                                if is_progress:
                                    print(f"Progress: {round((i / y) * 100, 2)}", flush=True)
                                    break

                        print(otline)
                        print(otline, file=f)

            proc.stdout.close()

            stderr_lines = [line.decode('utf-8').strip() for line in proc.stderr.readlines() if line.strip()]
            proc.stderr.close()
            proc.wait()

            if proc.returncode not in (0, 1):
                stderr_out = "\n".join(stderr_lines)
                if stderr_out:
                    print(stderr_out)
                print()
                print("Find failed unable to continue. quitting")
                return proc.returncode

            res = 0

            if os.path.isfile(recent_files) and cutoffTIME is not None:
                shutil.copy(recent_files, target_f)

        elif action == "pwsh":

            excl_file = 'excluded.txt'
            excl_path = os.path.join(tempdir, excl_file)
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
            args.extend(['-mergedRs', recent_files, '-excluded', excl_path])  # .ps1 resolves path from tgt_file or excl_file
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

            cmd += args
            pwsh_result, end = run_pwsh(cmd)
            if not pwsh_result and not end:
                return 1
            if end:
                res = 0

        if res == 0 and os.path.isfile(recent_files) and os.path.getsize(recent_files) != 0:

            if os.path.isfile(target_f) and cutoffTIME is not None:
                flth_frm = localappdata / "flth.csv"  # filter hits
                dbtarget_frm = localappdata / "recent.gpg"  # database
                CACHE_F_frm = localappdata / "ctimecache.gpg"
                CACHE_S_frm = localappdata / "systimeche.gpg"

                flth = str(flth_frm)
                dbtarget = str(dbtarget_frm)
                CACHE_F = str(CACHE_F_frm)
                CACHE_S = str(CACHE_S_frm)
                CACHE_S, _ = os.path.splitext(CACHE_S)  # to match index drives as well as CACHE_S it is `systimeche`

                arch_exclude = get_runtime_exclude_list(dbtarget, localappdata, tempdir, MODULENAME, USRDIR, flth, CACHE_F, CACHE_S)  # exclude certain files from archive .rar/.zip.  app inclusions and temp work area

                res = comp_archive(target_f, archive, arch_exclude, USR, MODULENAME, zipPROGRAM, zipPATH, downloads)

            elif cutoffTIME is not None:
                res = 1
                print(f"no archive path list: {target_f} couldnt compress")
                if action == "pwsh" and pwsh_result is None:
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


# if __name__ == "__main__":
#     main_entry(sys.argv[1:])
