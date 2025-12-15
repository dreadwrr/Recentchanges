#!/usr/bin/env python3
#   Porteus                                                                           12/14/2025
#   recentchanges. Developer buddy      `recentchanges`/ `recentchanges search`
#   Provide ease of pattern finding ie what files to block we can do this a number of ways
#   1) if a file was there (many as in more than a few) and another search lists them as deleted its either a sys file or not but unwanted nontheless
#   2) Is a system file inherent to the specifc platform
#   3) intangibles ie trashed items that may pop up infrequently and are not known about
#
#   This script is called by two methods. recentchanges and recentchanges search. The former is discussed below
#
#   `recentchanges` make xzm
#           Searches are saved in /tmp
#           1. Search results are unfiltered and copied files for the .xzm are from a filter.
#
#           The purpose of this script is to save files ideally less than 5 minutes old. So when compiling or you dont know where some files are
#   or what changed on your system. So if you compiled something you call this script to build a module of it for distribution. If not using for developing
#   call it a file change snapshot
#   We use the find command to list all files 5 minutes or newer. Filter it and then get to copying the files in a temporary staging directory.
#   Then take those files and make an .xzm. It will be placed in   /tmp  along with a transfer log to staging directory and file manifest of the xzm
#
#   `recentchanges search`
#           Searches are saved in /home/{user}/Downloads
#
#           This has the same names as `recentchanges` but also includes /tmp files and or a filesearch.
#           1. old searches can be grabbed from /Downloads, /tmp or /tmp/{MODULENAME}_MDY. for convenience if there is no differences it displays the old search for specified search criteria
#           2. The search is unfiltered and a filesearch is filtered.
#           2. rnt search inverses the results. For a standard search it will filter the results. For a file search it removes the filter.
#
#           Windows
#           This has the same names as `recentchanges` but also includes /tmp files and or a filesearch.
#           1. old searches can be grabbed from Desktop,C:\users\{user}\AppData\Local\save-changesnew\, C:\users\{user}\AppData\Local\save-changesnew\{MODULENAME}_MDY\. for convenience
#           if there is no differences it displays the old search for specified search criteria
#           2. The search is unfiltered and a filesearch is filtered.
#           2. rnt search inverses the results. rnt.bat   ie for a standard search it will filter the results. For a file search it removes the filter.
#  Also borrowed script features from various scripts on porteus forums
import os
import re
import signal
import sys
import tempfile
import time
from datetime import datetime, timedelta
from . import processha
from .filterhits import update_filter_csv
from .dirwalker import scan_system
from .fsearchfnts import set_excl_dirs
from .pstsrg import main as pst_srg
from .qtfunctions import setup_drive_settings
from .recentchangessearchparser import build_parser
from .pyfunctions import cprint
from .pyfunctions import dict_string
from .pyfunctions import dict_to_list_sys
from .pyfunctions import get_wdir
from .pyfunctions import is_integer
from .pyfunctions import load_config
# os.path.dirname(os.path.abspath(os.sys.argv[0])) // filter.py originally beside main.py
# script_path = os.path.abspath(__file__)
# script_dir = os.path.dirname(script_path)
# parent_dir = os.path.dirname(script_dir)
# sys.path.insert(0, parent_dir)
from .rntchangesfunctions import build_tsv
from .rntchangesfunctions import clear_logs
from .rntchangesfunctions import convertn
from .rntchangesfunctions import decr_ctime
from .rntchangesfunctions import display
from .rntchangesfunctions import encrm
from .rntchangesfunctions import filter_lines_from_list
from .rntchangesfunctions import filter_output
from .rntchangesfunctions import find_cmdhelp
from .rntchangesfunctions import find_user_folder
from .rntchangesfunctions import find_files
from .rntchangesfunctions import find_mft
from .rntchangesfunctions import find_ps1
from .rntchangesfunctions import findwsl
from .rntchangesfunctions import genkey
from .rntchangesfunctions import get_diffFile
from .rntchangesfunctions import get_runtime_exclude_list
from .rntchangesfunctions import hsearch
from .rntchangesfunctions import iskey
from .rntchangesfunctions import intst
from .rntchangesfunctions import logic
from .rntchangesfunctions import mftec_is_cutoff
from .rntchangesfunctions import output_results_exit
from .rntchangesfunctions import removefile
from .rntchangesfunctions import update_toml_setting


# Globals
stopf = False
is_calibrate = False  # use Mft backend to see if results from WSL and powershell are the same.
calibrate_output = False  # Output results after search and exit early to compare WSL, powershell and Mft
# Last calibrate date: 11/30/2025


def sighandle(signum, frame):
    global stopf
    if signum == 2:
        stopf = True
        sys.exit()  # ctrl-c


signal.signal(signal.SIGINT, sighandle)
signal.signal(signal.SIGTERM, sighandle)

'''
 init 0 - 20 %
 main search 20 - 60 %
 processing 60 - 65%
 pstsrg 65% - 90%
 pstsrg with POSTOP 65 - 85%
 pstsrg with scanIDX 65 - 80%
 pstsrg with POSTOP and scanIDX 65 - 75%
'''


def main(argone, argtwo, USR, PWD, argf="bnk", method="", iqt=False, db_output=None, POST_OP=False, scan_idx=False, showDiff=False, argwsl=False, dspPATH=None):

    temp = os.environ.get('TEMP')
    tmp = os.environ.get('TMP')
    systemp = r"C:\Windows\Temp"

    appdata_local = get_wdir()  # appdata software install aka workdir

    script_dir = appdata_local / "scripts"
    toml_file = appdata_local / "config" / "config.toml"
    json_file = appdata_local / "config" / "usrprofile.json"

    config = load_config(toml_file)  # setup_logger(process_label="RECENTCHANGES", wdir=appdata_local)
    FEEDBACK = config['analytics']['FEEDBACK']
    ANALYTICSECT = config['analytics']['ANALYTICSECT']
    email = config['backend']['email']
    email_name = config['backend']['name']
    checksum = config['diagnostics']['checkSUM']
    cdiag = config['diagnostics']['cdiag']
    scanIDX = config['diagnostics']['scanIDX']
    supbrw = config['diagnostics']['supbrw']
    supress = config['diagnostics']['supress']
    POSTOP = config['diagnostics']['POSTOP']
    ps = config['diagnostics']['proteusSHIELD']  # proteus shield
    updatehlinks = config['diagnostics']['updatehlinks']
    show_diff = config['diagnostics']['showDIFF']
    indexCACHEDIR = config['diagnostics']['proteus_CACHE']
    dspEDITOR = config['display']['dspEDITOR']
    compLVL = config['logs']['compLVL']
    MODULENAME = config['paths']['MODULENAME']
    flth_frm = appdata_local / "flth.csv"  # flth = res_path(config['paths']['flth'], USR)  # filter hits
    dbtarget_frm = appdata_local / "recent.gpg"  # res_path(config['paths']['pydbpst'], USR) C:\\Users\\{{user}}\\AppData\\Local\\save-changesnew\\recent.gpg  #
    CACHE_F_frm = appdata_local / "ctimecache.gpg"  # res_path(config['paths']['CACHE_F'], USR) CACHE_F = "C:\\Users\\{{user}}\\AppData\\Local\\save-changesnew\\ctimecache.gpg" # .         file cache
    CACHE_S_frm = appdata_local / "systimeche.gpg"  # res_path(config['paths']['CACHE_S'], USR) CACHE_S = "C:\\Users\\{{user}}\\AppData\\Local\\save-changesnew\\systimeche.gpg" # .         sys profile cache
    flth = str(flth_frm)
    dbtarget = str(dbtarget_frm)
    CACHE_F = str(CACHE_F_frm)
    CACHE_S = str(CACHE_S_frm)
    archivesrh = config['search']['archivesrh']
    basedir = config['search']['drive']  # main drive for search
    EXCLDIRS = config['search']['EXCLDIRS']
    ll_level = config['search']['logLEVEL']
    DRIVETYPE = config['search']['modelTYPE']
    wsl = config['search']['wsl']

    escaped_user = re.escape(USR)

    # uid = pwd.getpwnam(USR).pw_uid   linux     import grp     import pwd
    # gid = grp.getgrnam("root").gr_gid

    # init
    DRIVETYPE = setup_drive_settings(basedir, DRIVETYPE, json_file, toml_file)
    if DRIVETYPE is None:
        print("Unable to locate drive from setting [search] drive. see config.toml value", basedir)
        sys.exit(1)

    if iqt:
        show_diff = showDiff
        POSTOP = POST_OP
        scanIDX = scan_idx
        wsl = argwsl

    iswsl = False
    if wsl:
        iswsl = findwsl(toml_file)

    # end init

    TMPOUTPUT = []  # holding
    # Searches
    RECENT = []  # main results
    tout = []  # ctime results
    SORTCOMPLETE = []  # combined
    TMPOPT = []  # combined filtered
    # NSF
    COMPLETE_1 = []
    COMPLETE_2 = []
    COMPLETE = []  # combined
    # Diff file
    difff_file = []
    ABSENT = []  # actions
    rout = []  # actions from ha

    cfr = []  # cache dict

    start = 0
    end = 0
    cstart = 0
    cend = 0

    ag = 0

    diffrlt = False
    nodiff = False
    syschg = False
    flsrh = False
    validrlt = None

    dcr = True  # means to remove after encrypting. and backwards for this script dcr True is meant to mean leave open

    tmn = None
    search_time = None
    filename = None

    dbopt = None

    flnm = ""
    parseflnm = ""
    diffnm = ""

    filepath = ""
    DIRSRC = ""

    mergeddb = "recent_merged.db"
    excl_file = 'excluded.txt'  # find and powershell directory excludes from EXCLDIRS
    tsv_doc = "doctrine.tsv"

    proval = 20  # progress
    endval = 30

    fmt = "%Y-%m-%d %H:%M:%S"

    USRDIR = find_user_folder("Desktop")
    if USRDIR is None:
        raise EnvironmentError("Could not find user Desktop folder")
    # DOC_S = find_user_folder("Documents")

    tgt = basedir.split(":", 1)[0].lower()  # dynamic directory exclusion for WSL
    F = ["wsl", "find", f"/mnt/{tgt}"]
    PRUNE = ["\\("]
    for i, d in enumerate(EXCLDIRS):
        PRUNE += ["-path", f"/mnt/{tgt}/{d.replace('$', '\\$')}"]
        if i < len(EXCLDIRS) - 1:
            PRUNE.append("-o")
    PRUNE += ["\\)", "-prune",  "-o"]

    TAIL = ["-not", "-type", "d", "-printf", "%T@ %A@ %C@ %i %s %u %g %m %p\\0"]

    mmin = []
    cmin = []

    TEMPD = tempfile.gettempdir()

    with tempfile.TemporaryDirectory(dir=TEMPD) as mainl:

        if is_calibrate:

            c_ver = mftec_is_cutoff(appdata_local)
            if not c_ver:
                print("Mft requires --cutoff argument .NET 9 version to print to stdout", flush=True)
                return 1

        slog = os.path.join(mainl, "scr")  # feedback
        cerr = os.path.join(mainl, "cerr")  # priority

        if not iskey(email):
            if not genkey(email, email_name, TEMPD):
                print("Failed to generate a gpg key. quitting")
                return 1

        cfr = decr_ctime(CACHE_F)

        start = time.time()

        # initialize

        if argone != "search":
            THETIME = argone
        else:
            THETIME = argtwo

        # search criteria
        if THETIME != "noarguser":
            p = 60
            try:
                argone = int(THETIME)
                tmn = convertn(argone, p, 2)
                search_time = tmn
                cprint.cyan(f"Searching for files {argone} seconds old or newer")

            except ValueError:  # its a file search

                if not os.path.isdir(PWD):
                    print(f'Invalid argument {PWD}. PWD required.')
                    sys.exit(1)
                os.chdir(PWD)

                filename = argtwo  # sys.argv[2]
                if not os.path.isfile(filename) and not os.path.isdir(filename):
                    print('No such directory, file, or integer.')
                    sys.exit(1)

                _, ext = os.path.splitext(filename)  # Windows as .txt isnt used for linux
                argone = ".txt"  # compare by TMPOUTPUT
                if ext.lower() == ".txt":
                    argone = ""

                parseflnm = os.path.basename(filename)
                if not parseflnm:  # get directory name
                    # parseflnm = filename.rstrip('/').split('/')[-1] #linux directory linux
                    parseflnm = filename.rstrip("/\\").split("/")[-1].split("\\")[-1]

                cprint.cyan(f"Searching for files newer than {filename}")
                flsrh = True
                ct = int(time.time())
                frmt = int(os.stat(filename).st_mtime)
                ag = ct - frmt
                ag = convertn(ag, p, 2)
                search_time = ag
                # if iswsl:
                #     mmin = ["-mmin", f"-{search_time}"]
                #     cmin = ["-cmin", f"-{search_time}"]

        else:
            tmn = search_time = argone = 5
            cprint.cyan('Searching for files 5 minutes old or newer\n')

        if iswsl and tout:
            mmin = ["-mmin", f"-{search_time}"]
            cmin = ["-amin", f"-{search_time}"]

        if iqt:
            print(f"Progress: {proval}", flush=True)

        # sys.stdout.flush()

        logging_values = (appdata_local, ll_level, mainl)

        if is_calibrate:

            endval += 30
            init = True
            RECENT, COMPLETE, end, cstart = find_mft(DRIVETYPE, RECENT, COMPLETE, init, checksum, cfr, FEEDBACK, logging_values, end, cstart, search_time, iqt=iqt, strt=proval, endp=endval)

        # Windows default - Powershell
        elif not iswsl:
            endval += 15

            merged_database = os.path.join(mainl, mergeddb)  # results from powershell search in tempdir app is in
            excl_path = os.path.join(mainl, excl_file)
            set_excl_dirs(basedir, excl_path, EXCLDIRS)  # write exclude list to tempdir this app is in

            s_path = os.path.join(script_dir, "scanline.ps1")

            # single process like find command
            # 19s for system scan
            command = [
                "powershell.exe",
                "-ExecutionPolicy", "Bypass",
                "-File", s_path,
                "-rootPath", basedir,
                "-cutoffMinutes", str(search_time),
                "-mergedRs", merged_database,
                "-excluded", excl_path,  # dynamic directory exclusion pwsh
                "-StartR", str(proval),
                "-EndR", str(endval)
            ]

            if FEEDBACK:
                command += ["-feedback"]

            proval += 15
            endval += 15
            init = True

            RECENT, COMPLETE, end, cstart = find_ps1(command, DRIVETYPE, RECENT, COMPLETE_1, merged_database, init, checksum, updatehlinks, cfr, FEEDBACK, logging_values, end, cstart, iqt=iqt, strt=proval, endp=endval)

        # WSL find command
        else:

            cmin = ["-amin", f"-{search_time}"]
            current_time = datetime.now()
            search_start_dt = (current_time - timedelta(minutes=search_time))

            # minor areas find cant reach with powershell first
            mmin_files = []
            cmin_files = []
            if basedir == "C:\\":
                s_path = os.path.join(script_dir, "find_files.ps1")
                mmin_files, cmin_files = find_cmdhelp(s_path, search_time, USR)

            if not tout:
                find_command_cmin = F + PRUNE + cmin + TAIL
                init = True
                tout, COMPLETE_2, end, cstart = find_files(find_command_cmin, DRIVETYPE, cmin_files, "ctime", tout, COMPLETE_2, init, checksum, updatehlinks, cfr, FEEDBACK, logging_values, end, cstart, search_start_dt, iqt=iqt, strt=proval, endp=endval)  # mmin USR used for files find cant reach via powershell
                cmin_end = time.time()
                cmin_start = current_time.timestamp()
                cmin_offset = convertn(cmin_end - cmin_start, 60, 2)

                mmin = ["-mmin", f"-{search_time + cmin_offset:.2f}"]
                find_command_mmin = F + PRUNE + mmin + TAIL
                proval += 10
                endval += 30
                init = False
                RECENT, COMPLETE_1, end, cstart = find_files(find_command_mmin, DRIVETYPE, mmin_files, "main", RECENT, COMPLETE_1, init, checksum, updatehlinks, cfr, FEEDBACK, logging_values, end, cstart, search_start_dt, iqt=iqt, strt=proval, endp=endval)  # bypass ctime loop if xRC
            else:
                find_command_mmin = F + PRUNE + mmin + TAIL
                init = True
                endval += 30
                RECENT, COMPLETE_1, end, cstart = find_files(find_command_mmin, DRIVETYPE, mmin_files, "main", RECENT, COMPLETE_1, init, checksum, updatehlinks, cfr, FEEDBACK, logging_values, end, cstart, search_start_dt, iqt=iqt, strt=proval, endp=endval)  # bypass ctime loop if xRC
        cend = time.time()
        if iqt:
            print(f"Progress: {endval + 1}%")

        if RECENT:
            if cfr:  # savecache

                data_to_write = dict_to_list_sys(cfr)
                ctarget = dict_string(data_to_write)

                nc_cfile = intst(dbtarget, compLVL)

                rlt = encrm(ctarget, CACHE_F, email, nc_cfile, False)
                if not rlt:
                    print("Reencryption failed cache not saved.")

        else:
            print("No new files exiting.")
            return 0

        COMPLETE = COMPLETE_1 + COMPLETE_2  # nsf append to rout
        proval = 60  # current progress
        endval = 90  # next

        SORTCOMPLETE = RECENT

        # get everything from the start time

        SORTCOMPLETE.sort(key=lambda x: x[0])

        SRTTIME = SORTCOMPLETE[0][0]
        merged = SORTCOMPLETE[:]

        for entry in tout:
            tout_dt = entry[0]
            if tout_dt >= SRTTIME:
                merged.append(entry)
        merged.sort(key=lambda x: x[0])

        # the start time is stored before appending ctime results
        seen = {}

        for entry in merged:
            if len(entry) < 11:
                continue
            timestamp_truncated = entry[0]
            filepath = entry[1]
            cam_flag = entry[10]

            key = (timestamp_truncated, filepath)

            if key not in seen:
                seen[key] = entry
            else:
                existing_entry = seen[key]
                existing_cam = existing_entry[10]

                # Prefer non change as modified time
                if existing_cam == "y" and cam_flag is None:
                    seen[key] = entry

        deduped = list(seen.values())

        # inclusions from this script /  sort -u
        exclude_patterns = get_runtime_exclude_list(dbtarget, appdata_local, mainl, MODULENAME, USRDIR, flth, CACHE_F, CACHE_S, db_output=db_output)

        def filepath_included(filepath, exclude_patterns):
            filepath = filepath.lower()
            return not any(filepath.startswith(p.lower()) for p in exclude_patterns)

        SORTCOMPLETE = [
            entry for entry in deduped
            if filepath_included(entry[1], exclude_patterns)
        ]

        # mac os
        # hardlinks?
        # if updatehlinks:
        #     cprint.green('Updating hardlinks')
        #     SORTCOMPLETE = ulink(SORTCOMPLETE, MODULENAME, supbrw)

        # get everything before the end time
        if not flsrh:
            start_dt = SRTTIME
            range_sec = 300 if THETIME == 'noarguser' else int(THETIME)
            end_dt = start_dt + timedelta(seconds=range_sec)
            lines = [entry for entry in SORTCOMPLETE if entry[0] <= end_dt]
        else:
            lines = SORTCOMPLETE

        if calibrate_output:
            output_results_exit(RECENT, argone, is_calibrate, iswsl, fmt)

        # remove all tmp folders if it is `recentchanges` (method "rnt") if it is `recentchanges search` (method "") put them in a seperate file
        # C:\\Users\\{user}\\AppData\\Local\\Temp\\
        # r'C:\Windows\Temp'
        patterns = tuple(p for p in (systemp, temp, tmp) if isinstance(p, str) and p)
        tmp_lines = []         # amended from original
        non_tmp_lines = []      # .

        # filter out the Temp files
        for entry in lines:
            if entry[1].startswith(patterns):
                tmp_lines.append(entry)
            else:
                non_tmp_lines.append(entry)

        # tmp_lines = [entry for entry in lines if entry[1].startswith(patterns)]    original
        # non_tmp_lines = [entry for entry in lines if not entry[1].startswith(patterns)]

        SORTCOMPLETE = non_tmp_lines
        TMPOUTPUT = tmp_lines

        filtered_lines = []
        for entry in SORTCOMPLETE:
            ts_str = entry[0]
            filepath = entry[1]
            filtered_lines.append((ts_str, filepath))

        TMPOPT = filtered_lines  # human readable
        RECENT = TMPOPT[:]

        # Apply filter. RECENT is unfiltered and stored in db

        TMPOPT = filter_lines_from_list(TMPOPT, escaped_user)

        logf = []
        logf = RECENT
        if tmn:
            logf = RECENT  # all files
        if method != "rnt":
            if argf == "filtered" or flsrh:
                logf = TMPOPT  # filtered
                if argf == "filtered" and flsrh:
                    logf = RECENT  # dont filter inverse

        # Merge/Move old searches
        if SORTCOMPLETE:
            syschg = True
            OLDSORT = []
            if flsrh:
                flnm = f'xNewerThan_{parseflnm}{argone}'
                flnmdff = f'xDiffFromLast_{parseflnm}{argone}'
            elif argf == "filtered":
                flnm = f'xFltchanges_{argone}.txt'
                flnmdff = f'xFltDiffFromLastSearch_{argone}.txt'
            else:
                flnm = f'xSystemchanges{argone}.txt'
                flnmdff = f'xSystemDiffFromLastSearch{argone}.txt'

            if method == "rnt":
                DIRSRC = appdata_local  # 'recentchanges'     Either \AppData or desktop
            else:
                DIRSRC = USRDIR  # 'search'

            # is old search?
            filepath = os.path.join(DIRSRC, f'{MODULENAME}{flnm}')

            if os.path.isfile(filepath):
                with open(filepath, 'r') as f:
                    OLDSORT = f.readlines()

            # tryAppData\Local\save-changesnew\ for previous search
            if not OLDSORT and not flsrh and argf != "filtered" and method != "rnt":
                fallback_path = os.path.join(appdata_local, f'{MODULENAME}{flnm}')
                if os.path.isfile(fallback_path):
                    with open(fallback_path, 'r') as f:
                        OLDSORT = f.readlines()

            # try searches AppData\Local\save-changesnew\MODULENAME_MDY*
            if not OLDSORT:
                hsearch(OLDSORT, appdata_local, MODULENAME, argone)

            # Move or clear previous searches
            validrlt = clear_logs(USRDIR, DIRSRC, appdata_local, MODULENAME, method, archivesrh)

            if method != "rnt":
                # send \Temp results to user
                if TMPOUTPUT:
                    # b_argone = str(argone).replace('.txt', '') if str(argone).endswith('.txt') else str(argone)   linux
                    target_filename = f"{MODULENAME}xSystemTmpfiles{parseflnm}{argone}"
                    if is_integer(argone):  # windows
                        target_filename += ".txt"

                    target_path = os.path.join(USRDIR, target_filename)
                    with open(target_path, 'w') as dst:
                        for entry in TMPOUTPUT:
                            tss = entry[0].strftime(fmt)
                            fp = entry[1]
                            dst.write(f'{tss} {fp}\n')
                    # changeperm(target_path, uid)

            diffnm = os.path.join(DIRSRC, MODULENAME + flnmdff)

            # Difference file
            if OLDSORT:
                nodiff = True

                clean_oldsort = [line.strip() for line in OLDSORT]
                clean_logf_set = set(f'{entry[0].strftime(fmt)} {entry[1]}' for entry in logf)
                difff_file = [line for line in clean_oldsort if line not in clean_logf_set]

                if difff_file:
                    diffrlt = True
                    with open(diffnm, 'w') as file2:
                        for entry in difff_file:
                            print(entry, file=file2)
                        file2.write("\n")

                    # preprocess before db/ha. The differences before ha and then sent to processha after ha
                    processha.isdiff(SORTCOMPLETE, ABSENT, rout, diffnm, difff_file, flsrh, SRTTIME, fmt)

            # Send search result SORTCOMPLETE to user
            with open(filepath, 'w') as f:
                for entry in logf:
                    tss = entry[0].strftime(fmt)
                    fp = entry[1]
                    f.write(f'{tss} {fp}\n')
            # changeperm(filepath, uid) linux

            # 65% - 90%   normal for finishing pstsrg

            # Backend
            # file doctrine
            if POSTOP:
                endval = 85  # adjust 65% - 85%

            if scanIDX or iqt:
                dcr = False  # leave open as there is a system scan after
                if scanIDX:
                    endval = 80  # adjust 65% - 80%

            if POSTOP and scanIDX:
                endval = 75

            proval = 65

            dbopt = pst_srg(db_output, dbtarget, basedir, DRIVETYPE, SORTCOMPLETE, COMPLETE, logging_values, rout, checksum, updatehlinks, cdiag, email, ANALYTICSECT, ps, indexCACHEDIR, CACHE_S, compLVL, mainl, USR, dcr=dcr, iqt=iqt, strt=proval, endp=endval)
            proval = endval + 1
            endval = 100
            if iqt:
                print(f"Progress: {proval}")
            if not dbopt:
                print("There is a problem in pst_srg no return value. likely database wasnt created, path to database did not exist or permission issue")
                return 1

            if ANALYTICSECT:
                el = end - start
                print(f'Search took {el:.3f} seconds')
                if checksum:
                    el = cend - cstart
                    print(f'Checksum took {el:.3f} seconds')
                print()
            # Diff output to user
            csum = processha.processha(rout, ABSENT, diffnm, cerr, flsrh, argf, SRTTIME, escaped_user, supbrw, supress)

            # Filter hits
            update_filter_csv(RECENT, flth, escaped_user)
            sys.stdout.flush()

            # Terminal output process scr/cer
            if not csum and not supress:
                if os.path.exists(slog):  # escaped_user
                    filter_output(slog, escaped_user, 'Checksum', 'no', 'blue', 'yellow', 'scr', supbrw)

            if csum:
                if os.path.isfile(cerr):
                    with open(cerr, 'r') as src, open(diffnm, 'a') as dst:
                        dst.write("\ncerr\n")
                        for line in src:
                            if line.startswith("Warning File"):
                                continue
                            dst.write(line)
                    removefile(cerr)

        try:

            logic(syschg, nodiff, diffrlt, validrlt, appdata_local, MODULENAME, THETIME, argone, argf, filename, flsrh, method)  # feedback
            display(dspEDITOR, filepath, syschg, dspPATH)  # open text editor?
        except Exception as e:
            print(f"Error in logic or display {type(e).__name__} : {e} ")

        # Cleanup

        # if os.path.isfile(diffnm):   linux only
        #     changeperm(diffnm, uid, gid)

        if os.path.isfile(slog):
            removefile(slog)

        if not iswsl:  # powershellcleanup
            database_merged = os.path.join(mainl, mergeddb)
            removefile(database_merged)

        if POSTOP:  # File doctrine
            outpath = os.path.join(USRDIR, tsv_doc)
            if not os.path.isfile(outpath):
                if build_tsv(SORTCOMPLETE, rout, outpath):
                    cprint.green(f"File doctrine.tsv created {USRDIR}\\{tsv_doc}")
            elif not iqt:
                update_toml_setting('diagnostics', 'POSTOP', False, toml_file)  # one was already made disable the setting

        if dbopt not in ("new_profile", "encr_error") and scanIDX:  # Scan system index. If it is from the command line and a new profile was just made dont scan it. Encryption failure dont scan as there is a problem.

            cprint.green('Running POSTOP system index scan.')

            # append to old or use new default
            if diffrlt:
                diff_file = diffnm
            else:
                diff_file = get_diffFile(appdata_local, USRDIR, MODULENAME)

            if not iqt:
                dcr = True  # for command line remove the database after system scan and encrypting changes

            rlt = scan_system(dbopt, dbtarget, basedir, diff_file, updatehlinks, CACHE_S, email, ANALYTICSECT, show_diff, indexCACHEDIR, compLVL, dcr=dcr, iqt=iqt, strt=proval, endp=endval)

            if not iqt:  # from commandline, turn off so doesnt scan every time
                update_toml_setting('diagnostics', 'scanIDX', False, toml_file)

            if rlt != 0:
                if rlt == 1:
                    print("Post op index scan failed scan_system dirwalker.py")
                    return 1
                if rlt == 7:
                    if not iqt:
                        print("No profile created. set proteusSHIELD to create profile")
                    else:
                        print("No profile created. run build IDX on pg2")
                else:
                    print(f"Unexpected error scan_system : error code {rlt}")
                    return rlt

        if iqt:
            print("Progress: 100%", flush=True)
        return 0


def main_entry(argv):
    parser = build_parser()
    args = parser.parse_args(argv)

    calling_args = [
        args.argone,
        args.argtwo,
        args.USR,
        args.PWD,
        args.argf,
        args.method,
        args.iqt,
        args.db_output,
        args.POST_OP,
        args.scan_idx,
        args.showDiff,
        args.argwsl,
        args.dspPATH
    ]

    result = main(*calling_args)
    sys.exit(result)


# if __name__ == "__main__":
#     main_entry(sys.argv[1:])
