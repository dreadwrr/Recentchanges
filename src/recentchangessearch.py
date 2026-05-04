#! python3
#   Windows 10 / 11                                                                05/03/2026
#   recentchanges. Developer buddy      recentchanges/ recentchanges search
#   Provide ease of pattern finding ie what files to block we can do this a number of ways
#   1) if a file was there (many as in more than a few) and another search lists them as deleted its either a sys file or not but unwanted nontheless
#   2) Is a system file inherent to the specifc platform
#   3) intangibles ie trashed items that may pop up infrequently and are not known about
#
#   This script is called by two methods. recentchanges and recentchanges search. The former is discussed below
#
#   recentchanges make xzm
#           Searches are saved in /tmp Linux and <app_install> windows
#           1. Search results are unfiltered and copied files for the .xzm are from a filter. for windows the filter is used for filtered searches only
#
#           The purpose of this script is to save files ideally less than 5 minutes old. So when compiling or you dont know where some files are
#   or what changed on your system. So if you compiled something you call this script to build a module of it for distribution. If not using for developing
#   call it a file change snapshot
#   We use the find command to list all files 5 minutes or newer. Filter it and then get to copying the files in a temporary staging directory.
#   Then take those files and make an .xzm. It will be placed in /tmp along with a transfer log to staging directory and file manifest of the xzm
#
#   recentchanges search

#           This has the same name as recentchanges but also includes the tmp files and or a filesearch.
#           1. old searches can be grabbed from Desktop, <app_install>, <app_install>\\{moduleNAME}_MDY\\. for convenience
#           if there is no differences it displays the old search for specified search criteria
#           2. The search is unfiltered and a filesearch is filtered.
#           2. rnt search inverses the results. rnt.bat   ie for recentchanges search it will filter the results. For a file search it removes the filter.
#  Also borrowed script features from various scripts on porteus forums
import logging
import os
import re
import signal
import shutil
import sys
import tempfile
import time
from datetime import datetime, timedelta
from . import processha
from .config import load_toml
from .configfunctions import check_config
from .configfunctions import find_install
from .configfunctions import find_user_folder
from .configfunctions import get_config
from .config import dump_toml
from .dirwalker import scan_system
from .filterhits import update_filter_csv
from .fsearchfunctions import set_excl_dirs
from .gpgcrypto import decr_ctime
from .gpgcrypto import encr_cache
from .gpgkeymanagement import check_for_gpg
from .gpgkeymanagement import genkey
from .gpgkeymanagement import iskey
from .gpgkeymanagement import set_gpg
from .logs import setup_logger
from .pstsrg import main as pst_srg
from .pyfunctions import cprint
from .pyfunctions import user_path
from .recentchangessearchparser import build_parser
from .rntchangesfunctions import build_tsv
from .rntchangesfunctions import clear_logs
from .rntchangesfunctions import display
from .rntchangesfunctions import filter_lines_from_list
from .rntchangesfunctions import filter_output
from .rntchangesfunctions import find_ps1
from .rntchangesfunctions import find_scan
from .rntchangesfunctions import get_diff_file
from .rntchangesfunctions import get_runtime_exclude_list
from .rntchangesfunctions import hsearch
from .rntchangesfunctions import logic
from .rntchangesfunctions import multi_value
from .rntchangesfunctions import name_of
from .rntchangesfunctions import removefile
from .rntchangesfunctions import resolve_editor
from .rntchangesfunctions import time_convert
from .qtdrivefunctions import setup_drive_cache


def sighandle(signum, frame):
    # global stopf

    if signum in (signal.SIGINT, signal.SIGTERM):
        if signum == 2:
            print("Exit on ctrl-c", flush=True)
            sys.exit(0)


signal.signal(signal.SIGINT, sighandle)
signal.signal(signal.SIGTERM, sighandle)


'''
init 0 - 20 %
main search 20 - 60 %
processing 60 - 65%
pstsrg 65% - 90%
pstsrg with postop 65 - 85%
pstsrg with scanIDX 65 - 80%
pstsrg with postop and scanIDX 65 - 75%
'''


def main(argone, argtwo, usr, pwrd, argf="bnk", method="", iqt=False, drive=None, dtype=None, dbopt=None, cache_s=None, POST_OP=False, scan_idx=False, showDiff=False, dspPATH=None):

    appdata_local = find_install()  # appdata software install aka workdir
    toml_file, json_file, usr = get_config(appdata_local, usr, platform="Windows")

    script_dir = appdata_local / "scripts"
    flth_frm = appdata_local / "flth.csv"
    dbtarget_frm = appdata_local / "recent.gpg"
    cache_f_frm = appdata_local / "ctimecache.gpg"
    cache_s_frm = appdata_local / "systimeche.gpg"
    flth = str(flth_frm)
    dbtarget = str(dbtarget_frm)
    cache_f = str(cache_f_frm)
    cache_s_str = str(cache_s_frm)

    j_settings = {}  # convenience for commandline if basedir other than C:\\ always have available.
    # if basedir is C:\\ doesnt not touch json for speed as its set that way most of the time **
    config = load_toml(toml_file)  # setup_logger(process_label="RECENTCHANGES", wdir=appdata_local)
    if not config:
        return 1
    feedback = config['analytics']['feedback']
    analytics = config['analytics']['analyticSECT']
    analyticSECT = config['analytics']['analyticSECT']
    email = config['backend']['email']
    email_name = config['backend']['name']
    cachermPATTERNS = config['backend']['cachermPATTERNS']
    checksum = config['diagnostics']['checkSUM']
    cdiag = config['diagnostics']['cdiag']
    scanIDX = config['diagnostics']['scanIDX']
    autoIDX = config['diagnostics']['autoIDX']
    suppress_browser = config['diagnostics']['supbrw']
    supbrwLIST = config['diagnostics']['supbrwLIST']
    suppress = config['diagnostics']['suppress']
    postop = config['diagnostics']['postop']
    ps = config['shield']['proteusSHIELD']  # proteus shield
    show_diff = config['diagnostics']['showDIFF']
    compLVL = config['logs']['compLVL']
    moduleNAME = config['paths']['moduleNAME']
    archivesrh = config['search']['archivesrh']
    basedir = config['search']['drive']  # main drive for search
    ll_level = config['logs']['logLEVEL']
    log_file = config['logs']['userLOG']
    exclDIRS = user_path(config['search']['exclDIRS'], usr)
    driveTYPE_frm = config['search']['driveTYPE']
    pwrshell = config['search']['pwrshell']
    dspEDITOR = config['display']['dspEDITOR']
    if dspEDITOR:
        dspEDITOR = multi_value(dspEDITOR)
    dspPATH_frm = config['display']['dspPATH']

    escaped_user = re.escape(usr)

    # db cache patterns in config
    cachermPATTERNS = config['backend']['cachermPATTERNS']
    cachermPATTERNS = [
        p.replace("{{user}}", usr)
        for p in cachermPATTERNS
    ]

    # suppress browser list in config. regex
    supbrwLIST = [
        p.replace("{{user}}", escaped_user)
        for p in supbrwLIST
    ]

    # init

    gnupg_home = None

    if iqt:
        basedir = drive
        driveTYPE = driveTYPE_frm
        if dtype in ("HDD", "SSD"):
            driveTYPE = dtype
        else:
            print("driveTYPE for drive", basedir, " was null check json file", json_file)

        show_diff = showDiff
        postop = POST_OP
        scanIDX = scan_idx
        dspPATH = dspPATH
    else:
        if shutil.which("gpg") is None:
            gpg_path, gnupg_home = set_gpg(appdata_local, "gpg")
        if not check_for_gpg():
            print("Unable to verify gpg in path. Likely path was partially initialized. quitting")
            return 1

        dspPATH = ""
        if dspEDITOR:  # user wants results output in text editor
            dspEDITOR, dspPATH = resolve_editor(dspEDITOR, dspPATH_frm, toml_file)  # verify we have a working one
            if dspEDITOR is None:
                return 1

        outfile = name_of(dbtarget, '.db')
        dbopt = os.path.join(appdata_local, outfile)

        if ps or scanIDX:
            proteusPATH = config['shield']['proteusPATH']
            nogo = user_path(config['shield']['nogo'], usr)
            suppress_list = user_path(config['shield']['filterout'], usr)
            if not check_config(proteusPATH, nogo, suppress_list):
                return 1

        # if the drive type is not set auto detect it and update toml. look in json for partuuid and build cache_s
        #
        # if for some reason the mount changed for the drive update the json, rename the cache files and rename database tables

        # summary if the drive is unkown its detected and the toml is updated
        cache_s, _, suffix, driveTYPE = setup_drive_cache(
            basedir, appdata_local, dbopt, dbtarget, json_file, toml_file, cache_s_str, driveTYPE_frm, usr, email, compLVL, j_settings=j_settings
        )
        if not cache_s or not suffix:
            return 1
        if not j_settings:
            if basedir != "C:\\":
                print("failed to load json in setup_drive_cache")
                return 1

    # make a named tuple or dict for args and to pass less args for clarity
    user_setting = {
        'usr': usr,
        'email': email,
        'basedir': basedir,
        'driveTYPE': driveTYPE,
        'feedback': feedback,
        'analytics': analytics,
        'analyticSECT': analyticSECT,
        'checksum': checksum,
        'ps': ps,
        'cdiag': cdiag,
        'compLVL': compLVL
    }

    # end init

    # VARS
    log_file = appdata_local / "logs" / log_file

    tmpoutput = []  # holding
    # Searches
    recent = []  # main results
    tout = []  # ctime results
    sortcomplete = []  # combined
    tmpopt = []  # combined filtered

    # NSF
    complete_1, complete_2 = [], []
    complete = []  # combined

    # Diff file
    difference = []
    absent = []  # actions
    rout = []  # actions from ha

    cfr = {}  # cache dict

    start = end = cstart = cend = ag = 0
    validrlt = tmn = filename = search_time = None

    diffrlt = False
    nodiff = False
    syschg = False
    flsrh = False
    filtered = False
    validrlt = None

    dcr = True  # means to remove after encrypting. and backwards for this script dcr True is meant to mean leave open

    flnm = ""
    parseflnm = ""
    diff_file = ""

    filepath = ""
    dirSRC = ""

    tsv_doc = "doctrine.tsv"
    mergeddb = "recent_merged.db"
    excl_file = 'excluded.txt'  # find and powershell directory excludes from exclDIRS

    proval = 20  # progress
    endval = 30

    fmt = "%Y-%m-%d %H:%M:%S"

    usrDIR = find_user_folder("Desktop")
    if usrDIR is None:
        raise EnvironmentError("Could not find user Desktop folder")
    # DOC_S = find_user_folder("Documents")

    tempd = tempfile.gettempdir()

    with tempfile.TemporaryDirectory(dir=tempd) as tempwork:

        scr = os.path.join(tempwork, "scr")  # feedback
        cerr = os.path.join(tempwork, "cerr")  # priority

        if not iqt:
            is_key, err = iskey(email)
            if is_key is False:
                if not genkey(appdata_local, usr, email, email_name, dbtarget, cache_f, cache_s, flth, tempwork):
                    print("Failed to generate a gpg key. quitting")
                    return 1
            elif is_key is None:
                print(err)
                return 1

        cfr = decr_ctime(cache_f)

        start = time.time()

        logging_values = (log_file, ll_level, appdata_local, tempwork)

        setup_logger(log_file, logging_values[1], "MAIN")

        # initialize

        # Linux. windows ln 496 start, rntchangesfunction.py find_scan and ctime.py init_recentchanges using journal db dir cache system
        # load ctime or files created or copied with preserved metadata.
        # if xRC
        # tout = init_recentchanges(script_dir, home_dir, xdg_runtime, inotify_creation_file, cfr, xRC, checksum, moduleNAME, log_file)

        if argone != "search":
            thetime = argone
        else:
            thetime = argtwo

        if argf == "filtered":
            filtered = True

        # search criteria
        if thetime != "noarguser":
            p = 60
            try:
                argone = int(thetime)
                tmn = time_convert(argone, p, 2)
                search_time = tmn
                search_string = f"files {argone} seconds old or newer"

            except ValueError:  # its a file search

                argone = ".txt"
                if not os.path.isdir(pwrd):
                    print(f'Invalid argument {pwrd}. PWD required.')
                    sys.exit(1)
                os.chdir(pwrd)

                filename = argtwo  # sys.argv[2]
                if not os.path.isfile(filename) and not os.path.isdir(filename):
                    print('No such directory, file, or integer.')
                    sys.exit(1)

                parseflnm = os.path.basename(filename)
                if not parseflnm:  # get directory name
                    parseflnm = filename.rstrip("/\\").split("/")[-1].split("\\")[-1]
                if parseflnm.endswith('.txt'):
                    argone = ""

                filtered = True if not filtered else False

                flsrh = True
                ct = int(time.time())
                frmt = int(os.stat(filename).st_mtime)
                ag = ct - frmt
                ag = time_convert(ag, p, 2)
                search_time = ag
                search_string = f"files newer than {filename}"

        else:
            tmn = search_time = argone = 5
            search_string = "files 5 minutes old or newer"

        cprint.cyan(f'Searching for{" filtered" if filtered else ""} {search_string}\n')

        if iqt:
            print(f"Progress: {proval}", flush=True)

        # sys.stdout.flush()

        # Main search

        current_time = datetime.now()
        search_start_dt = (current_time - timedelta(minutes=search_time))
        logger = logging.getLogger("FSEARCH")

        # Windows default - Powershell
        if pwrshell:
            endval += 15

            # powershell 5 or 7
            pwsh_path = shutil.which("pwsh")
            if not pwsh_path:
                pwsh_path = "powershell.exe"

            excl_path = os.path.join(tempwork, excl_file)
            set_excl_dirs(basedir, excl_path, exclDIRS)  # write exclude list to tempdir this app is in

            s_path = os.path.join(script_dir, "scanline.ps1")

            # single process like find command, 19s for system scan
            command = [
                pwsh_path,
                "-NoProfile",
                "-ExecutionPolicy", "Bypass",
                "-File", s_path,
                "-rootPath", basedir,
                "-cutoffMinutes", str(search_time),
                "-excluded", excl_path,  # dynamic directory exclusion pwsh
                "-StartR", str(proval),
                "-EndR", str(endval)
            ]
            if iqt:
                command += ["-progress"]
            if feedback:
                command += ["-feedback"]
            proval += 15
            endval += 15
            init = True

            recent, complete_1, end, cstart = find_ps1(
                command, recent, complete_1, init, cfr, search_start_dt, user_setting,
                logging_values, end, cstart, iqt=iqt, strt=proval, endp=endval
            )

        # os.scandir
        else:

            init = True

            recent, complete_1, end, cstart = find_scan(
                recent, complete_1, init, cfr, search_start_dt, user_setting, logging_values,
                end, cstart, exclDIRS, iqt=iqt, strt=proval, endp=endval, logger=logger
            )

        cend = time.time()
        # if iqt:
        #     print(f"Progress: {endval + 1}%")  # for linux for gui knows it can stop without corrupting .gpg
        sys.stdout.flush()

        # end Main search

        if recent is None or tout is None:
            return 1

        if cfr and (recent or tout):
            encr_cache(cfr, cache_f, email, compLVL)

        if not recent:
            if not tout:
                cprint.cyan("No new files found")
                if iqt:
                    print("Progress: 100.00%")
                return 0
            # for entry in tout:
            #     tss = entry[0].strftime(fmt)
            #     fp = entry[1]
            #     print(f'{tss} {fp}')
            recent = tout[:]
            tout = []

        complete = complete_1 + complete_2  # nsf append to rout in pstsrg before stat insert
        proval = 60  # current progress
        endval = 90  # next

        sortcomplete = recent

        sortcomplete.sort(key=lambda x: x[0])  # get everything from the start time

        srttime = sortcomplete[0][0]  # store the start time
        merged = sortcomplete[:]

        for entry in tout:
            if not entry:
                continue
            tout_dt = entry[0]
            if tout_dt >= srttime:
                merged.append(entry)
        merged.sort(key=lambda x: x[0])

        seen = {}

        for entry in merged:
            if len(entry) < 12:
                continue

            filepath = entry[1]
            cam_flag = entry[11]

            key = filepath

            if key not in seen:
                seen[key] = entry
            else:
                existing_entry = seen[key]
                existing_cam = existing_entry[11]

                if existing_cam == "y" and cam_flag is None:
                    seen[key] = entry

        deduped = list(seen.values())

        # inclusions from this script /  sort -u
        exclude_patterns = get_runtime_exclude_list(appdata_local, usrDIR, moduleNAME, flth, dbtarget, cache_f, cache_s, gnupg_home, str(log_file), dbopt=dbopt)

        def filepath_included(filepath, exclude_patterns):
            filepath = filepath.lower()
            return not any(filepath.startswith(p) for p in exclude_patterns)

        sortcomplete = [
            entry for entry in deduped
            if filepath_included(entry[1], exclude_patterns)
        ]

        lines = []
        if not flsrh:
            start_dt = srttime
            range_sec = 300 if thetime == 'noarguser' else int(thetime)
            end_dt = start_dt + timedelta(seconds=range_sec)
            lines = [entry for entry in sortcomplete if entry[0] <= end_dt]
        else:
            lines = sortcomplete

        temp = os.environ.get('TEMP')
        tmp = os.environ.get('TMP')
        systemp = r"C:\Windows\Temp"

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

        sortcomplete = non_tmp_lines
        tmpoutput = tmp_lines

        filtered_lines = []
        for entry in sortcomplete:
            if len(entry) >= 16:
                ts_str = entry[0]
                filepath = entry[1]  # no escaped [16] needed
                filtered_lines.append((ts_str, filepath))

        tmpopt = filtered_lines  # human readable
        recent = tmpopt[:]

        # Apply filter. recent is unfiltered all data to store in db
        tmpopt = filter_lines_from_list(tmpopt, escaped_user)

        logf = recent  # all files
        if filtered:
            logf = tmpopt

        # Merge/Move old searches
        if sortcomplete:
            syschg = True
            oldsort = []
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
                dirSRC = appdata_local  # recentchanges
            else:
                dirSRC = usrDIR  # recentchanges search

            # is old search?
            result_output = os.path.join(dirSRC, f'{moduleNAME}{flnm}')

            if os.path.isfile(result_output):
                with open(result_output, 'r') as f:
                    oldsort = f.readlines()

            if not flsrh and argf != filtered:
                # try <app_install> for previous search
                if method != "rnt" and not oldsort:
                    fallback_path = os.path.join(appdata_local, f'{moduleNAME}{flnm}')
                    if os.path.isfile(fallback_path):
                        with open(fallback_path, 'r') as f:
                            oldsort = f.readlines()

                # try <app_install>\\moduleNAME_MDY*
                if not oldsort:
                    hsearch(oldsort, appdata_local, moduleNAME, flnm)

            # Move or clear previous searches
            validrlt = clear_logs(dirSRC,  method, appdata_local, moduleNAME, archivesrh)

            target_path = None
            # output tmp file results
            if method != "rnt":
                # send Temp results to user
                if tmpoutput:
                    target_filename = f"{moduleNAME}xSystemTmpfiles{parseflnm}{argone}"

                    target_path = os.path.join(usrDIR, target_filename)
                    with open(target_path, 'w') as dst:
                        for entry in tmpoutput:
                            tss = entry[0].strftime(fmt)
                            fp = entry[1]
                            dst.write(f'{tss} {fp}\n')

            diff_file = os.path.join(dirSRC, moduleNAME + flnmdff)

            # Difference file
            if oldsort:
                nodiff = True

            clean_oldsort = [line.strip() for line in oldsort]
            clean_logf_set = set(f'{entry[0].strftime(fmt)} {entry[1]}' for entry in logf)
            difference = [line for line in clean_oldsort if line not in clean_logf_set]

            if difference:
                diffrlt = True
                removefile(diff_file)
                with open(diff_file, 'w') as file2:
                    for entry in difference:
                        print(entry, file=file2)

                # preprocess before db/ha. The differences before ha and then sent to processha after ha
                processha.isdiff(sortcomplete, absent, rout, diff_file, difference, flsrh, srttime, fmt)

            # Send search result sortcomplete to user
            with open(result_output, 'w') as f:
                for entry in logf:
                    tss = entry[0].strftime(fmt)
                    fp = entry[1]
                    f.write(f'{tss} {fp}\n')

            proval = 65  # - 90%   normal for finishing pstsrg

            # file doctrine
            if postop:
                endval = 85  # adjust 65% - 85%

            if scanIDX or iqt:
                dcr = True  # leave open as there is a system scan after

            if scanIDX:
                endval = 80  # adjust 65% - 80%

            if postop and scanIDX:
                endval = 75

            if iqt:
                print(f"Progress: {proval}", flush=True)
            elif not scanIDX:
                dcr = False
            # Backend

            dbopt, csum = pst_srg(
                dbopt, dbtarget, basedir, sortcomplete, complete, rout, scr, cerr, cache_s, cachermPATTERNS, json_file, gnupg_home, user_setting, logging_values,
                dcr=dcr, iqt=iqt, strt=proval, endp=endval
            )
            # dbopt return from pst_srg is either path, encr_error, new_profile or None
            proval = endval + 1
            endval = 100

            if not iqt and scanIDX:
                dcr = False  # for command line reset to default False. This means to remove db after system scan. qt remains open for gui
            if not dbopt:
                print("There is a problem in pst_srg no return value. likely database wasnt created, path to database did not exist or permission issue")
                return 1
            if iqt:
                print(f"Progress: {proval}")  # print +1 for stop request polling Linux *
            if scanIDX and not os.path.isfile(dbopt):
                print(f"dbopt missing from pstsrg. {dbopt} unable to scan profile")
                scanIDX = False
                # if dbopt and dbopt != "encr_error":
                #     if os.path.isfile(dbtarget):
                #         change_perm(dbtarget, uid, gid, 0o644)

            if analyticSECT:
                print(f'Search took {end - start:.3f} seconds')
            if checksum:
                print(f'Checksum took {cend - cstart:.3f} seconds')
            print()

            # Diff output to user

            processha.processha(rout, absent, diff_file, cerr, flsrh, argf, srttime, escaped_user, supbrwLIST, suppress_browser, suppress)
            # Filter hits
            update_filter_csv(recent, flth, escaped_user)
            sys.stdout.flush()

            # File doctrine
            if postop:
                outpath = os.path.join(usrDIR, tsv_doc)
                if not os.path.isfile(outpath):
                    if build_tsv(sortcomplete, tmpopt, logf, rout, escaped_user, outpath, method, fmt):
                        cprint.green(f"File doctrine.tsv created {usrDIR}\\{tsv_doc}")
                elif not iqt:
                    # update_toml_values({'diagnostics': {'postop': False}}, toml_file)  # if one was already made disable the setting
                    config['diagnostics']['postop'] = False
                    dump_toml(None, config, toml_file)

            # Terminal output process scr/cer
            if not csum and not suppress:
                if os.path.exists(scr):
                    filter_output(scr, escaped_user, 'Checksum', 'no', 'blue', 'yellow', 'scr', supbrwLIST, suppress_browser, suppress)

            if csum:
                if os.path.isfile(cerr):
                    with open(cerr, 'r') as src, open(diff_file, 'a') as dst:
                        dst.write("\ncerr\n")
                        for line in src:
                            if line.startswith("Warning File"):
                                continue
                            dst.write(line)
                    removefile(cerr)
                # end Terminal output

            # Cleanup

            if os.path.isfile(scr):
                removefile(scr)

            if pwrshell:  # powershellcleanup
                database_merged = os.path.join(tempwork, mergeddb)
                removefile(database_merged)

        try:

            logic(syschg, nodiff, diffrlt, validrlt, thetime, argone, argf, result_output, filename, flsrh, method)  # feedback
            display(dspEDITOR, result_output, syschg, dspPATH)  # open text editor?
        except Exception as e:
            print(f"Error in logic or display {type(e).__name__} : {e} ")

        if dbopt not in ("new_profile", "encr_error", "db_error") and scanIDX:  # Scan system index. If it is from the command line and a new profile was just made dont scan it. Encryption failure dont scan as there is a problem.

            cprint.green('Running postop system index scan.')

            # append to old or use new default
            diff_file = diff_file if diffrlt else get_diff_file(appdata_local, usrDIR, moduleNAME)

            rlt = scan_system(appdata_local, dbopt, dbtarget, basedir, usr, diff_file, cache_s, email, analyticSECT, show_diff, compLVL, dcr=dcr, iqt=iqt, strt=proval, endp=endval)
            if not iqt and not autoIDX:  # if commandline, turn off so doesnt scan every time. autoIDX permissive to auto scan
                # update_toml_values({'diagnostics': {'scanIDX': False}}, toml_file)
                config['diagnostics']['scanIDX'] = False
                dump_toml(None, config, toml_file)

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

        if syschg:
            if iqt:
                print("Progress: 100%", flush=True)
            return 0
        return 1


def main_entry(argv):
    parser = build_parser()
    args = parser.parse_args(argv)

    calling_args = [
        args.argone,
        args.argtwo,
        args.usr,
        args.pwd,
        args.argf,
        args.method,
        args.iqt,
        args.drive,
        args.dtype,
        args.db_output,
        args.cache_file,
        args.post_OP,
        args.scan_idx,
        args.showDiff,
        args.dspPATH
    ]

    result = main(*calling_args)
    sys.exit(result)
