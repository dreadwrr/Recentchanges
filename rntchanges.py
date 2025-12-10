#!/usr/bin/env python3
# Command line save-changesnew aka developer buddy                                                                      12/08/2025
#
# This script is the entry point for recentchanges. The inv flag is passed in from a .bat or sym link (rnt.bat)
#
# There are 2 positional arguments. a third is the inv flag and is filtered out before executing script.
# the filtered arg just changes a regular search to the inverse for `recentchanges search`, `recentchanges search n`, `recentchanges search myfile`
#
# for `recentchanges` the arguments shift. as its `recentchanges` or `recentchanges n` and the filter arg doesnt apply. There is a SRC tag but not implemented
# yet. `recentchanges` takes 1 argument the time n or no arguments for 5 minutes.
#
# `recentchanges search` - Desktop unfiltered files and tmp files. Also search for newer than file filtered. if called from rnt.bat its the opposite.
# `query` - show stats from the database from past searches
#
# `recentchanges` - AppData unfiltered and archives searches. No tmp files.
#
# all searches are stored in the database before filtering. This is for hybrid analysis to capture the necessary data

# argone - search time for `recentchanges` or keyword search for `recentchanges search` or keyword query to get stats from database
# argtwo - search time
# argf - inv flag from rnt.bat for `recentchanges search`
import os
import shutil
import sys
from src.pyfunctions import load_config
from src.recentchangessearch import main as recentchanges_main
from src.rntchangesfunctions import get_usr
from src.pyfunctions import get_wdir
from src.rntchangesfunctions import check_for_gpg
from src.rntchangesfunctions import is_admin
from src.rntchangesfunctions import multi_value
from src.rntchangesfunctions import resolve_editor
from src.rntchangesfunctions import set_gpg
from src.query import main as query_main
from src.qtfunctions import setup_drive_settings
# from src.rntchangesfunctions import res_path
# from .rntchangesfunctions import pwsh_7


# Handle inv flag
def filter_invflag(argv, pad_length=5):

    arge = ["" if item == "inv" else item for item in argv]
    argf = "filtered" if "inv" in argv else ""

    if len(arge) < pad_length:
        arge.extend([""] * (pad_length - len(arge)))
    return arge, argf


def main(argv):
    if ("inv" in argv and len(argv) > 4) or len(argv) > 3:
        print("Incorrect usage. recentchanges search time or recentchanges time.")
        return 1
    # inst, ver = pwsh_7()
    # if not inst:
    #     if ver is not None:
    #         print(f"PowerShell 7 required. installed:{ver}")
    #     return 1
    is_admin()
    USR = get_usr()
    if not USR:
        print("Unable to get username exiting.")
        return 1
    # $Host.UI.SupportsVirtualTerminal       set ansi colors
    PWD = os.getcwd()
    SRCDIR = "noarguser"
    appdata_local = get_wdir()
    # appdata_local = wdir / "save-changesnew"  original install was C:\\Users\\{{user}}\\AppData\\local
    json_file = appdata_local / "config" / "usrprofile.json"
    toml_file = appdata_local / "config" / "config.toml"
    config = load_config(toml_file)
    email = config['backend']['email']
    dspEDITOR = config['display']['dspEDITOR']
    if dspEDITOR:
        dspEDITOR = multi_value(dspEDITOR)
    dspPATH_frm = config['display']['dspPATH']
    flth = appdata_local / "flth.csv"  # "C:\\Users\\{{user}}\\AppData\\Local\\save-changesnew\\flth.csv"    # config['paths']['flth']  used to be changeable (linux)
    dbtarget = appdata_local / "recent.gpg"  # config['paths']['pydbpst']
    basedir = config['search']['drive']
    driveTYPE = config['search']['modelTYPE']

    if shutil.which("gpg") is None:
        set_gpg(appdata_local, "gpg")
    if not check_for_gpg:
        print("Unable to verify gpg in path. Likely path was partially initialized. quitting")
        return 1

    dspPATH = ""
    if dspEDITOR:  # user wants results output in text editor
        dspEDITOR, dspPATH = resolve_editor(dspEDITOR, dspPATH_frm, toml_file)  # verify we have a working one

    args = argv[1:]

    arge, argf = filter_invflag(args)  # filter out the invflag from the last parameter and set argf to `filtered`. passed from rnt.bat.

    argone = arge[0] or "noarguser"
    THETIME = arge[1] or "noarguser"

    if not driveTYPE and argone != "query":
        driveTYPE = setup_drive_settings(basedir, driveTYPE, json_file, toml_file, False, appdata_local)
        if driveTYPE is None:
            return 1
    if argone != "query":
        if driveTYPE not in ('HDD', 'SSD'):
            print(f"Incorrect setting modelTYPE: {driveTYPE} in config: {toml_file}")
            return 1


    if argone == "search":  # `recentchanges search`
        return recentchanges_main(argone, THETIME, USR, PWD, argf, "", dspPATH=dspPATH)

    elif argone == "query":

        flth = str(flth)  # res_path(flth, USR)
        dbtarget = str(dbtarget)  # res_path(dbtarget, USR)
        return query_main(dbtarget, email, USR, flth)

    else:  # `recentchanges`
        argf = "bnk"
        THETIME = arge[0] or "noarguser"  # Shift for this script

        if THETIME == "SRC":
            SRCDIR = THETIME
            THETIME = arge[1] or "noarguser"

        if THETIME == "search":
            print("Exiting not a search.")
            return 1

        return recentchanges_main(THETIME, SRCDIR, USR, PWD, argf, "rnt", dspPATH=dspPATH)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
