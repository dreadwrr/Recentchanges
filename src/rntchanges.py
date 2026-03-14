#! python3
# Command line save-changesnew aka developer buddy                                                                      03/07/2026
#
# This script is the entry point for recentchanges. The inv flag is passed in from a .bat or sym link (rnt.bat)
#
# There are 2 positional arguments. a third is the inv flag and is filtered out before executing script.
# the filtered arg just changes a regular search to the inverse for recentchanges search, recentchanges search n, recentchanges search myfile
#
# for recentchanges the arguments shift. as its recentchanges or recentchanges n and the filter arg doesnt apply.
# recentchanges takes 1 argument the time n or no arguments for 5 minutes.
#
# recentchanges search - Desktop unfiltered files and tmp files. Also search for newer than file filtered. if called from rnt.bat its the opposite.
# query - show stats from the database from past searches
#
# reset - reset gpg key pair and clear gpgs
# recentchanges - AppData unfiltered and archives searches. No tmp files.
#
# all searches are stored in the database before filtering. This is for hybrid analysis to capture the necessary data

# argone - search time for `recentchanges` or keyword search for `recentchanges search` or keyword query to get stats from database
# argtwo - search time
# argf - inv flag from rnt.bat for `recentchanges search`
import os
import sys
from src.configfunctions import get_user
from src.query import main as query_main
from src.recentchangessearch import main as recentchanges_main
from src.rntchangesfunctions import is_admin


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

    is_admin()

    USR = get_user()
    if not USR:
        print("Unable to get username exiting.")
        return 1

    # $Host.UI.SupportsVirtualTerminal       set ansi colors
    PWD = os.getcwd()
    args = argv[1:]

    arge, argf = filter_invflag(args)  # filter out the invflag from the last parameter and set argf to filtered. passed from rnt.bat.

    argone = arge[0] or "noarguser"
    THETIME = arge[1] or "noarguser"

    if argone == "query" or argone == "reset":
        reset = argone == "reset"
        return query_main(user=USR, reset=reset)

    elif argone == "search":  # recentchanges search
        return recentchanges_main(argone, THETIME, USR, PWD, argf, "")

    else:  # recentchanges
        argf = "bnk"

        SRCDIR = "SRC" if "SRC" in arge[:2] else "noarguser"

        THETIME = arge[0] or "noarguser"  # Shift for this script
        if THETIME == "SRC":
            THETIME = arge[1] or "noarguser"

        if THETIME == "search":
            print("Exiting not a search.")
            return 1

        if THETIME == "SRC":
            THETIME = "noarguser"

        return recentchanges_main(THETIME, SRCDIR, USR, PWD, argf, "rnt")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
