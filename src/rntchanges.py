#! python3
# Command line save-changesnew aka developer buddy                                                                      05/30/2026
#
# This script is the entry point for recentchanges. The inv flag is passed in from a .bat or sym link (rnt.bat)
#
# There are 2 positional arguments. a third is the inv flag and is filtered out before executing script.
# the filtered arg just changes a regular search to the inverse
#
# for recentchanges the arguments shift
# recentchanges -  AppData unfiltered files. takes 1 argument the time n or no arguments for 5 minutes.
#
# recentchanges search - Desktop unfiltered files. Also search for newer than file filtered. if called from rnt.bat its the opposite.
# query - show stats from the database from past searches
#
# reset - reset gpg key pair and clear gpgs. prompt to remove configs
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

    usr = get_user()
    if not usr:
        print("Unable to get username exiting.")
        return 1

    # $Host.UI.SupportsVirtualTerminal       set ansi colors
    pwd = os.getcwd()
    args = argv[1:]

    arge, argf = filter_invflag(args)  # filter out the invflag from the last parameter and set argf to filtered. passed from rnt.bat.

    argone = arge[0] or "noarguser"
    thetime = arge[1] or "noarguser"

    if argone == "query" or argone == "reset":
        reset = argone == "reset"
        return query_main(user=usr, reset=reset)

    elif argone == "search":  # recentchanges search
        return recentchanges_main(argone, thetime, usr, pwd, argf, "")

    else:  # recentchanges

        srcDIR = "SRC" if "SRC" in arge[:2] else "noarguser"

        thetime = arge[0] or "noarguser"  # Shift for this script
        if thetime == "SRC":
            thetime = arge[1] or "noarguser"

        if thetime == "search":
            print("Exiting not a search.")
            return 1

        if thetime == "SRC":
            thetime = "noarguser"

        return recentchanges_main(thetime, srcDIR, usr, pwd, argf, "rnt")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
