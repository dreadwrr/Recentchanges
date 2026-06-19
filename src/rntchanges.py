#! python3
# Command line save-changesnew aka developer buddy                                                                      06/19/2026
#
# This script is the entry point for recentchanges. The inv flag is passed in from a .bat or sym link (rnt.bat)
#
# There are 2 positional arguments. a third is the inv flag
# the filtered arg just changes a regular search to the inverse for recentchanges search, recentchanges search n, recentchanges search myfile
#
# for recentchanges the arguments shift. as its recentchanges or recentchanges n.
#
# the main purpose is to output unfiltered system files and tmp files.
#
# recentchanges - output to AppData
# can take 1 argument the time n or no arguments for 5 minutes.
#
# recentchanges search - output to Desktop
# can take 1 argument the time n or no arguments for 5 minutes.
#
# recentchanges query - show stats from the database from past searches
#
# recentchanges reset - delete gpg key and gpg files and prompt to reset config files
#
# argone - the search time for `recentchanges` or the keyword search for `recentchanges search` or keyword query to get stats from database
# argtwo - search time for `recentchanges search`
# argf - inv flag from rnt symlink
import os
import sys
from src.configfunctions import get_user
from src.query import main as query_main
from src.recentchangessearch import main as recentchanges_main
from src.rntchangesfunctions import is_admin


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

    argone = argv[1] if len(sys.argv) > 1 and argv[1] else "noarguser"
    argtwo = argv[2] if len(sys.argv) > 2 and argv[2] else "noarguser"

    srcDIR = ""
    method = ""
    argf = ""

    if argone == "inv":
        argf = "filtered"
        argone = "noarguser"
    elif argtwo == "inv":
        argf = "filtered"
        argtwo = "noarguser"
    elif "inv" in argv:
        argf = "filtered"

    if argone == "query" or argone == "reset":
        reset = argone == "reset"
        return query_main(user=usr, reset=reset)

    elif argone == "search":  # recentchanges search
        thetime = argtwo
        return recentchanges_main(argone, thetime, usr, pwd, argf, method)

    else:  # recentchanges

        thetime = argone  # shift for recentchanges
        method = "rnt"

        if thetime == "SRC":
            thetime = argtwo if argtwo != "SRC" else "noarguser"

        if argtwo == "search":
            print("Exiting not a search.")
            return 1

        srcDIR = "SRC" if "SRC" in sys.argv else "noarguser"

        return recentchanges_main(thetime, srcDIR, usr, pwd, argf, method)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
