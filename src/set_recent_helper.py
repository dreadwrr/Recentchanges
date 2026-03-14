# run set tasks for recentchanges
# flake8: noqa: E402
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.dirwalker import main_entry as dirwalker_main
from src.findfile import main_entry as findfile_main
from src.recentchangessearch import main as recentchanges_main
from src.recentchangessearchparser import build_subparser
from src.rntchanges import main as rntchanges_main

# 03/13/2026


def dispatch_internal(argv):

    len_args = len(argv)
    if len_args > 5:
        script = argv[1].lower()
        args = argv[2:]

        cmd = args[0]

        DISPATCH_MAP = {
            "dirwalker.py": {
                "hardlink": 7,
                "scan": 8,
                "build": 9,
                "downloads": 11,
            },
            "recentchangessearch.py": recentchanges_main,
            "findfile.py": findfile_main,
        }

        entry = DISPATCH_MAP.get(script)

        if isinstance(entry, dict):
            if cmd not in entry:
                print(
                    f"Invalid parameter for dirwalker; expected one of: "
                    f"{'/'.join(entry.keys())}, got {cmd}"
                )
                sys.exit(1)
            min_args = entry[cmd]
            if len_args < min_args:
                print(f"Not enough args for '{cmd}', expected {min_args}, got {len_args}")
                sys.exit(1)

            sys.exit(dirwalker_main(args))

        elif entry:

            if script == "recentchangessearch.py":
                recent_args = build_subparser(script)
                sys.exit(entry(*recent_args))
            elif script == "findfile.py":
                sys.exit(entry(args))
    sys.exit(rntchanges_main(argv))

def main(argv):
    arglen = len(argv)
    if arglen < 2:
        return False
    if arglen > 5:
        res = dispatch_internal(argv)
        if not res:
            return 1
        return res
    method = argv[1].lower()
    if method == "run":
        sys.exit(rntchanges_main(argv[1:]))
    sys.exit(rntchanges_main(argv))


if __name__ == "__main__":

    res = dispatch_internal(sys.argv)
    if not res:
        sys.exit(1)
    sys.exit(res)