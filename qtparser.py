import sys
from src.dirwalker import main_entry as dirwalker_main
from src.findfile import main_entry as findfile_main
from src.recentchangessearch import main as recentchanges_main
from src.recentchangessearchparser import build_subparser
from src.rntchanges import main as rntchanges_main


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
                "build": 7,
                "downloads": 12,
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
