import argparse
from .rntchangesfunctions import to_bool
from .rntchangesfunctions import multi_value


def build_dwalk_parser():
    parser = argparse.ArgumentParser(
        description="Dispatcher for dirwalker: scan, build, downloads."
    )

    subparsers = parser.add_subparsers(dest="action", required=True)

    # SCAN SUBCOMMAND
    scan_p = subparsers.add_parser("scan", help="Scan IDX for changes to profile files")

    scan_p.add_argument("dbopt")
    scan_p.add_argument("dbtarget")
    scan_p.add_argument("basedir")
    scan_p.add_argument("difffile", help="result output for scan IDX")
    scan_p.add_argument("updatehlinks", type=to_bool)
    scan_p.add_argument("CACHE_S")
    scan_p.add_argument("email")
    scan_p.add_argument("ANALYTICSECT", nargs="?", type=to_bool, default=True, help="show extra display such as total time")
    scan_p.add_argument("showDiff", nargs="?", type=to_bool, default=False, help="showDiff boolean show profile files that no longer exist if True (default: False)")
    scan_p.add_argument("cache_idx", nargs="?", type=to_bool, default=False, help="cache_idx boolean show profile cache directories that have files when original count was 0. (default: False)")
    scan_p.add_argument("compLVL", nargs="?", type=int, default=200, help="compLVL integer when to turn off compression (default: 200)")
    scan_p.add_argument("dcr", nargs="?", type=to_bool, default=False, help="dcr boolean whether to delete .db after encrypting. (default: False)")
    scan_p.add_argument("iqt", nargs="?", type=to_bool, default=False, help="iqt boolean from Qt app show progress (default: False)")
    scan_p.add_argument("strt", nargs="?", type=int, default=0, help="strt integer where to start progress (default: 0)")
    scan_p.add_argument("endp", nargs="?", type=int, default=100, help="endp integer where to end progress (default: 100)")

    # BUILD SUBCOMMAND
    build_p = subparsers.add_parser("build", help="Build IDX or drive index")

    build_p.add_argument("dbopt")
    build_p.add_argument("dbtarget")
    build_p.add_argument("basedir")
    build_p.add_argument("updatehlinks", type=to_bool)
    build_p.add_argument("CACHE_S")
    build_p.add_argument("email")
    build_p.add_argument("ANALYTICSECT", nargs="?", type=to_bool, default=True, help="show extra display such as total time")
    build_p.add_argument("idx_drive", nargs="?", type=to_bool, default=False, help="idx_drive boolean its a drive index and exit early (default: False)")
    build_p.add_argument("cache_idx", nargs="?", type=to_bool, default=True, help="cache_idx boolean create a cache .gpg of system directories (default: True)")
    build_p.add_argument("compLVL", nargs="?", type=int, default=200, help="compLVL integer when to turn off compression (default: 200)")
    build_p.add_argument("iqt", nargs="?", type=to_bool, default=False, help="iqt boolean from Qt app show progress (default: False)")
    build_p.add_argument("strt", nargs="?", type=int, default=0, help="strt integer where to start progress (default: 0)")
    build_p.add_argument("endp", nargs="?", type=int, default=100, help="endp integer where to end progress (default: 100)")

    # DOWNLOADS SUBCOMMAND
    downloads_p = subparsers.add_parser("downloads", help="Find downloads button")

    downloads_p.add_argument("dbopt")
    downloads_p.add_argument("dbtarget")
    downloads_p.add_argument("basedir")
    downloads_p.add_argument("mdltype", help="basedir model ssd or hdd for branch logic serial or multi")
    downloads_p.add_argument("tempdir")
    downloads_p.add_argument("CACHE_S")
    downloads_p.add_argument("dspEDITOR", type=multi_value)
    downloads_p.add_argument("dspPATH")
    downloads_p.add_argument("email")
    downloads_p.add_argument("ANALYTICSECT", nargs="?", type=to_bool, default=True, help="show extra display such as total time")
    downloads_p.add_argument("compLVL", nargs="?", type=int, default=200, help="compLVL integer when to turn off compression (default: 200)")

    return parser
