import argparse
import sys
from .rntchangesfunctions import to_bool
# from rntchangesfunctions import multi_value


def build_subparser(script):
    parser = argparse.ArgumentParser(
        description="Run recentchanges from cmdline 6 required 5 optional"
    )
    subparsers = parser.add_subparsers(dest="args", required=True)

    arg_e = subparsers.add_parser(script, help="Run main search with hybrid analysis")
    parse_recent_args(arg_e)  # use the parser for recentchangessearch

    r_args = parser.parse_args()

    if r_args.args == script:
        recent_args = [
            r_args.argone, r_args.argtwo, r_args.USR, r_args.PWD, r_args.argf, r_args.method,
            r_args.iqt, r_args.drive, r_args.db_output, r_args.cache_file, r_args.POST_OP,
            r_args.scan_idx, r_args.showDiff, r_args.argwsl, r_args.dspPATH
        ]
        return recent_args
    else:
        print("Parser fault for recentchangessearch. exit")
        sys.exit(1)


def parse_recent_args(parser):
    parser.add_argument("argone", help="First required argument keyword search or the search time in seconds")
    parser.add_argument("argtwo", help="Second required argument the search time for recentchanges search or noarguser")
    parser.add_argument("USR", help="Username")
    parser.add_argument("PWD", help="Password")
    parser.add_argument("argf", nargs="?", default="bnk", help="Optional argf or inverted (default: bnk)")
    parser.add_argument("method", nargs="?", default="", help="Optional method rnt means recentchanges \"\" means recentchanges search (default: empty)")
    parser.add_argument("iqt", nargs="?", type=to_bool, default=False,
                        help="iqt boolean from Qt app show progress (default: False)")
    parser.add_argument("drive", nargs="?", default=None,
                        help="basedir or drive from qt gui. the target search drive (default:None)")
    parser.add_argument("db_output", nargs="?", default=None,
                        help="Path to decrypted database from qt application for pst_srg and ha (default:None)")
    parser.add_argument("cache_file", nargs="?", default=None,
                        help="Path to systimeche.gpg or systimeche_xsdx.gpg profile cache file for build and scan IDX (default:None)")
    parser.add_argument("POST_OP", nargs="?", type=to_bool, default=False,
                        help="POST_OP boolean postop create file doctrine (default: False)")
    parser.add_argument("scan_idx", nargs="?", type=to_bool, default=False,
                        help="scan_idx boolean postop scan index (default: False)")
    parser.add_argument("showDiff", nargs="?", type=to_bool, default=False,
                        help="showDiff boolean show symmetric differences for idx scan (default: False)")
    parser.add_argument("argwsl", nargs="?", type=to_bool, default=False,
                        help="argwsl boolean running windows subsystem of linux (default: False)")
    parser.add_argument("dspPATH", nargs="?", default=None,
                        help="Optional dspPATH verified path to editor (default: None)")

    return parser


def build_parser():
    parser = argparse.ArgumentParser(
        description="Run recentchanges from cmdline 4 required 9 optional"
    )
    parser = parse_recent_args(parser)

    return parser
