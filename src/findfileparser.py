import argparse
# from .rntchangesfunctions import to_bool
from .rntchangesfunctions import multi_value


def parse_recent_args(parser):
    parser.add_argument("action", help="Is it wsl or pwsh")
    parser.add_argument("filename", help="the filename or pattern to search for")
    parser.add_argument("extension", help="extension to match")
    parser.add_argument("basedir", help="search target")
    parser.add_argument("user", help="Username for resolving user-specific paths in configuration")
    parser.add_argument("dspEDITOR", type=multi_value, help="Either \"False\" or 7zip or winrar")
    parser.add_argument("dspPATH", help="Path to run text editor executable")
    parser.add_argument("tempdir", help="The work area to do temp work")

    parser.add_argument("cutoffTIME", nargs="?", default=None,
                        help="modified time for compress option (default: None)")
    parser.add_argument("zipPROGRAM", nargs="?", default=None,
                        help="zip program to use for archive (default: None)")
    parser.add_argument("zipPATH", nargs="?", default=None,
                        help="zip program path (default: None)")
    parser.add_argument("USRDIR", nargs="?", default=None,
                        help="user desktop path used for exclusions for the compressed archive (default: None)")
    parser.add_argument("downloads", nargs="?", default=None,
                        help="where to save the archive if default not wanted (default: None)")

    return parser


def build_parser():
    parser = argparse.ArgumentParser(
        description="Run recentchanges from cmdline 6 required 5 optional"
    )
    parser = parse_recent_args(parser)

    return parser
