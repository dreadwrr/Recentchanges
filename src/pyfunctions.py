import fnmatch
import os
import re
from datetime import datetime


def suppress_list(escaped_user, suppress_list):
    compiled = [re.compile(re.escape(p)) for p in suppress_list]
    return compiled


def cache_clear_patterns(usr, cachermPATTERNS):
    return [
        p.replace("{{user}}", usr)
        for p in cachermPATTERNS
    ]


def user_path(settingName, theusr):
    if isinstance(settingName, list):
        return [s.replace("{{user}}", theusr) for s in settingName]
    elif isinstance(settingName, str):
        return settingName.replace("{{user}}", theusr)
    else:
        raise ValueError(f"Invalid type for settingName: {type(settingName).__name__}, expected str or list")


# Convert Sql-like % wildcard to fnmatch *
def matches_any_pattern(s, patterns):

    for pat in patterns:
        pat = pat.replace('%', '*')
        if fnmatch.fnmatch(s, pat):
            return True
    return False


# Initialize check no compressio
def cnc(target_file, compLVL):
    CSZE = 1024*1024
    if os.path.isfile(target_file):
        _, ext = os.path.splitext(target_file)
        try:
            file_size = os.stat(target_file).st_size
            size = file_size
            if ext == ".gpg":
                size = file_size // 2

            return size // CSZE >= compLVL  # no compression
        except Exception as e:
            print(f"Error setting compression of {target_file}: {e}")
    return False


class cprint:
    CYAN = "\033[36m"
    RED = "\033[31m"
    GREEN = "\033[1;32m"
    BLUE = "\033[34m"
    YELLOW = "\033[33m"
    MAGENTA = "\033[35m"
    WHITE = "\033[37m"
    RESET = "\033[0m"

    @staticmethod
    def colorize(color, msg, fp=None):
        """Return ANSI string; print to stdout if fp is None."""
        text = f"{color}{msg}{cprint.RESET}"
        if fp is None:
            print(text)  # default: print to console
        else:
            return text  # just return string, don’t print

    @staticmethod
    def cyan(msg, fp=None): return cprint.colorize(cprint.CYAN, msg, fp)
    @staticmethod
    def red(msg, fp=None): return cprint.colorize(cprint.RED, msg, fp)
    @staticmethod
    def green(msg, fp=None): return cprint.colorize(cprint.GREEN, msg, fp)
    @staticmethod
    def blue(msg, fp=None): return cprint.colorize(cprint.BLUE, msg, fp)
    @staticmethod
    def yellow(msg, fp=None): return cprint.colorize(cprint.YELLOW, msg, fp)
    @staticmethod
    def magenta(msg, fp=None): return cprint.colorize(cprint.MAGENTA, msg, fp)
    @staticmethod
    def white(msg, fp=None): return cprint.colorize(cprint.WHITE, msg, fp)
    @staticmethod
    def reset(msg, fp=None): return cprint.colorize(cprint.RESET, msg, fp)

    @staticmethod
    def plain(msg, fp=None):
        if fp is None:
            print(msg)  # default: print to console
        else:
            return msg  # just return string without printing


def epoch_to_str(epoch, fmt="%Y-%m-%d %H:%M:%S"):
    try:
        dt = datetime.fromtimestamp(float(epoch))
        return dt.strftime(fmt)
    except (TypeError, ValueError):
        return None


def epoch_to_date(epoch):
    try:
        return datetime.fromtimestamp(float(epoch))
    except (TypeError, ValueError):
        return None
    except OSError as e:
        print("epoch was", epoch, e)


# obj from obj or str
def parse_datetime(value, fmt="%Y-%m-%d %H:%M:%S"):
    if isinstance(value, datetime):
        return value
    try:
        return datetime.strptime(str(value).strip(), fmt)
    except (ValueError, TypeError, AttributeError):
        return None


# encoding
def ap_decode(s):
    s = s.replace('\\ap0A', '\n')
    s = s.replace('\\ap09', '\t')
    s = s.replace('\\ap22', '"')
    # s = s.replace('\\ap24', '$')
    s = s.replace('\\ap20', ' ')
    s = s.replace('\\ap5c', '\\')
    return s


def escf_py(filename):
    filename = filename.replace('\\', '\\ap5c')
    filename = filename.replace('\n', '\\\\n')
    # filename = filename.replace('"', '\\"')
    # filename = filename.replace('\t', '\\t')
    # filename = filename.replace('$', '\\$')
    return filename


def unescf_py(s):
    s = s.replace('\\\\n', '\n')
    # s = s.replace('\\"', '"')
    # s = s.replace('\\t', '\t')
    # s = s.replace('\\$', '$')
    s = s.replace('\\ap5c', '\\')
    return s
# end encoding


# def parse_line(line):                original
#     quoted_match = re.search(r'"((?:[^"\\]|\\.)*)"', line)
#     if not quoted_match:
#         return None
#     raw_filepath = quoted_match.group(1)
#     # try:
#     #     filepath = codecs.decode(raw_filepath.encode(), 'unicode_escape')
#     # except UnicodeDecodeError:
#     #     filepath = raw_filepath
#     filepath = unescf_py(raw_filepath)

#     # Remove quoted path
#     line_without_file = line.replace(quoted_match.group(0), '').strip()
#     other_fields = line_without_file.split()

#     if len(other_fields) < 7:
#         return None

#     timestamp1 = other_fields[0] + ' ' + other_fields[1]
#     timestamp2 = other_fields[2] + ' ' + other_fields[3]
#     inode = other_fields[4]
#     timestamp3 = other_fields[5] + ' ' + other_fields[6]
#     rest = other_fields[7:]

#     return [timestamp1, filepath, timestamp2, inode, timestamp3] + rest


def is_integer(value):
    try:
        int(value)
        return True
    except (ValueError, TypeError):
        return False


def is_valid_datetime(value, fmt):
    try:
        datetime.strptime(str(value).strip(), fmt)
        return True
    except (ValueError, TypeError, AttributeError):
        return False


def new_meta(record, metadata):
    return (
        record[0] != metadata[0] or  # onr
        record[1] != metadata[1] or  # domain
        record[2] != metadata[2]  # mode
    )


def sys_record_flds(record, sys_records, prev_count):
    sys_records.append((
        record[0],  # timestamp
        record[1],  # filename
        record[2],  # changetime
        record[3],  # inode
        record[4],  # accesstime
        record[5],  # checksum
        record[6],  # filesize
        record[7],  # symlink
        record[8],  # owner
        record[9],  # group
        record[10],  # permissions
        record[11],  # casmod
        record[12],  # target
        record[13],  # lastmodified
        record[14],  # hardlinks
        prev_count + 1,  # count
        record[15]  # mtime_us
    ))


# def collisions(xdata, cerr, c, ps):
#     """ return collisions from the database. see ln628 pysql.py not used currently """
#     reported = set()
#     csum = False
#     colcheck = collision(c, ps)

#     if colcheck:

#         collision_map = defaultdict(set)
#         for a_filename, b_filename, file_hash, size_a, size_b in colcheck:
#             collision_map[a_filename, file_hash].add((b_filename, file_hash, size_a, size_b))
#             collision_map[b_filename, file_hash].add((a_filename, file_hash, size_b, size_a))
#         try:
#             with open(cerr, "a", encoding="utf-8") as f:
#                 for record in xdata:
#                     filename = record[1]
#                     checks = record[5]
#                     size_non_zero = record[6]
#                     if size_non_zero:
#                         key = (filename, checks)
#                         if key in collision_map:
#                             for other_file, file_hash, size1, size2 in collision_map[key]:
#                                 pair = tuple(sorted([filename, other_file]))
#                                 if pair not in reported:
#                                     csum = True
#                                     print(f"COLLISION: {filename} {size1} vs {other_file} {size2} | Hash: {file_hash}", file=f)
#                                     reported.add(pair)
#         except IOError as e:
#             print(f"Failed to write collisions: {e} {type(e).__name__}  \n{traceback.format_exc()}")
#     return collision_map, csum
