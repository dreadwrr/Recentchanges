import csv
import logging
import fnmatch
import hashlib
import os
import re
import sys
import traceback
from datetime import datetime
from pathlib import Path
from io import StringIO
import tomlkit

# Terminal supression and cache clear. Note: supress_terminal is regex

supress_terminal = [
    r'mozilla',  # unconfirmed
    r'\\.mozilla',  # unconfirmed
    r'chromium-ungoogled',  # unconfirmed
    r'Microsoft\\Edge',
    r'Opera Software',
    r'C:\\Users\\{{user}}\\AppData\\Local\\SomeFolder',
    r'Default\\Cache',   # spotify
    r'Chrome\\User Data\\Default'
]

# Cache clear
# patterns to delete from database. Non regex
cache_clear = [
    r"%caches%",
    r'%.cache%',
    r"%FontCache%",
    r"%__pycache__%",
    r'%C:\Users\{{user}}\AppData\Local\save-changesnew\flth%',
    r'%\Microsoft\Windows\Recent\%'
]
# filter hits to reset on cache clear in ftlh.csv in app install. copy from filter items wanted from filter.py
flth_literal_patterns = [

    r'C:\\Users\\{{user}}\\AppData\\Local\\Packages\\[^\\\\]+\\LocalCache',
    r'C:\\Users\\{{user}}\\AppData\\Local\\Packages\\.*?\\LocalCache'

]
# end Cache clear


# for terminal and hardlink supression.
def sbwr(escaped_user):  # note regex

    webb = [p.replace("{{user}}", escaped_user) for p in supress_terminal]

    compiled = [re.compile(p) for p in webb]
    return compiled


def get_delete_patterns(usr):
    patterns = [p.replace("{{user}}", usr) for p in cache_clear]
    return patterns


# for resetting filterhits CSV on cache clear
def reset_csvliteral(csv_file):

    patterns_to_reset = flth_literal_patterns
    try:
        with open(csv_file, newline='') as f:
            reader = csv.reader(f)
            rows = list(reader)
        for row in rows[1:]:
            if row[0] in patterns_to_reset:
                row[1] = '0'
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(rows)
    except (FileNotFoundError, PermissionError):
        pass


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


def collision(cursor, is_sys, sys_tables=None):

    if is_sys:
        tables = ['logs'] + list(sys_tables or [])

        union_sql = " UNION ALL ".join([f"SELECT filename, checksum, filesize FROM {t}" for t in tables])

        query = f"""
            SELECT a.filename, b.filename, a.checksum
            FROM ({union_sql}) a
            JOIN ({union_sql}) b
            ON a.checksum = b.checksum
            AND a.filename < b.filename
            AND a.filesize != b.filesize
        """

    else:
        table_name = 'logs'
        query = f"""
            SELECT a.filename, b.filename, a.checksum
            FROM {table_name} a
            JOIN {table_name} b
            ON a.checksum = b.checksum
            AND a.filename < b.filename
            AND a.filesize != b.filesize
        """

    cursor.execute(query)
    return cursor.fetchall()


def detect_copy(filename, inode, checksum, sys_tables, cursor, ps):
    if ps:
        sys_a, sys_b = sys_tables
        query = f'''
            SELECT filename, inode, checksum
            FROM logs
            UNION ALL
            SELECT filename, inode, checksum
            FROM {sys_a}
            WHERE checksum = ?
            UNION ALL
            SELECT filename, inode, checksum
            FROM {sys_b}
            WHERE checksum = ?
        '''
        cursor.execute(query, (checksum, checksum))
    else:
        query = '''
            SELECT filename, inode
            FROM logs
            WHERE checksum = ?
        '''
        cursor.execute(query, (checksum,))

    candidates = cursor.fetchall()

    for row in candidates:
        if ps:
            o_filename, o_inode, _ = row
        else:
            o_filename, o_inode = row

        if o_filename != filename or o_inode != inode:
            return True

    return False


def get_recent_changes(filename, cursor, table, e_cols=None):
    columns = [
        "timestamp", "filename", "creationtime", "inode",
        "accesstime", "checksum", "filesize", "owner",
        "domain", "mode"
    ]
    if e_cols:
        if isinstance(e_cols, str):
            e_cols = [col.strip() for col in e_cols.split(',') if col.strip()]
        columns += e_cols

    col_str = ", ".join(columns)

    query = f'''
        SELECT {col_str}
        FROM {table}
        WHERE filename = ?
        ORDER BY timestamp DESC
        LIMIT 1
    '''
    cursor.execute(query, (filename,))
    return cursor.fetchone()


def get_recent_sys(filename, cursor, sys_tables, e_cols=None):
    sys_a, sys_b = sys_tables

    columns = [
        "timestamp", "filename", "creationtime", "inode",
        "accesstime", "checksum", "filesize", "owner",
        "domain", "mode"
    ]
    if e_cols:
        if isinstance(e_cols, str):
            e_cols = [col.strip() for col in e_cols.split(',') if col.strip()]
        columns += e_cols

    col_str = ", ".join(columns)

    cursor.execute(f'''
        SELECT {col_str}
        FROM {sys_b}
        WHERE filename = ?
        ORDER BY timestamp DESC
        LIMIT 1
    ''', (filename,))
    row = cursor.fetchone()
    if row:
        return row
    cursor.execute(f'''
        SELECT {col_str}
        FROM {sys_a}
        WHERE filename = ?
        LIMIT 1
    ''', (filename,))
    return cursor.fetchone()


def increment_f(conn, c, sys_tables, records):

    sys_b = sys_tables[1]
    # sys_a = sys_tables[0]

    if not records:
        return False

    sql_insert = f"""
        INSERT OR IGNORE INTO {sys_b} (
            timestamp, filename, creationtime, inode, accesstime, checksum,
            filesize, symlink, owner, domain, mode, casmod, lastmodified,
            hardlinks, count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    # sql_update = f"UPDATE {sys_a} SET count = count + 1 WHERE filename = ?"

    try:
        with conn:
            c.executemany(sql_insert, records)

            # filenames = [(record[1],) for record in records]
            # c.executemany(sql_update, filenames)

        return True

    except Exception as e:
        print(f"Error increment_f table {sys_b}: {type(e).__name__} {e}")
        return False


# Convert Sql-like % wildcard to fnmatch *
def matches_any_pattern(s, patterns):

    for pat in patterns:
        pat = pat.replace('%', '*')
        if fnmatch.fnmatch(s, pat):
            return True
    return False


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


# obj from obj or str
def parse_datetime(value, fmt="%Y-%m-%d %H:%M:%S"):
    if isinstance(value, datetime):
        return value
    try:
        return datetime.strptime(str(value).strip(), fmt)
    except (ValueError, TypeError, AttributeError):
        return None


def escf_py(filename):
    filename = filename.replace('\\', '\\\\')
    filename = filename.replace('\n', '\\n')
    filename = filename.replace('"', '\\"')
    filename = filename.replace('$', '\\$')
    return filename


def unescf_py(s):
    s = s.replace('\\n', '\n')
    s = s.replace('\\"', '"')
    s = s.replace('\\$', '$')
    s = s.replace('\\\\', '\\')
    return s


def clear_conn(conn, cur):
    for obj, name in ((cur, "cursor"), (conn, "connection")):
        try:
            if obj:
                obj.close()
        except Exception as e:
            print(f"Warning: failed to close {name}: {e}")

# def unescf_py(escaped):  old
#     s = escaped
#     s = s.replace('\\\\', '\\')
#     s = s.replace('\\n', '\n')
#     s = s.replace('\\"', '"')
#     s = s.replace('\\$', '$')
#     return s

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


# ha funcs
def get_md5(file_path):
    try:
        with open(file_path, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()
    except FileNotFoundError:
        return None
    except Exception:
        # print(f"Error reading {file_path}: {e}")
        return None


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
        record[10] != metadata[2] or  # mode
        record[8] != metadata[0] or  # onr
        record[9] != metadata[1]  # domain
    )


def sys_record_flds(record, sys_records, prev_count):
    sys_records.append((
        record[0],  # timestamp
        record[1],  # filename
        record[2],  # creationtime
        record[3],  # inode
        record[4],  # accesstime
        record[5],  # checksum
        record[6],  # filesize
        record[7],  # symlink
        record[8],  # owner
        record[9],  # domain
        record[10],  # mode
        record[11],  # casmod
        record[12],  # lastmodified
        record[13],  # hardlinks
        prev_count + 1  # incremented count
    ))


# hanly mc
def goahead(filepath):
    try:
        st = filepath.stat()
        return st
    except FileNotFoundError:
        return "Nosuchfile"
    except (PermissionError, OSError, Exception) as e:
        logging.debug("Skipping exception stating file %s: %s - %s", filepath.name, type(e).__name__, e, exc_info=True)
    return None


# prepare for file output
def dict_to_list_sys(cachedata):
    data_to_write = []
    for root, versions in cachedata.items():
        for modified_ep, metadata in versions.items():
            row = {
                "checksum": metadata.get("checksum"),
                "size": metadata.get("size"),
                "modified_time": metadata.get("modified_time"),
                "modified_ep": modified_ep,
                "owner": metadata.get("owner"),
                "domain": metadata.get("domain"),
                "root": root,
            }
            data_to_write.append(row)
    return data_to_write


# recentchangessearch,
def dict_string(data: list[dict]) -> str:
    if not data:
        return ""

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys(), delimiter='|', quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(data)
    return output.getvalue()

# toml                  tomblib
# def load_config(conf_path):
#     if not os.path.isfile(conf_path):
#         print("Unable to find config file:", conf_path)
#         sys.exit(1)
#     with open(conf_path, 'rb') as f:
#         config = tomllib.load(f)
#     return config


def load_config(conf_path):  # tomlkit
    conf_path = Path(conf_path)
    if not conf_path.is_file():
        print("Unable to find config file:", conf_path)
        sys.exit(1)

    # Read the file as text
    text = conf_path.read_text(encoding="utf-8")

    # Parse using tomlkit
    try:
        config = tomlkit.parse(text)
    except Exception as e:
        print(f"Failed to parse TOML: {e}")
        sys.exit(1)

    return config


def check_script_path(script, appdata_local=None):

    #  ab_path = os.path.abspath(__file__)
    # cmd = os.path.join(appdata_local, cmd) # appdata_local
    script_path = os.path.join(appdata_local, script) if appdata_local else script
    return script_path


# app location
def get_wdir():
    # wdir = Path(sys.argv[0]).resolve().parent
    wdir = Path(__file__).resolve().parent.parent
    return wdir


def set_logger(root, process_label="MAIN"):
    fmt = logging.Formatter(f'%(asctime)s [%(levelname)s] [{process_label}] %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
    for handler in root.handlers:
        handler.setFormatter(fmt)


# Before setup_logger - return handler for user setting
def init_logger(ll_level, appdata_local):
    log_flnm = "errs.log"
    level_map = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "DEBUG": logging.DEBUG,
    }
    log_level = level_map.get(ll_level, logging.ERROR)
    log_path = appdata_local / "logs" / log_flnm

    return log_path, log_level


# set log level by handler for script or script area
def setup_logger(ll_level=None, process_label="MAIN", wdir=None):

    root = logging.getLogger()
    try:
        if not wdir:
            wdir = Path(get_wdir())  # appdata software install aka workdir

        if wdir and not ll_level:
            config_path = Path(wdir) / "config" / "config.toml"

            config = load_config(config_path)
            ll_level = config['search'].get('logLEVEL', 'ERROR')

        if wdir and ll_level:
            appdata_local = wdir

            if not root.hasHandlers():

                log_path, log_level = init_logger(ll_level.upper(), appdata_local)

                logging.basicConfig(
                    filename=log_path,
                    level=log_level,
                    format=f'%(asctime)s [%(levelname)s] [%(name)s] [{process_label}] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                )
            else:
                set_logger(root, process_label)
        else:
            print("Unable to get app location to set logging or log level")
    except Exception as e:
        print(f"Error setting up logger: {type(e).__name__} {e} \n{traceback.format_exc()}")
