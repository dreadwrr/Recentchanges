import hashlib
import logging
import os
import pywintypes
import win32con
import win32file
import win32security
from datetime import datetime


def parse_iso8601(s):
    try:
        if s[-6] in ("+", "-") and s[-3] == ":":
            s = s[:-6]

        if "." in s:
            main, frac = s.split(".")
            frac = (frac + "000000")[:6]
            s = f"{main}.{frac}"

        return datetime.fromisoformat(s)
    except (ValueError, TypeError, AttributeError) as e:
        logging.debug("parse_iso8601: invalid date format: %s : %s", s, e, exc_info=True)
        return None


def calculate_checksum(file_path):
    try:
        hash_func = hashlib.md5()
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                hash_func.update(chunk)
        return hash_func.hexdigest()
    except Exception:
        return None


def upt_cache(cfr, existing_keys, checksum, file_size, time_stamp, modified_ep, owner, domain, file_path):
    size_s = str(file_size) if file_size else ''
    mod_ep_s = str(modified_ep)
    checksum_s = str(checksum) if checksum else ''
    key = (checksum_s, size_s, mod_ep_s, file_path)

    if key not in existing_keys:
        if file_path not in cfr:
            cfr[file_path] = {}
        cfr[file_path][mod_ep_s] = {
            "checksum": checksum_s,
            "size": size_s,
            "modified_time": str(time_stamp),
            "owner": str(owner) if owner else '',
            "domain": str(domain) if domain else ''
        }

        existing_keys.add(key)


# prev_entry = CACHE_S.get(root)
def get_cached(cfr, size, modified_ep, path):
    if not cfr:
        return None

    versions = cfr.get(path)
    if not versions:
        return None

    if modified_ep is not None:
        row = versions.get(str(modified_ep))
        if row and str(size) == row["size"]:
            return {
                "checksum": row.get("checksum"),
                "owner": row.get("owner"),
                "domain": row.get("domain"),
                "modified_ep": modified_ep
            }
        return None

    latest_ep = max(versions.keys(), key=lambda k: int(k))
    row = versions[latest_ep]
    if str(size) == row["size"]:
        return {
            "checksum": row.get("checksum"),
            "owner": row.get("owner"),
            "domain": row.get("domain"),
            "modified_ep": latest_ep
        }

    return None


def get_reparse_type(path):
    try:
        attrs = win32file.GetFileAttributes(path)
        if not (attrs & win32con.FILE_ATTRIBUTE_REPARSE_POINT):
            return None
        if os.path.islink(path):
            return "symlink"
        return "reparse_point"
    except Exception as e:
        logging.debug(f"Failed to get reparse type for {path}: {e}")
        return None


def get_file_id(filepath, updatehlinks=False):

    try:
        handle = win32file.CreateFile(
            filepath,
            win32con.GENERIC_READ,
            win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE | win32con.FILE_SHARE_DELETE,
            None,
            win32con.OPEN_EXISTING,
            win32con.FILE_ATTRIBUTE_NORMAL,
            None
        )

        try:
            # sym = None

            info = win32file.GetFileInformationByHandle(handle)

            # attrs = info[0]
            # if attrs & win32con.FILE_ATTRIBUTE_REPARSE_POINT:
            #     sym = "y"

            file_index = (info[8] << 32) + info[9]  # FileIndexHigh FileIndexLow

            # hard_link = hardlink     fallback
            if updatehlinks:
                hard_link = info[7]
                hard_link = hard_link - 1 if hard_link else None
            else:
                hard_link = None

            # if prev_hlink_value and hard_link is None:
            #     hard_link = prev_hlink_value

            creation_frm = info[1].timestamp()  # pywin types dt object
            creation_time = datetime.fromtimestamp(creation_frm)  # creation_frm.astimezone()  .replace(tzinfo=None)

            return file_index, hard_link, creation_time  # , sym
        finally:
            handle.Close()
    except pywintypes.error as e:
        if e.winerror in (2, 3):  # file not found, path not found
            return "not_found", None, None
        return None, None, None


def ishlink(st=None, fpath=None):
    try:
        if st is None and fpath is None:
            logging.debug("ishlink no args given. returning None")
            return None
        if fpath is not None:
            st = fpath.stat()
        # inode = st.st_ino
        return st.st_nlink  # return int instead of string
    except Exception:
        return None


def issym(ppath):
    try:
        return ppath.is_symlink()
    except (FileNotFoundError, PermissionError, OSError):
        return False


def get_onr(fpath):
    try:
        sd = win32security.GetFileSecurity(fpath, win32security.OWNER_SECURITY_INFORMATION)
        owner_sid = sd.GetSecurityDescriptorOwner()
        name, domain, _ = win32security.LookupAccountSid(None, owner_sid)
        return name, domain
    except Exception:
        return None


def get_mode(fp, st, sym):
    mode = ['-'] * 6  # PowerShell
    if sym == 'y':
        mode[5] = 'l'
    if (hasattr(st, 'st_file_attributes') and (st.st_file_attributes & 0x1)) or not os.access(fp, os.W_OK):
        mode[2] = 'r'
    if hasattr(st, 'st_file_attributes'):
        attr = st.st_file_attributes
        # FILE_ATTRIBUTE_HIDDEN = 0x2
        # FILE_ATTRIBUTE_SYSTEM = 0x4
        # FILE_ATTRIBUTE_ARCHIVE = 0x20
        if attr & win32con.FILE_ATTRIBUTE_HIDDEN:
            mode[3] = 'h'
        if attr & win32con.FILE_ATTRIBUTE_SYSTEM:
            mode[4] = 's'
        if attr & win32con.FILE_ATTRIBUTE_ARCHIVE:
            mode[1] = 'a'
    return ''.join(mode)

# try:
#     if hasattr(st, 'st_mode'):   linux?

#             if not (st.st_mode & stat.S_IWUSR):
#                 mode[4] = 'r'


def get_mfmode(attribs, sym):
    mode = ["-"] * 6
    if "Archive" in attribs:
        mode[1] = "a"
    if "ReadOnly" in attribs:
        mode[2] = "r"
    if "Hidden" in attribs:
        mode[3] = "h"
    if "System" in attribs:
        mode[4] = "s"
    if sym or "ReparsePoint" in attribs:
        mode[5] = "l"
    # if "Compressed" in attribs:
    #     mode[5] = "c"
    # if "Temporary" in attribs:
    #     mode[6] = "t"
    # if "SparseFile" in attribs:
    #     mode[7] = "x"

    return "".join(mode)


# default unknown and set if symlink
def defaultm(sym):
    mode = ['-'] * 6
    if sym:
        mode[5] = "l"
    return ''.join(mode)


# write a list of exclude paths for powershell search scripts
def set_excl_dirs(basedir, excl_path, EXCLDIRS):
    with open(excl_path, "w") as f:
        for entry in EXCLDIRS:
            str_out = basedir + entry.replace("$", "\\$")
            f.write(f"{str_out}\n")
