import logging
import os
import pywintypes
import stat
import win32con
import win32file
import win32security
from datetime import datetime
from .logs import emit_log


def upt_cache(cfr, checks, file_size, time_stamp, modified_ep, file_path):

    if not checks:
        return
    versions = cfr.setdefault(file_path, {})
    row = versions.get(modified_ep)

    if row and row.get("checksum") == checks and row.get("size") == file_size:
        return

    cfr[file_path][modified_ep] = {
        "checksum": checks,
        "size": file_size,
        "modified_time": time_stamp,
    }


def get_cached(cfr, file_size, modified_ep, file_path):
    if not isinstance(cfr, dict):
        return None

    versions = cfr.get(file_path)
    if not versions:
        return None

    if modified_ep is not None:
        row = versions.get(modified_ep)
        if row:
            row_size = row.get("size")
            if (
                row_size is not None
                and file_size == row["size"]
                and row.get("checksum")
            ):
                return {
                    "checksum": row.get("checksum"),
                    "modified_ep": modified_ep
                }

    return None


# return the last known modified_ep
def get_last_mtime(cfr, file_path, latest_ep):
    if not isinstance(cfr, dict):
        return None

    versions = cfr.get(file_path)
    if not isinstance(versions, dict) or not versions:
        return None

    candidates = [ep for ep in versions.keys() if ep not in (None, '', latest_ep)]
    if not candidates:
        return None

    return max(candidates)


def normalize_timestamp(mod_time: str) -> int:
    sec, dot, frac = mod_time.partition(".")
    if not dot:
        frac = "0"
    frac = (frac + "000000")[:6]
    return int(sec) * 1_000_000 + int(frac)


def iso8601_utc(s, log_q):
    try:
        return datetime.fromisoformat(s)
    except (ValueError, TypeError, AttributeError) as e:
        emit_log("ERROR", f"parse_iso8601: invalid date format: {s} {e} ", log_q)
        return None


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


def get_reparse_type(path):
    try:
        attrs = win32file.GetFileAttributes(path)
        if not (attrs & win32con.FILE_ATTRIBUTE_REPARSE_POINT):
            return None
        if os.path.islink(path):
            return "symlink"
        return "reparse"
    except Exception as e:
        logging.debug(f"Failed to get reparse type for {path}: {e}")
        return None


def is_reparse_point(st):
    return bool(getattr(st, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT)


# OPEN_REPARSE = getattr(win32file, "FILE_FLAG_OPEN_REPARSE_POINT", 0x00200000)
def get_file_id(filepath, log_q=None, logger=None):
    try:
        # win32con.FILE_ATTRIBUTE_NORMAL ln 125
        handle = win32file.CreateFile(
            filepath,
            win32con.GENERIC_READ,
            win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE | win32con.FILE_SHARE_DELETE,
            None,
            win32con.OPEN_EXISTING,
            win32file.FILE_FLAG_OPEN_REPARSE_POINT,
            None
        )
        try:
            sym = None

            info = win32file.GetFileInformationByHandle(handle)

            attrs = info[0]
            if attrs & win32con.FILE_ATTRIBUTE_REPARSE_POINT:
                sym = "y"

            # size = (info[5] << 32) | info[6]

            file_index = (info[8] << 32) + info[9]
            hard_link = info[7]

            creation_frm = info[1].timestamp()  # pywin types dt object
            creation_time = datetime.fromtimestamp(creation_frm)  # creation_frm.astimezone()  .replace(tzinfo=None)

            # creation_us = int(creation_frm) * 1_000_000
            # print("creation_us", creation_us)

            # atime_ts = info[2]

            # mtime_frm = info[3].timestamp() last write time
            # m_time = datetime.fromtimestamp(mtime_frm)

            # mtime_us = int(mtime_frm) * 1_000_000
            # print("mtime_us", creation_us)
            # mode
            is_hidden = bool(attrs & win32con.FILE_ATTRIBUTE_HIDDEN)
            is_system = bool(attrs & win32con.FILE_ATTRIBUTE_SYSTEM)
            is_archive = bool(attrs & win32con.FILE_ATTRIBUTE_ARCHIVE)
            is_readonly = bool(attrs & win32con.FILE_ATTRIBUTE_READONLY)
            mode = ['-'] * 6  # PowerShell
            if sym == 'y':
                mode[5] = 'l'
            if is_readonly or not os.access(filepath, os.W_OK):
                mode[2] = 'r'
            if is_hidden:
                mode[3] = 'h'
            if is_system:
                mode[4] = 's'
            if is_archive:
                mode[1] = 'a'
            mode = ''.join(mode)
            # end mode

            return file_index, sym, hard_link, creation_time, mode
        finally:
            handle.Close()

    except pywintypes.error as e:
        if e.winerror in (2, 3):  # file not found, path not found
            return "not_found", None, None, None
        emsg = f"get_file_id unable to get inode symlinks hardlinks creationtime mode file: {filepath} {type(e).__name__} error: {e}"
        if log_q:
            emit_log("DEBUG", emsg, log_q)
        elif logger:
            logger.debug(emsg)
        return None, None, None, None, None


def file_owner(fpath, logger=None):
    try:
        sd = win32security.GetFileSecurity(fpath, win32security.OWNER_SECURITY_INFORMATION)
        owner_sid = sd.GetSecurityDescriptorOwner()
        name, domain, _ = win32security.LookupAccountSid(None, owner_sid)
        return name, domain
    except Exception as e:
        emit_log("DEBUG", f"file_owner unable to resolve owner domain file: {fpath} error: {e}", logger)
        return None


def get_mode(fp, st, is_symlink):

    mode = ['-'] * 6  # PowerShell
    if is_symlink == 'y':
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


def get_mft_mode(attribs, is_symlink=None):
    sym = None
    mode = ["-"] * 6
    if "Archive" in attribs:
        mode[1] = "a"
    if "ReadOnly" in attribs:
        mode[2] = "r"
    if "Hidden" in attribs:
        mode[3] = "h"
    if "System" in attribs:
        mode[4] = "s"
    if is_symlink or "ReparsePoint" in attribs:
        sym = "y"
        mode[5] = "l"
    # if "Compressed" in attribs:
    #     mode[5] = "c"
    # if "Temporary" in attribs:
    #     mode[6] = "t"
    # if "SparseFile" in attribs:
    #     mode[7] = "x"
    return "".join(mode), sym


def default_mode(is_symlink):
    # default unknown and set if symlink
    mode = ['-'] * 6
    if is_symlink:
        mode[5] = "l"
    return ''.join(mode)


def set_excl_dirs(basedir, excl_path, EXCLDIRS):
    """ write a list of exclude paths for powershell search scripts """
    with open(excl_path, "w") as f:
        for entry in EXCLDIRS:
            #  str_out = basedir + entry.replace("$", "\\$")
            str_out = os.path.join(basedir, entry.lstrip("\\/"))
            f.write(f"{str_out}\n")


# def getacl(ffile):
#     acl_s = []
#     try:
#         sd = win32security.GetFileSecurity(ffile, win32security.DACL_SECURITY_INFORMATION)
#         dacl = sd.GetSecurityDescriptorDacl()
#         if dacl is None:
#             return None
#
#         for i in range(dacl.GetAceCount()):
#             ace = dacl.GetAce(i)
#             acl_s.append(str(ace))
#         return acl_s
#     except FileNotFoundError:
#         return None
#     except pywintypes.error as e:
#         return None
# sid = ace[2]
# name, domain, _ = win32security.LookupAccountSid(None, sid)
# print(f"{domain}\\{name}: {ace[1]}")  # ace[1] = access mask
