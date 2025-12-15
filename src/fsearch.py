# Get metadata hash of files and return array                       12/09/2025
import logging
import multiprocessing
import os
import random
import traceback
from datetime import datetime
from pathlib import Path
from .fsearchfnts import defaultm
from .fsearchfnts import get_mode
from .fsearchfnts import issym
from .fsearchfnts import upt_cache
from .fsearchfnts import get_file_id
from .fsearchfnts import get_cached
from .fsearchfnts import get_onr
from .fsearchfnts import calculate_checksum
from .pyfunctions import setup_logger
from .pyfunctions import epoch_to_date
from .pyfunctions import is_integer
# from .fsearchfnts import ishlink

fmt = "%Y-%m-%d %H:%M:%S"


# Parallel SORTCOMPLETE search and  ctime hashing
#
# change_time = line[2]
# inode = line[3]
# owner = line[5]
# domain = line[6]

def process_line(line, checksum, updatehlinks, file_type, search_start_dt, CACHE_F):

    label = "Sortcomplete"
    CSZE = 1024 * 1024

    size_int = None
    cache_owner = None
    cache_domain = None
    owner = None
    domain = None
    checks = None
    sym = None
    mode = None
    cam = None
    last_modified = None

    if len(line) < 9:
        logging.debug("process_line index error missing value from find command expected 9, %s", line)
        return None

    mod_time = line[0]
    access_time = line[1]
    size = line[4]
    file_path = line[8]

    if not os.path.exists(file_path):
        return None
    mtime = epoch_to_date(mod_time)
    if not os.path.isfile(file_path):
        if not mtime:
            mt = datetime.now().strftime(fmt)
        else:
            mt = mtime.replace(microsecond=0)
        return ("Nosuchfile", mt, mt, file_path)
    if mtime is None:
        logging.debug("process_line date conversion mtime failed %s file %s, line: %s", mod_time, file_path, line)
        return

    inode, hardlink, c_time = get_file_id(file_path, updatehlinks)  # py32win
    if inode == "not_found":
        mt = mtime.replace(microsecond=0)
        logging.debug("process_line no such file return from py32win : {line}")
        return ("Nosuchfile", mt, mt, file_path)
    if not c_time:
        logging.debug("process_line file no creation time from py32win file: %s line: %s", file_path, line)
        if file_type == "ctime":
            return

    if not (file_type == "ctime" and c_time > mtime and c_time >= search_start_dt) and file_type != "main":
        return

    pathf = Path(file_path)
    try:
        st = pathf.stat()  # hardlink = str(st.st_nlink) linux
    except Exception:
        st = None

    try:
        if is_integer(size):
            size_int = int(size)
        else:
            if st:
                size_int = st.st_size
            else:
                size_int = os.path.getsize(file_path)
    except Exception as e:
        logging.debug("couldnt resolve size int for file: %s, line %s size int is None. %s err: %s", file_path, line, type(e).__name__, e)

    if checksum:
        if size_int is not None and size_int > CSZE:

            cached = get_cached(CACHE_F, size_int, mtime.timestamp(), file_path)

            if cached is None:
                checks = calculate_checksum(file_path)
                if checks:
                    label = "Cwrite"
            else:
                checks = cached.get("checksum")
                cache_owner = cached.get("owner")
                cache_domain = cached.get("domain")
        else:
            checks = calculate_checksum(file_path)

    if st:
        if issym(pathf):
            sym = "y"
        # ishlink(st) else None alternative future linux ect
        mode = get_mode(file_path, st, sym)
        resolve_onr = get_onr(file_path)
        owner, domain = resolve_onr if resolve_onr else (None, None)

    if not owner and cache_owner:
        owner = cache_owner
        logging.debug("was unable to reserve owner file: %s, falled back to cached owner: %s", file_path, cache_owner)
    if not domain and cache_domain:
        domain = cache_domain
        logging.debug("was unable to resolve domain file: %s, falled back to cached domain: %s", file_path, cache_domain)
    if not mode:
        mode = defaultm(sym)

    atime = epoch_to_date(access_time)

    if file_type == "ctime":
        last_modified = mtime
        mtime = c_time
        cam = "y"

    # tuple
    return (
        label,
        mtime.replace(microsecond=0),
        file_path,
        c_time.strftime(fmt) if c_time is not None else None,
        inode,
        atime.strftime(fmt) if atime is not None else None,
        checks,
        size_int,
        sym,
        owner,
        domain,
        mode,
        cam,
        last_modified.strftime(fmt) if last_modified is not None else None,
        hardlink,
        str(mtime.timestamp())
    )


def process_line_worker(chunk_args):

    try:
        chunk, checksum, updatehlinks, logging_values, file_type, search_start_dt, CACHE_F, strt, endp, special_k, chunk_index = chunk_args
    except (ValueError, TypeError) as e:
        print(f"Error entering processing line in process_line_worker: {type(e).__name__} {e} traceback:\n {traceback.format_exc()}")
        return None

    setup_logger(logging_values[1], "FSEARCH", logging_values[0])

    delta_p = 0
    dbit = False
    if chunk_index == special_k:
        dbit = True
        delta_p = endp - strt

    results = []

    for i, line in enumerate(chunk):
        try:

            result = process_line(line, checksum, updatehlinks, file_type, search_start_dt, CACHE_F)

        except Exception as e:
            len_chunk = len(chunk)
            logging.error("process_line_worker Error processing %s of %s in chunk %s", i, len_chunk, chunk_index, exc_info=True)
            logging.error(e)
            result = None

        if dbit:
            prog_i = strt + ((i + 1) / len(chunk)) * delta_p
            print(f'Progress: {prog_i:.2f}', flush=True)

        if result is not None:
            results.append(result)
    return results


def process_lines(lines, model_type, checksum, updatehlinks, file_type, search_start_dt, logging_values, CACHE_F, iqt=False, strt=20, endp=60):

    special_k = -1

    if len(lines) < 30 or model_type.lower() == "hdd":

        if iqt:
            special_k = 0

        chunk_args = [(lines, checksum, updatehlinks, logging_values, file_type, search_start_dt, CACHE_F, strt, endp, special_k, 0)]
        ck_results = [process_line_worker(arg) for arg in chunk_args]

    else:

        min_chunk_size = 10
        max_workers = max(1, min(8, os.cpu_count() or 4, len(lines) // min_chunk_size))

        chunk_size = max(1, (len(lines) + max_workers - 1) // max_workers)
        chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

        if iqt:
            special_k = random.randint(0, len(chunks)-1)

        chunk_args = [(chunk, checksum, updatehlinks, logging_values, file_type, search_start_dt, CACHE_F, strt, endp, special_k, idx) for idx, chunk in enumerate(chunks)]

        with multiprocessing.Pool(processes=max_workers) as pool:
            ck_results = pool.map(process_line_worker, chunk_args)

    results = [item for sublist in ck_results if sublist is not None for item in sublist]  # flatten the list

    return process_res(results, CACHE_F, "FSEARCH") if results else ([], [])


def process_res(results, CACHE_F, process_label):

    logger = logging.getLogger(process_label)
    sortcomplete = []
    complete = []
    cwrite = []

    for entry in results:
        if entry is None:
            continue
        if isinstance(entry, tuple) and len(entry) > 0:
            if entry[0] == "Nosuchfile":
                complete.append(entry)
                # epath = escf_py(res[3]) if len(res) > 3 else ""
                # complete.append((res[0], res[1], res[2], res[3]))

            elif entry[0] == "Cwrite":
                cwrite.append(entry[1:])
                sortcomplete.append(entry[1:])
            else:
                sortcomplete.append(entry[1:])
    try:
        existing_keys = set()

        if cwrite:

            if CACHE_F:
                for root, versions in CACHE_F.items():
                    for modified_ep, row in versions.items():
                        key = (
                            row.get("checksum"),
                            row.get("size"),
                            modified_ep,
                            root
                        )
                        existing_keys.add(key)

            for res in cwrite:
                checksum = res[5]
                file_size = res[6]
                time_stamp = res[0].strftime("%Y-%m-%d %H:%M:%S")
                modified_ep = res[14]
                owner = res[8]
                domain = res[9]
                file_path = res[1]

                upt_cache(CACHE_F, existing_keys, checksum, file_size, time_stamp, modified_ep, owner, domain, file_path)
    except Exception as e:
        print(f"Error updating cache: {type(e).__name__}: {e}")
        logger.error(f"Error updating cache: {e}", exc_info=True)

    return sortcomplete, complete


def process_find_lines(lines, model_type, checksum, updatehlinks, file_type, search_start_dt, logging_values, CACHE_F, iqt=False, strt=20, endp=60):
    return process_lines(lines, model_type, checksum, updatehlinks, file_type, search_start_dt, logging_values, CACHE_F, iqt, strt, endp)
#
# End parallel #
#
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
