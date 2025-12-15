# Get metadata hash of files and return array                       12/13/2025
import logging
import multiprocessing
import os
import random
import traceback
from datetime import datetime
from .fsearch import process_res
from .pyfunctions import setup_logger
from . import fsearchfnts

# Parallel SORTCOMPLETE search and  ctime hashing


def process_line(line, checksum, updatehlinks, CACHE_F):

    fmt = "%Y-%m-%d %H:%M:%S"

    label = "Sortcomplete"
    CSZE = 1024 * 1024

    ctime = None
    checks = None
    cache_owner = None
    cache_domain = None
    cam = None
    last_modified = None

    try:
        mod_time = line[0]
        file_path = line[1]
        change_time = line[2]
        access_time = line[4]
        size_int = line[6]
        sym = line[7]
        owner = line[8]
        domain = line[9]
        mode = line[10]
    except IndexError as e:
        print(f"process_line index error line {e} {type(e).__name__} {line}")  # \n{traceback.format_exc()}
        logging.debug("process_line index error line %s", line, exc_info=True)
        return None
    if not os.path.exists(file_path):
        return None

    mtime = fsearchfnts.parse_iso8601(mod_time)
    if not os.path.isfile(file_path):
        if not mtime:
            mt = datetime.now().strftime(fmt)
        else:
            mt = mtime.replace(microsecond=0)
        return ("Nosuchfile", mt, mt, file_path)
    if mtime is None:
        logging.debug("process_line mtime missing from: %s", line)
        return None

    if checksum:
        if size_int is not None and size_int > CSZE:
            cached = fsearchfnts.get_cached(CACHE_F, size_int, mtime.timestamp(), file_path)
            if cached is None:
                checks = fsearchfnts.calculate_checksum(file_path)
                if checks:
                    label = "Cwrite"
            else:
                checks = cached.get("checksum")
                cache_owner = cached.get("owner")
                cache_domain = cached.get("domain")
        else:
            checks = fsearchfnts.calculate_checksum(file_path)

    # pywin32
    inode, hardlink, ctime_pywin = fsearchfnts.get_file_id(file_path, updatehlinks)
    if inode == "not_found":
        mt = mtime.replace(microsecond=0)
        logging.debug("process_line no such file after checksum return from py32win %s : %s", file_path, line)
        return ("Nosuchfile", mt, mt, file_path)
    if not ctime_pywin:
        logging.debug("process_line file had no creation time from pywin32 diagnostic purpose %s : %s", file_path, line)

    if not owner:
        resolve_onr = fsearchfnts.get_onr(file_path)
        owner, domain = resolve_onr if resolve_onr else (None, None)

    if not owner and cache_owner:
        owner = cache_owner
        logging.debug("was unable to resolve owner file: %s, falled back to cached owner: %s", file_path, cache_owner)
    if not domain and cache_domain:
        domain = cache_domain
        logging.debug("was unable to resolve domain file: %s, falled back to cached domain: %s", file_path, cache_domain)

    # sym = "y" if fsearchfnts.issym(pathf) else None
    # try:
    #     if os.path.islink(file_path): linux
    #         sym = "y"
    # except Exception as e:
    #     pass

    ctime = fsearchfnts.parse_iso8601(change_time)
    if ctime and ctime > mtime:
        last_modified = mtime
        mtime = ctime
        cam = "y"
    elif not ctime:
        logging.debug("process_line creation time was None at casmod check: %s : %s", file_path, line)

    atime = fsearchfnts.parse_iso8601(access_time)
    # hardlink = hardlink - 1 if hardlink is not None else None

    return (
        label,
        mtime.replace(microsecond=0),
        file_path,
        ctime.strftime(fmt) if ctime is not None else None,
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
        chunk, checksum, updatehlinks, logging_values, CACHE_F, strt, endp, special_k, chunk_index = chunk_args
    except (TypeError, ValueError) as e:
        print(f"fsearchps1 Error in process_line_worker unpacking arguments for line processing {type(e).__name__} {e} traceback:\n {traceback.format_exc()}")
        return None

    setup_logger(logging_values[1], "fsearchPS1", logging_values[0])

    delta_p = 0
    dbit = False
    if chunk_index == special_k:

        dbit = True
        delta_p = endp - strt

    results = []

    for i, line in enumerate(chunk):
        try:
            result = process_line(line, checksum, updatehlinks, CACHE_F)

        except Exception as e:
            logging.error("fsearchps1.py Error processing line %s in chunk %s: %s", i, chunk_index, e, exc_info=True)
            result = None

        if dbit:
            prog_i = strt + ((i + 1) / len(chunk)) * delta_p
            print(f'Progress: {prog_i:.2f}', flush=True)

        if result is not None:
            results.append(result)
    return results


def process_lines(lines, model_type, checksum, updatehlinks, logging_values, CACHE_F, iqt=False, strt=20, endp=60):

    special_k = -1

    if len(lines) < 30 or model_type.lower() == "hdd":

        if iqt:
            special_k = 0

        chunk_args = [(lines, checksum, updatehlinks, logging_values, CACHE_F, strt, endp, special_k, 0)]
        ck_results = [process_line_worker(arg) for arg in chunk_args]

    else:

        min_chunk_size = 10
        max_workers = max(1, min(8, os.cpu_count() or 4, len(lines) // min_chunk_size))

        chunk_size = max(1, (len(lines) + max_workers - 1) // max_workers)
        chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

        if iqt:
            special_k = random.randint(0, len(chunks)-1)

        chunk_args = [(chunk, checksum, updatehlinks, logging_values, CACHE_F, strt, endp, special_k, idx) for idx, chunk in enumerate(chunks)]

        with multiprocessing.Pool(processes=max_workers) as pool:
            ck_results = pool.map(process_line_worker, chunk_args)


    results = [item for sublist in ck_results if sublist is not None for item in sublist]

    return process_res(results, CACHE_F, "fsearchPS1") if results else (None, None)


def process_find_lines(lines, model_type, checksum, updatehlinks, logging_values, CACHE_F, iqt=False, strt=20, endp=60):
    return process_lines(lines, model_type, checksum, updatehlinks, logging_values, CACHE_F, iqt, strt, endp)
#
# End parallel #
