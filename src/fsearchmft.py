
#  Mft parallel                12/08/2025
#  Certain fields are different from fsearch and fsearchps1. The MFT doesnt have to walk the filesystem.
# C# and rust parsers are efficient. This can be invaluable in locating a file or file(s).
import logging
import multiprocessing
import random
import os
import pandas as pd
import traceback
from datetime import datetime
from pathlib import Path
from .fsearch import process_res
from .fsearchfnts import calculate_checksum
from .fsearchfnts import defaultm
from .fsearchfnts import get_cached
from .fsearchfnts import get_onr
from .fsearchfnts import get_mfmode
from .fsearchfnts import issym
from .pyfunctions import setup_logger

# mftecmd


# Parallel SORTCOMPLETE search and  ctime hashing
#
def process_line(line, checksum, CACHE_F):

    fmt = "%Y-%m-%d %H:%M:%S"

    label = "Sortcomplete"
    CSZE = 1024 * 1024

    checks = None
    cache_owner = None
    cache_domain = None
    mode = None

    mtime = line[0]
    ctime = line[1]
    atime = line[2]
    size = line[3]
    lastmodified = line[4]
    mode_attribs = line[5]
    hardlink = line[6]
    inode = line[7]
    cam = line[8]
    file_path = line[9]
    if not os.path.exists(file_path):
        return None
    if not os.path.isfile(file_path):
        if not mtime:
            mtime = datetime.now().strftime(fmt)
        return ("Nosuchfile", mtime, mtime, file_path)
    if mtime is None:
        return None
    if isinstance(mtime, pd.Timestamp):
        mtime = mtime.to_pydatetime()

    pathf = Path(file_path)

    if checksum:

        if size is not None and size > CSZE:
            mod_time = mtime.strftime(fmt)
            cached = get_cached(CACHE_F, size, mod_time, file_path)
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

    sym = "y" if issym(pathf) else None
    if mode_attribs != "None":
        mode = get_mfmode(mode_attribs, sym)
    if not mode:
        mode = defaultm(sym)
        logging.debug("missing mode_attribs line: %s", line)

    resolve_onr = get_onr(file_path)
    owner, domain = resolve_onr if resolve_onr else (None, None)

    if not owner and cache_owner:
        owner = cache_owner
    if not domain and cache_domain:
        domain = cache_domain

    return (
        label,
        mtime,  # .replace(microsecond=0)
        file_path,
        ctime,
        inode,
        atime,
        checks,
        size,
        sym,
        owner,
        domain,
        mode,
        cam,
        lastmodified,
        hardlink
    )


def process_line_worker(chunk_args):

    try:
        chunk, checksum, logging_values, CACHE_F, strt, endp, special_k, chunk_index = chunk_args
    except (TypeError, ValueError) as e:
        print(f"Error processing a line args skipping one process {type(e).__name__} {e} traceback:\n {traceback.format_exc()}")
        return None

    setup_logger(logging_values[1], "fsearchMFT", logging_values[0])

    results = []

    delta_p = 0
    dbit = False
    if chunk_index == special_k:
        dbit = True
        delta_p = endp - strt

    for i, line in enumerate(chunk):
        try:
            result = process_line(line, checksum, CACHE_F)

        except Exception as e:
            err_msg = f"fsearchmft.py Error processing line {i} in chunk {chunk_index}: {type(e).__name__}: {e}"
            print(err_msg)
            logging.error(err_msg, exc_info=True)
            result = None

        if dbit:
            prog_i = strt + ((i + 1) / len(chunk)) * delta_p
            print(f'Progress: {prog_i:.2f}', flush=True)

        if result is not None:
            results.append(result)
    return results


def process_lines(lines, model_type, checksum, table, logging_values, CACHE_F, iqt=False, strt=20, endp=60):

    special_k = -1

    if len(lines) < 30 or model_type.lower() == "hdd":

        if iqt:
            special_k = 0

        chunk_args = [(lines, checksum, logging_values, CACHE_F, strt, endp, special_k, 0)]
        ck_results = [process_line_worker(arg) for arg in chunk_args]

    else:

        min_chunk_size = 10
        max_workers = max(1, min(8, os.cpu_count() or 4, len(lines) // min_chunk_size))

        chunk_size = max(1, (len(lines) + max_workers - 1) // max_workers)
        chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

        if iqt:
            special_k = random.randint(0, len(chunks) - 1)

        chunk_args = [(chunk, checksum, logging_values, CACHE_F, strt, endp, special_k, idx) for idx, chunk in enumerate(chunks)]

        with multiprocessing.Pool(processes=max_workers) as pool:
            ck_results = pool.map(process_line_worker, chunk_args)

    results = [item for sublist in ck_results if sublist is not None for item in sublist]

    return process_res(results, table, CACHE_F, "fsearchMFT") if results else (None, None)  # fsearch.py


def process_find_lines(lines, model_type, checksum, table, logging_values, CACHE_F, iqt=False, strt=20, endp=60):
    return process_lines(lines, model_type, checksum, table, logging_values, CACHE_F, iqt, strt, endp)
#
# End parallel #
