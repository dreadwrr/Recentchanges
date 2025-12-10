
# Build index
#
# Scan a drive for specified files and hash/get meta data.
#
# formerly scan_f # 12/08/2025
import logging
import os
from pathlib import Path
from .dirwalkerfnts import walk_meta
from .pyfunctions import setup_logger
from .pyfunctions import is_integer


def build_index(chunk, updatehlinks, appdata_local, ll_level, strt, endp, i, special_k):
    f_f = "build_index"
    setup_logger(ll_level, f_f, appdata_local)
    rec_count = 0
    c = 0
    dbit = False
    if i == special_k:
        dbit = True
        rec_count = len(chunk)
    sys_data = []

    last_printed = -1
    for record in chunk:
        c += 1
        if dbit:
            fraction = c / rec_count
            p_g = round(strt + fraction * (endp - strt), 2)
            if p_g > last_printed:
                if p_g <= endp:
                    print(f'Progress: {p_g:.2f}%', flush=True)
                    last_printed = p_g
                if p_g >= endp:
                    dbit = False

        file_path = Path(record)
        if os.path.isfile(record):
            walk_meta(file_path, record, updatehlinks, sys_data)
        else:
            logging.debug("file not found during the scan, skipping: %s", file_path)
    return sys_data
