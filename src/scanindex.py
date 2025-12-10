import logging
import os
from pathlib import Path
from .dirwalkerfnts import meta_sys
from .fsearchfnts import calculate_checksum
from .pyfunctions import setup_logger
from .pyfunctions import is_integer


def scan_index(chunk, updatehlinks, appdata_local, ll_level, strt, endp, i, special_k):

    setup_logger(ll_level, "scan_index", appdata_local)

    c = 0
    t_fold = 0
    last_printed = -1
    dbit = False
    if i == special_k:
        dbit = True
        t_fold = len(chunk)

    sys_data = []
    results = []

    x, y = 0, 0

    filename = None
    for record in chunk:
        try:
            c += 1
            if dbit:
                fraction = c / t_fold
                p_g = round(strt + fraction * (endp - strt), 2)
                if p_g > last_printed:
                    if p_g <= endp:
                        print(f'Progress: {p_g:.2f}%', flush=True)
                        last_printed = p_g
                    if p_g >= endp:
                        dbit = False
            filename = str(record[1])
            if os.path.isfile(filename):
                x += 1
                checksum = record[5]
                if checksum:
                    file_path = Path(filename)
                    md5 = calculate_checksum(file_path)
                    if md5:
                        if md5 != checksum:
                            rlt = meta_sys(file_path, filename, md5, updatehlinks, sys_data, record)  # append meta data for file to sys_data

                            if rlt is not None and not is_integer(rlt):
                                logging.debug("Hash skipped %s integer error. record: %s", filename, record)
            else:
                y += 1
                results.append(record)
        except (ValueError, IndexError) as e:
            emsg = f"Encountered an error processing record {c} of {len(chunk)}, file {filename}: {e}\n{type(e).__name__}"  # {traceback.format_exc()}
            print(emsg)
            logging.error("Error scan_index file: %s, record %s", filename, record, exc_info=True)

    return sys_data, results, x, y
