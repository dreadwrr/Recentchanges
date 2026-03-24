import math
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from .logs import emit_log
from . import logs
from .pyfunctions import is_valid_datetime
from .pyfunctions import is_integer
from .pyfunctions import matches_any_pattern
from .pyfunctions import new_meta
from .pyfunctions import parse_datetime
from .pyfunctions import sys_record_flds
from .pysql import get_recent_changes
from .pysql import get_recent_sys
# hybrid analysis 11/19/2025 01/08/2026 python version 03/16/2026 linux Qt


def stealth(filename, label, entry, current_size, original_size, cdiag):

    if current_size and original_size:
        file_path = Path(filename)
        if file_path.is_file():
            delta = abs(current_size - original_size)

            if original_size == current_size:
                entry["cerr"].append(f'Warning file {label} same filesize different checksum. Contents changed.')

            elif delta < 12 and delta != 0:
                message = f'Checksum indicates a change in {label}. Size changed slightly - possible stealth edit.'

                if cdiag:
                    entry["scr"].append(f'{message} ({original_size} → {current_size}).')
                else:
                    entry["scr"].append(message)


def hanly(parsed_chunk, checksum, cdiag, dbopt, ps, usr, logging_values, sys_tables, cachermPATTERNS, show_progress=False, logger=None, strt=65, endp=90):

    results, sys_records, log_entries = [], [], []
    if logger:
        log_entries = None
    if not ps:
        sys_tables = ()

    fmt = "%Y-%m-%d %H:%M:%S"
    time_period = 5  # days for a file that isnt regularly updated. 5 default

    dbit = False
    csum = False

    with sqlite3.connect(dbopt) as conn:
        cur = conn.cursor()

        r = x = 0
        delta_v = 0
        current_step = 0

        steps = []
        if show_progress:
            delta_v = endp - strt
            dbit = True
            total_e = len(parsed_chunk)
            steps = sorted({math.ceil(i * total_e / 10) for i in range(1, 11)})
            step_len = len(steps)
        for record in parsed_chunk:
            r += 1
            x += 1
            if dbit:
                if current_step < step_len and r >= steps[current_step]:
                    if logger:
                        prog_v = strt + round(delta_v * (steps[current_step] / total_e))  # single core
                        print(f"Progress: {prog_v}%", flush=True)
                    else:
                        emit_log("prog", x, logs.WORKER_LOG_Q)  # multi
                        x = 0
                    current_step += 1  # end progress

            previous_timestamp = None
            recent_sym = None
            current_size = None
            original_size = None
            is_sys = False

            if len(record) < 16:

                emit_log("DEBUG", f"sortcomplete entry malformed.  less than required 17 : {record}", logs.WORKER_LOG_Q, logger=logger)
                continue

            entry = {"cerr": [], "flag": [], "scr": [], "sys": [], "dcp": []}

            recent_timestamp = parse_datetime(record[0], fmt)
            if not recent_timestamp:

                emit_log("DEBUG", f"missing timestamp on parsed entry: {record}", logs.WORKER_LOG_Q, logger=logger)
                continue

            filename = record[1]
            label = filename

            recent_entries = get_recent_changes(filename, cur, 'logs', ['mtime_us'])
            recent_sys = get_recent_sys(filename, cur, sys_tables, ['mtime_us', 'count']) if ps else None

            if not recent_entries and not recent_sys and checksum:
                entry["dcp"].append(record)  # is copy?
                results.append(entry)
                continue

            previous = recent_entries

            if ps and recent_sys and len(recent_sys) > 14:

                previous_timestamp = parse_datetime(recent_sys[0], fmt)

                if previous_timestamp:

                    is_sys = True
                    previous = recent_sys

                    def insert_sys_entry(entry, record, recent_sys, sys_records):

                        prev_count = recent_sys[-1]
                        sys_record_flds(record, sys_records, prev_count)
                    previous_sysctime = parse_datetime(recent_sys[2], fmt)
                    recent_ctime = parse_datetime(record[2], fmt)

                    if (
                        (recent_timestamp > previous_timestamp)
                        or (recent_ctime and previous_sysctime and recent_ctime > previous_sysctime)
                        or not (previous_sysctime or previous[5])
                        or (record[5] and previous[5] and record[5] != previous[5])
                    ):
                        insert_sys_entry(entry, record, recent_sys, sys_records)

                else:
                    emit_log("ERROR", f"recent sys entry missing mtime skipping recent_sys: {recent_sys}", logs.WORKER_LOG_Q, logger=logger)

            elif ps and recent_sys:
                emit_log("DEBUG", f"recent sys entry less than required length 14 : recent_sys: {recent_sys}", logs.WORKER_LOG_Q, logger=logger)

            if previous is None or len(previous) < 13:
                emit_log("DEBUG", f"previous record less than required length 14. previous: {previous}", logs.WORKER_LOG_Q, logger=logger)
                continue
            if checksum:

                recent_sym = record[7]
                previous_sym = previous[7]
                current_size = record[6]
                original_size = previous[6]

                if not record[5] or not previous[5]:
                    # emit_log("DEBUG", f"No checksum for file {record} \n recent {previous}", logs.WORKER_LOG_Q, logger=logger)
                    continue

                if not os.path.isfile(filename):
                    entry["flag"].append(f'Deleted {record[0]} {record[0]} {label}')
                    results.append(entry)
                    continue

                if logging_values[1] == "DEBUG":
                    if current_size is None or original_size is None:
                        emit_log("DEBUG", f"invalid format detected size not an integer record: {record} and previous: {previous}", logs.WORKER_LOG_Q, logger=logger)

            if not is_sys:
                previous_timestamp = parse_datetime(previous[0], fmt)

            if (
                (is_integer(record[3]) and is_integer(previous[3]))
                and previous_timestamp
            ):  # format check
                recent_cam = record[11]
                previous_cam = previous[11]
                cam_file = (recent_cam == "y" or previous_cam == "y")

                mtime_usec_zero = record[15]
                if is_integer(mtime_usec_zero) and mtime_usec_zero % 1_000_000 == 0:
                    entry["scr"].append(f'Unusual modified time file has microsecond all zero: {label} timestamp: {mtime_usec_zero}')

                if recent_timestamp == previous_timestamp:
                    if checksum:
                        # st = goahead(filename, logs.WORKER_LOG_Q, logger=logger)
                        # if st == "Nosuchfile":
                        #     entry["flag"].append(f'Deleted {record[0]} {record[0]} {label}')
                        #     results.append(entry)
                        #     continue
                        # elif st:
                        #     a_mod = st.st_mtime
                        #     afrm_dt = epoch_to_date(a_mod)
                        #     a_mod_us = st.st_mtime_ns // 1000
                        #     a_size = st.st_size
                        #     a_ino = st.st_ino
                        #     owner_domain = file_owner(file_path, logs.WORKER_LOG_Q, logger=logger)
                        #     if owner_domain in (None, "Nosuchfile"):
                        #          emit_log("DEBUG", f""hanly failed to convert convert uid to user name for user {st.st_uid} line: {record}", logs.WORKER_LOG_Q, log_entries, logger=logger)
                        #          entry["flag"].append(f'Deleted {record[0]} {record[0]} {label}')
                        #          results.append(entry)
                        #          continue
                        #     owner, domain = owner_domain if owner_domain else (None, None)
                        #     attrs = getattr(st, "st_file_attributes", 0)
                        #     sym = None
                        #     if is_reparse_point(st):
                        #          sym = "yes"
                        #     mode = get_mode(attrs, sym)
                        #     or
                        #     _, _, _, _, _, mode, status = get_file_id(file_path, logs.WORKER_LOG_Q, logger=logger)  # inode reparse hardlink c_time
                        #     if status in ("Nosuchfile", "Error"):
                        #          entry["flag"].append(f'Deleted {record[0]} {record[0]} {label}')
                        #          results.append(entry)
                        #          continue
                        #     a_ctime = st.st_ctime
                        #     ctime_str = epoch_to_date(a_ctime).replace(microsecond=0)
                        #     ln 211
                        # else:
                        #     emit_log("DEBUG", f"Skipping {file_path} couldnt stat in ha current record {record} \n previous record {previous}", logs.WORKER_LOG_Q, log_entries, logger=logger)
                        #     continue
                        if is_valid_datetime(record[4], fmt):  # access time format check
                            previous_mtime_us = previous[13]
                            if isinstance(previous_mtime_us, int) and mtime_usec_zero == previous_mtime_us:
                                if not cam_file and recent_sym != "y":
                                    if record[5] != previous[5]:
                                        csum = True
                                        entry["flag"].append(f'Suspect {record[0]} {record[2]} {label}')
                                        entry["cerr"].append(f'Suspect file: {label} previous checksum {previous[5]} currently {record[5]}. changed without a new modified time.')

                                if record[3] == previous[3]:  # inode

                                    metadata = (previous[8], previous[9], previous[10])
                                    if new_meta((record[8], record[9], record[10]), metadata):
                                        entry["flag"].append(f'Metadata {record[0]} {record[2]} {label}')
                                        entry["scr"].append(f'Permissions of file: {label} changed {metadata[0]} {metadata[1]} {metadata[2]} → {record[8]} {record[9]} {record[10]}')
                                # else:  # Shifted during search
                                #     if not cam_file:
                                #         if cdiag:
                                #             entry["scr"].append(f'File changed during the search. {label} at {afrm_dt}. Size was {original_size}, now {a_size}')
                                #         else:
                                #             entry["scr"].append('File changed during search. File likely changed. system cache item.')
                                #
                                # since the modified time changed you could run checks from else block below. It would make the function messy with refactoring all the checks. Also these
                                # files are either system or cache files. Would also lead to repeated feedback when the search is ran again. This check provides feedback of what files are actively
                                # changing on the system which is sufficient
                                # md5 = None
                                # cacheable = current_size > CSZE
                                # if current_size is not None:
                                #     if cacheable:
                                #         md5 =
                                #     else:
                                #         md5 = record[5] # file wasnt cached and was calculated in fsearch earlier
                                # md5, file_dt, file_us, file_st, status = calculate_checksum(file_path, afrm_dt, a_mod, a_ino, a_size, prev_hash=None, st=None, retry=2, max_retry=2, cacheable=True, logs.WORKER_LOG_Q, logger=logger)
                                # if md5 is not None:
                                #     if status == "Retried":
                                #         md5, mtime, st, mtime_us, c_time, inode, size = set_stat(line, md5, file_dt, file_st, file_us, a_ino, logs.WORKER_LOG_Q, logger=logger)
                                # else:
                                #     if status == "Nosuchfile":
                                #          entry["flag"].append(f'Deleted {record[0]} {record[0]} {label}')
                                #          results.append(entry)
                                #          continue
                                # if md5:
                                #     if md5 != previous[5]:
                                #         stealth(filename, label, entry, a_size, original_size, cdiag)
                                # if a_ino == previous[3]:
                                #     metadata = (previous[8], previous[9], previous[10])
                                #     if new_meta((auid, agid, aperm), metadata):
                                #         entry["flag"].append(f'Metadata {record[0]} {record[2]} {label}')
                                #         entry["scr"].append(f'Permissions of file: {label} changed {metadata[0]} {metadata[1]} {metadata[2]} → {auid} {agid} {aperm}')

                else:

                    if checksum:

                        if record[3] != previous[3]:

                            if record[5] == previous[5]:

                                entry["flag"].append(f'Overwrite {record[0]} {record[2]} {label}')
                            else:
                                entry["flag"].append(f'Replaced {record[0]} {record[2]} {label}')
                                stealth(filename, label, entry, current_size, original_size, cdiag)

                        else:

                            if record[5] != previous[5]:

                                entry["flag"].append(f'Modified {record[0]} {record[2]} {label}')
                                stealth(filename, label, entry, current_size, original_size, cdiag)
                            else:

                                if recent_sym == "y" and previous_sym == "y":
                                    link_target = record[12]
                                    prev_target = previous[12]
                                    if link_target != prev_target:
                                        entry["scr"].append(f'Symlink target change {prev_target} → {link_target}')

                                else:
                                    metadata = (previous[8], previous[9], previous[10])
                                    if new_meta((record[8], record[9], record[10]), metadata):
                                        entry["flag"].append(f'Metadata {record[0]} {record[2]} {label}')
                                        entry["scr"].append(f'Permissions of file: {label} changed {metadata[0]} {metadata[1]} {metadata[2]} → {record[8]} {record[9]} {record[10]}')
                                    else:
                                        if not cam_file:
                                            entry["flag"].append(f'Touched {record[0]} {record[2]} {label}')

                    else:
                        if record[3] != previous[3]:
                            entry["flag"].append(f'Replaced {record[0]} {record[2]} {label}')
                        else:
                            if not cam_file:
                                entry["flag"].append(f'Modified {record[0]} {record[2]} {label}')

                    if not cam_file:
                        time_delta = datetime.now() - timedelta(days=time_period)
                        if previous_timestamp < time_delta:
                            message = f'File that isnt regularly updated {label}.'
                            if is_sys:
                                entry["scr"].append(f'{message} and is a system file.')
                            else:
                                screen = cachermPATTERNS
                                if not matches_any_pattern(label, screen):
                                    entry["scr"].append(message)

                if entry["cerr"] or entry["flag"] or entry["scr"] or entry["sys"]:
                    results.append(entry)

            else:
                print("Hanly formatting problem was logged")
                em = (
                    f"current inode {record[3]} previous {previous[3]}, current timestamp {recent_timestamp} previous"
                    + f"{previous_timestamp} \n original {previous} \n current {record}"
                )
                emit_log("DEBUG", em, logs.WORKER_LOG_Q, logger=logger)

        if dbit and current_step <= len(steps) - 1:
            if logger:
                # prog_v = round(delta_v) + strt
                print(f"Progress: {endp}%", flush=True)  # :.2f
            else:
                emit_log("prog", x, logs.WORKER_LOG_Q)

    return results, sys_records, log_entries, csum
