# hybrid analysis  12/08/2025
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from .pyfunctions import get_delete_patterns
from .pyfunctions import get_recent_changes
from .pyfunctions import get_recent_sys
from .pyfunctions import goahead
from .pyfunctions import is_integer
from .pyfunctions import is_valid_datetime
from .pyfunctions import matches_any_pattern
from .pyfunctions import new_meta
from .pyfunctions import parse_datetime
from .pyfunctions import setup_logger
from .pyfunctions import sys_record_flds


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


def hanly(parsed_chunk, checksum, cdiag, dbopt, ps, usr, ll_level, sys_tables, chunk_index, special_k, strt=65, endp=90):

    results = []
    sys_records = []

    fmt = "%Y-%m-%d %H:%M:%S"

    dbit = False

    if not ps:
        sys_tables = ()

    with sqlite3.connect(dbopt) as conn:
        cur = conn.cursor()

        setup_logger(ll_level, process_label="hanlymc")

        r = 0
        incr = 10
        current_step = 0
        delta_v = endp - strt
        steps = []
        if chunk_index == special_k:  # progress
            dbit = True
            total_e = len(parsed_chunk)
            steps = [int((i / 10) * total_e) for i in range(1, 11)]

        for record in parsed_chunk:
            if dbit:
                r += 1
                if current_step < len(steps) and r >= steps[current_step]:
                    prog_i = (current_step + 1) * incr
                    prog_v = round((delta_v * (prog_i / 100))) + strt
                    print(f"Progress: {prog_v}%", flush=True)
                    current_step += 1  # end progress

            current_size = None
            original_size = None
            is_sys = False

            if len(record) < 15:
                logging.debug("record length issue: %s", record)
                continue

            entry = {"cerr": [], "flag": [], "scr": [], "sys": [], "dcp": []}

            filename = record[1]
            label = filename

            recent_entries = get_recent_changes(label, cur, 'logs', ['casmod'])
            recent_sys = get_recent_sys(label, cur, sys_tables, ['casmod', 'count']) if ps else None

            if not recent_entries and not recent_sys:
                entry["dcp"].append(record)   # is copy?
                continue

            recent_timestamp = record[0]
            previous = recent_entries

            if ps and recent_sys and len(recent_sys) >= 12:

                recent_systime = parse_datetime(recent_sys[0], fmt)

                if recent_systime:
                    is_sys = True
                    entry["sys"].append("")
                    prev_count = recent_sys[-1]
                    sys_record_flds(record, sys_records, prev_count)
                    previous = recent_sys

            if previous is None or len(previous) < 11:
                logging.debug("previous record has unexpected size or is None %s", previous)
                continue

            if checksum:

                if not record[5]:
                    logging.debug("No checksum for file %s", record)
                    continue

                if is_integer(record[6]):
                    current_size = record[6]
                else:
                    logging.debug("invalid format detected size not an integer: %s", record)

                if is_integer(previous[6]):
                    original_size = previous[6]
                else:
                    logging.debug("invalid format detected size not an integer: %s", previous)

            previous_timestamp = parse_datetime(previous[0], fmt)

            if (is_integer(record[3]) and is_integer(previous[3])  # format check
                    and recent_timestamp and previous_timestamp):

                recent_mod_time = record[14]
                if "." in recent_mod_time and int(recent_mod_time.split(".")[1]) == 0:
                    entry["scr"].append(f'Unusual modified time file has microsecond all zero: {label} timestamp: {recent_mod_time}')

                if recent_timestamp == previous_timestamp:
                    file_path = Path(filename)

                    if checksum:

                        st = goahead(file_path)
                        if st == "Nosuchfile":
                            entry["flag"].append(f'Deleted {record[0]} {record[2]} {label}')

                        elif st:

                            a_mod = st.st_mtime
                            a_size = st.st_size
                            afrm_dt = datetime.fromtimestamp(a_mod).replace(microsecond=0)
                            if afrm_dt and is_valid_datetime(record[4], fmt):  # format check

                                if afrm_dt == previous_timestamp:

                                    if previous[5] and record[5] != previous[5]:
                                        entry["flag"].append(f'Suspect {record[0]} {record[2]} {label}')
                                        entry["cerr"].append(f'Suspect file: {label} changed without a new modified time.')
                                        print(previous[5])
                                        print(record[5])

                                    if record[3] == previous[3]:  # inode

                                        metadata = (previous[7], previous[8], previous[9])
                                        if new_meta(record, metadata):
                                            entry["flag"].append(f'Metadata {record[0]} {record[2]} {label}')
                                            entry["scr"].append(f'Permissions of file: {label} changed {record[8]} {record[9]} {record[10]} → {metadata[0]} {metadata[1]} {metadata[2]}')
                                    else:
                                        entry["flag"].append(f'Copy {record[0]} {record[2]} {label}')

                                else:
                                    casmod = previous[10]
                                    if casmod != 'y':
                                        # shift during search?
                                        if cdiag:
                                            entry["scr"].append(f'File changed during the search. {label} at {afrm_dt}. Size was {original_size}, now {a_size}')
                                        else:
                                            entry["scr"].append(f'File changed during search. {label} File likely changed. system cache item.')

                        else:
                            logging.debug("Skipping %s couldnt stat in ha current record %s \n previous record %s", file_path, record, previous)

                else:

                    if checksum:

                        if record[3] != previous[3]:  # inode

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
                                metadata = (previous[7], previous[8], previous[9])
                                if new_meta(record, metadata):

                                    entry["flag"].append(f'Metadata {record[0]} {record[2]} {label}')
                                    entry["scr"].append(f'Permissions of file: {label} changed {record[8]} {record[9]} {record[10]} → {metadata[0]} {metadata[1]} {metadata[2]}')
                                else:
                                    entry["flag"].append(f'Touched {record[0]} {record[2]} {label}')

                    else:
                        if record[3] != previous[3]:
                            entry["flag"].append(f'Replaced {record[0]} {record[2]} {label}')
                        else:
                            entry["flag"].append(f'Modified {record[0]} {record[2]} {label}')

                    two_days_ago = datetime.now() - timedelta(days=5)
                    if previous_timestamp < two_days_ago:
                        message = f'File that isnt regularly updated {label}.'
                        if is_sys:
                            entry["scr"].append(f'{message} and is a system file.')
                        else:
                            screen = get_delete_patterns(usr)
                            if not matches_any_pattern(label, screen):
                                entry["scr"].append(message)

                if entry["cerr"] or entry["flag"] or entry["scr"] or entry["sys"]:
                    results.append(entry)

            else:
                logging.debug("hanlymc timestamp missing or invalid inode format from database for file %s", filename)
                logging.debug("current inode %s previous %s, current timestamp %s previous %s", record[3], previous[3], recent_timestamp, previous_timestamp)
                logging.debug("original %s \n current %s", previous, record)

        if dbit and current_step <= len(steps) - 1:
            prog_v = round(delta_v) + strt
            print(f"Progress: {prog_v}%", flush=True)

    return results, sys_records
