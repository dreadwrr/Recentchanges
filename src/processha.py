# manipulate array before database srg and send output to diff file 01/03/2026 03/02/2026
import logging
import os
from datetime import datetime
from .pyfunctions import parse_datetime
from .rntchangesfunctions import filter_lines_from_list
from .rntchangesfunctions import filter_output


# return a datetime object for sorting
def parse_rout(line):
    tsmp = line[1]
    key_value = parse_datetime(tsmp)
    if key_value:
        return key_value
    logging.debug("Invalid sort key in processha , line: %s", line)
    print("Invalid sort key in processha", line)
    return datetime.min


# preprocess diff file
def isdiff(RECENT, ABSENT, rout, diffnm, difff_file, flsrh, parsed_PRD, fmt):

    ranged = []

    if not flsrh:

        for line in difff_file:
            parts = line.strip().split(None, 2)
            if not parts:
                continue
            tsmp = f'{parts[0]} {parts[1]}'
            timestp = parse_datetime(tsmp, fmt)
            if timestp is None:
                continue

            if timestp >= parsed_PRD:
                ranged.append(line)
    else:
        ranged = difff_file[:]

    if ranged:
        d_paths = set(entry[1] for entry in RECENT)

        with open(diffnm, 'a') as file2:
            for line in ranged:
                parts = line.strip().split(None, 2)
                if len(parts) < 3:
                    continue
                timestamp_str = parts[0] + " " + parts[1]
                timestamp = parse_datetime(timestamp_str, fmt)
                if timestamp is None:
                    continue

                filepath = parts[2]

                if filepath in d_paths:
                    ABSENT.append(f"Modified {line}")

                else:
                    ABSENT.append(f"Deleted {line}")
                    rout.append(f"Deleted {timestamp} {line}")

            if ABSENT:

                file2.write('\nApplicable to your search\n')
                # file2.write('\n'.join(ABSENT) + '\n')

                for line in ABSENT:
                    if line.startswith("Deleted"):
                        line = line.replace("Deleted", "Deleted ", 1)
                    file2.write(line + '\n')
    else:
        with open(diffnm, 'a') as file2:
            print('None of above is applicable to search. It is the previous search', file=file2)


# post ha to diff file
def processha(rout, ABSENT, diffnm, cerr, flsrh, argf, parsed_PRD, escaped_user, supbrwLIST, suppress_browser, suppress):

    def get_last_part(line):
        parts = line.strip().split(None, 3)
        return parts[-1] if parts else None

    cleaned_rout = []
    outline = []

    if rout:

        for line in rout:

            parts = line.strip().split(maxsplit=5)
            if len(parts) < 6:
                continue
            if parts[0] in ("Deleted", "Nosuchfile"):
                continue
            action = parts[0]
            ts1 = f'{parts[1]} {parts[2]}'
            # ts2 = f'{parts[3]} {parts[4]}'
            fpath = parts[5]
            cleaned_rout.append((action, ts1, fpath))

        absent_paths = {get_last_part(line) for line in ABSENT if line.strip()}

        DIFFMATCHED = [
            line for line in cleaned_rout
            if line[2] not in absent_paths
        ]

        if flsrh or argf == "filtered":
            if not (flsrh and argf == "filtered"):
                DIFFMATCHED = filter_lines_from_list(DIFFMATCHED, escaped_user, 2)

        if flsrh:
            DIFFMATCHED = [
                line for line in DIFFMATCHED
                if (ts := parse_rout(line)) and ts >= parsed_PRD
            ]

        DIFFMATCHED.sort(key=parse_rout)

        for line in DIFFMATCHED:

            status = line[0]
            timestamp = line[1]
            fpath = line[2]
            extra_space = " " if status != "Overwrite" else ""
            if status == "Copy":
                extra_space = extra_space + "\t"
            formatted_line = f"{status}{extra_space}\t{timestamp} {fpath}\n"
            outline.append(formatted_line)

    if outline:
        with open(diffnm, 'a') as f:
            f.write('\nHybrid analysis\n\n')
            f.writelines(outline)

    if os.path.exists(cerr):
        filter_output(cerr, escaped_user, 'Warning', 'Suspect', 'yellow', 'red', 'elevated', supbrwLIST, suppress_browser, suppress)
