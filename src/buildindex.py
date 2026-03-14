
# Build index - Scan a drive for specified files and hash/get meta data.
# formerly scan_f # 03/03/2026
import os
from .logs import emit_log
from . import logs
from .dirwalkerfunctions import scandir_meta


def build_index(chunk, i, num_chunks, show_progress=False, strt=0, endp=100):

    # rec_count = 0
    c = x = 0
    dbit = False

    if show_progress:
        dbit = True
        rec_count = len(chunk)
        #     # scale = (endp - strt) / rec_count
        steps = sorted(set(int((r / 10) * rec_count) for r in range(1, 11)))
        step_len = len(steps)

    sys_data, log_entries = [], []

    current_step = 0

    # incr = 10
    # delta_v = endp - strt
    # last_printed = -1

    for record in chunk:
        c += 1
        x += 1
        if len(record) < 5:
            continue
        file_path = record[0]

        try:

            if dbit:
                if current_step < step_len and c >= steps[current_step]:

                    emit_log("prog", x, logs.WORKER_LOG_Q)
                    x = 0
                    current_step += 1
                    # prog_i = (current_step + 1) * incr  # single core orig
                    # prog_v = round((delta_v * (prog_i / 100))) + strt
                    #  print(f"Progress: {prog_v}%", flush=True)

                # p_g = strt + (c * scale)  # original
                # if p_g > last_printed:
                #     print(f"Progress: {p_g:.2f}%", flush=True)
                #     last_printed = p_g
                #     if p_g >= endp:
                #         dbit = False

            if os.path.isfile(file_path):
                st = record[1]
                sym = record[2]
                target = record[3]
                found = record[4]

                rlt, status = scandir_meta(file_path, st, sym, target, found, sys_data, logs.WORKER_LOG_Q)

                if not rlt:
                    if rlt is False and status == "Nosuchfile":
                        emit_log("DEBUG", f"scandir_meta File not found: {file_path}: ", logs.WORKER_LOG_Q)
                    elif rlt is None:
                        emit_log("DEBUG", f"status: {status}, Hash skipped {file_path} . record: {record}",  logs.WORKER_LOG_Q)
            else:
                emit_log("DEBUG", f"file not found during indexing, skipping: {file_path}",  logs.WORKER_LOG_Q)

        except Exception as e:
            emit_log("ERROR", f"build_index Encountered an error chunk {i}\\{num_chunks} processing record {c} of {len(chunk)}, file {file_path}, line: {record}: {type(e).__name__} {e}", logs.WORKER_LOG_Q)
            raise
    if dbit and current_step <= len(steps) - 1:
        emit_log("prog", x, logs.WORKER_LOG_Q)
    return sys_data, log_entries, c
