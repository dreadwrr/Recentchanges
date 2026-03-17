import os
from . import logs
from .logs import emit_log
from .dirwalkerfunctions import meta_sys


def scan_index(chunk, is_sym, i, num_chunks, show_progress=False, strt=0, endp=100):
    # the checksum could change on live system checksum is verified with retries
    c = r = 0
    # t_fold = 0
    dbit = False

    if show_progress:
        dbit = True
        t_fold = len(chunk)
        # scale = (endp - strt) / t_fold
        steps = sorted(set(int((i / 10) * t_fold) for i in range(1, 11)))
        step_len = len(steps)

    sys_data, link_data, nsf_records, log_entries = [], [], [], []

    x, y = 0, 0

    current_step = 0
    # incr = 10
    # delta_v = endp - strt
    # last_printed = -1

    for record in chunk:
        c += 1
        r += 1
        if len(record) < 16:
            emit_log("DEBUG", f"scan_index  record length less than required 16. skipping {record}", logs.WORKER_LOG_Q)
            continue
        file_path = record[1]
        try:
            if dbit:

                if current_step < step_len and c >= steps[current_step]:
                    emit_log("prog", r, logs.WORKER_LOG_Q)
                    r = 0
                    current_step += 1
                    # prog_i = (current_step + 1) * incr  # single core orig
                    # prog_v = round((delta_v * (prog_i / 100))) + strt
                    # print(f"Progress: {prog_v}%", flush=True)

                # p_g = strt + (c * scale)  # original
                # if p_g > last_printed:
                #     print(f"Progress: {p_g:.2f}%", flush=True)
                #     last_printed = p_g
                #     if p_g >= endp:
                #         dbit = False

            if os.path.isfile(file_path):
                x += 1
                previous_md5 = record[5]
                previous_symlink = record[7]
                previous_target = record[12]
                previous_count = record[15]
                if not previous_md5:
                    emit_log("DEBUG", f"Previous hash was missing attempting to run checksum. file:  {file_path}", logs.WORKER_LOG_Q)
                rlt, status = meta_sys(file_path, previous_md5, previous_symlink, previous_target, previous_count, is_sym, sys_data, link_data, logs.WORKER_LOG_Q)  # append meta data to sys_data
                if not rlt:
                    if rlt is False and status == "Nosuchfile":
                        x -= 1
                        y += 1
                        nsf_records.append(record)
                    elif rlt is None:  # Permission error or Error
                        if not os.path.isfile(file_path):
                            x -= 1
                            y += 1
                            nsf_records.append(record)
                    # emit_log("DEBUG", f"status: {status}, Hash skipped {file_path} . record: {record}", logs.WORKER_LOG_Q)
            else:
                y += 1
                nsf_records.append(record)
        except Exception as e:
            em = (
                f"scan_index Encountered an error chunk {i}\\{num_chunks} processing record {c} of {len(chunk)}, "
                + f"file {file_path}, line: {record}: {type(e).__name__} {e}"
            )
            emit_log("ERROR", em, logs.WORKER_LOG_Q)
            raise

    if dbit and current_step <= len(steps) - 1:
        emit_log("prog", r, logs.WORKER_LOG_Q)
    return sys_data, link_data, nsf_records, log_entries, x, y, c
