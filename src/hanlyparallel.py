import gc
import logging
import multiprocessing as mp
import os
import sqlite3
import threading
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from .hanlymc import hanly
from .logs import emit_log
from .logs import init_process_worker
from .logs import logs_to_queue
from .logs import logging_worker
from .pyfunctions import cprint
from .pysql import detect_copy
from .pysql import increment_f
# 03/30/2026


# tfile
#  logger_process(all_results, batch_incr, rout, scr, cerr, dbopt, ps, logger)
def logger_process(results, sys_records, sys_tables, rout, scr, cerr, dbopt, ps, logger=None):
    # append rout messages to the rout list from hanly
    # if there are sys_records add them to the database sys changes sys_b
    #
    # distribute the appropriate messages to cerr and scr.

    log = logger if logger else logging
    key_to_files = {
        "flag": [rout],
        "cerr": [cerr],
        "scr": [scr],
    }
    with sqlite3.connect(dbopt) as conn:
        c = conn.cursor()

        file_messages = {}

        for entry in results:

            for key, files in key_to_files.items():
                if key in entry:

                    messages = entry[key]
                    if not isinstance(messages, list):
                        messages = [messages]
                    for fpath in files:
                        if isinstance(fpath, list):  # rout is a list
                            fpath.extend(messages)
                        else:
                            file_messages.setdefault(fpath, []).extend(messages)  # write these to cerr scr

            if "dcp" in entry:
                dcp_messages = entry["dcp"]
                if not isinstance(dcp_messages, list):
                    dcp_messages = [dcp_messages]

                if dcp_messages:

                    try:
                        for msg in dcp_messages:

                            if msg is not None and len(msg) > 6:
                                filesize = msg[6]
                                if filesize:
                                    timestamp = msg[0]
                                    filepath = msg[1]
                                    ct = msg[2]
                                    inode = msg[3]
                                    checksum = msg[5]
                                    result = detect_copy(filepath, inode, checksum, sys_tables, c, ps)
                                    if result:
                                        # label = escf_py(filepath)
                                        rout.append(f'Copy {timestamp} {ct} {filepath}')
                            else:
                                log.debug("Skipping dcp message due to insufficient length: %s", msg)

                    except Exception as e:
                        em = "Error checking for copies"
                        print(f"{em} {e} {type(e).__name__}")
                        log.error(em, exc_info=True)

        # update sys changes in one batch
        if sys_records:
            try:
                increment_f(conn, c, sys_tables, sys_records, logger=log)  # add changes to sys_b
            except Exception as e:
                em = "Failed to update sys table in hanlyparallel increment_f"  # {traceback.format_exc()}"
                print(f"{em} : {e} {type(e).__name__}")
                log.error(em, exc_info=True)

        for fpath, messages in file_messages.items():
            if messages:
                try:
                    with open(fpath, "w", encoding="utf-8") as f:
                        f.write('\n'.join(str(msg) for msg in messages) + '\n')

                except IOError as e:
                    em = f"Error logging to {fpath}"
                    print(f"{em}: {e}")
                    log.error(em, exc_info=True)
                except Exception as e:
                    em = f"Unexpected error to {fpath} logger_process"
                    print(f"{em}: {e} : {type(e).__name__}")
                    log.error(em, exc_info=True)


def hanly_parallel(drive_type, rout, scr, cerr, parsed, cachermPATTERNS, ANALYTICSECT, checksum, cdiag, dbopt, ps, user, logging_values, sys_tables, iqt=False, strt=65, endp=90):

    all_results = []
    batch_incr = []
    if not parsed:
        return False
    len_parsed = len(parsed)
    if len_parsed == 0:
        return False

    csum = False

    if ANALYTICSECT:
        cprint.green('Hybrid analysis on')

    logger = logging.getLogger("HANLY")

    show_progress = False
    if iqt:
        show_progress = True

    if len_parsed < 80 or drive_type.lower() == "hdd":

        # avoid starting the queue and use logging directly to avoid overhead

        # log_q = queue.SimpleQueue()
        # init_process_worker(log_q)

        # try:

        # tlog = threading.Thread(target=logging_worker, args=(log_q, len_parsed, strt, endp, show_progress, logger), daemon=True)
        # tlog.start()
        init_process_worker(None)
        all_results, batch_incr, _, csum = hanly(parsed, checksum, cdiag, dbopt, ps, user, logging_values, sys_tables, cachermPATTERNS, show_progress, logger, strt, endp)
        # if log_entries:
        #     logs_to_queue(log_entries, log_q)

        # finally:
        #     log_q.put(None)
        #     tlog.join()

    else:

        max_workers = min(8, os.cpu_count() or 1, len_parsed)
        chunk_size = max(1, (len_parsed + max_workers - 1) // max_workers)
        chunks = [parsed[i:i + chunk_size] for i in range(0, len_parsed, chunk_size)]

        ctx = mp.get_context()
        log_q = ctx.Queue(maxsize=4096)
        log_t = threading.Thread(target=logging_worker, args=(log_q, len_parsed, strt, endp, show_progress, logger), daemon=True)
        log_t.start()
        try:

            with ProcessPoolExecutor(
                max_workers=max_workers,
                mp_context=ctx,
                initializer=init_process_worker,
                initargs=(log_q,)
            ) as executor:
                futures = [
                    executor.submit(
                        hanly, chunk, checksum, cdiag, dbopt, ps, user, logging_values, sys_tables, cachermPATTERNS, show_progress, None, strt, endp
                    )
                    for chunk in chunks
                ]

                for future in as_completed(futures):
                    try:
                        results, sys_records, log_entries, is_csum = future.result()
                        if results:
                            all_results.extend(results)
                        if sys_records:
                            batch_incr.extend(sys_records)
                        if log_entries:
                            logs_to_queue(log_entries, log_q)
                        if is_csum:
                            csum = True

                        # done += 1

                        # if show_progress:
                        #     prog = strt + ((endp - strt) * (done / num_chunks))
                        #     print(f"Progress: {prog:.2f}%", flush=True)

                    except BrokenProcessPool as e:
                        print("hanly encountered a multiprocessing error")
                        emit_log("ERROR", f"unable to run hanly an error occured {e} \n{traceback.format_exc()}", log_q)
                        break
                    except Exception as e:
                        em = f"Worker error from hanly multiprocessing: {type(e).__name__} {e}"
                        print(em)
                        emit_log("ERROR", f"{em} \n {traceback.format_exc()}", log_q)
                        break
        finally:
            log_q.put(None)
            log_t.join()
            log_q.close()
            log_q.join_thread()

    print("processing results", flush=True)
    logger = logging.getLogger("HANLYLOGGER")
    logger_process(all_results, batch_incr, sys_tables, rout, scr, cerr, dbopt, ps, logger)
    gc.collect()
    return csum
