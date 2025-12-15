import gc
import logging
import traceback
import os
import sqlite3
from concurrent.futures import ProcessPoolExecutor, as_completed
from random import randint
from .hanlymc import hanly
from .pyfunctions import detect_copy
from .pyfunctions import increment_f

# append rout messages to the rout list. also append copies to the rout list.
# if there are system files labelled as sys add them to the database sys changes sys_b
# then increase the count in sys_a for the original record.
# distribute the appropriate messages to cerr and scr.
# # tfile


def logger_process(results, sys_records, sys_tables, rout, scr=None, cerr=None, dbopt="/usr/local/save-changesnew/recent.db", ps=False, logger=None):

    crecord = False

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
                        if isinstance(fpath, list):  # rout was a file in early design but now is a list. appended to it
                            fpath.extend(messages)
                        else:
                            file_messages.setdefault(fpath, []).extend(messages)  # write these to cerr scr

            if "dcp" in entry:
                dcp_messages = entry["dcp"]
                if not isinstance(dcp_messages, list):
                    dcp_messages = [dcp_messages]

                if dcp_messages:

                    for msg in dcp_messages:
                        try:
                            if len(msg) > 5:
                                timestamp = msg[0]
                                label = msg[1]
                                ct = msg[2]
                                inode = msg[3]
                                checksum = msg[5]
                                result = detect_copy(label, inode, checksum, sys_tables, c, ps)
                                if result:
                                    rout.append(f'Copy {timestamp} {ct} {label}')
                            else:
                                if logger:
                                    logger.debug("Skipping dcp message due to insufficient length: %s", msg)

                        except Exception as e:
                            if logger:
                                logger.debug("Error updating DB for sys entry '%s': %s : %s", msg, e, type(e).__name__, exc_info=True)

            if "sys" in entry:  # a new system file entry
                crecord = True

        if crecord:
            try:
                increment_f(conn, c, sys_tables, sys_records)  # add it to sys_b and update count in sys_a
            except Exception as e:
                if logger:
                    logger.debug("Failed to update sys table in hanlyparallel as: %s : %s", e, type(e).__name__, exc_info=True)

    for fpath, messages in file_messages.items():
        if messages:
            try:
                with open(fpath, "a", encoding="utf-8") as f:
                    f.write('\n'.join(str(msg) for msg in messages) + '\n')

            except IOError as e:
                if logger:
                    logger.debug("Error writing to %s cer or scr file: as %s : %s", fpath, e, type(e).__name__, exc_info=True)
            except Exception as e:
                if logger:
                    logger.debug("Unexpected error logger_process: %s : %s", e, type(e).__name__, exc_info=True)


def hanly_parallel(model_type, rout, scr, cerr, parsed, checksum, cdiag, dbopt, ps, user, ll_level, sys_tables, iqt=False, strt=65, endp=90):

    all_results = []
    batch_incr = []

    special_k = -1

    if not parsed or len(parsed) == 0:
        return

    logger = logging.getLogger("hanly parallel HANLYLogger")

    if len(parsed) < 40 or model_type.lower() == "hdd":
        if iqt:
            special_k = 0
        all_results, batch_incr = hanly(parsed, checksum, cdiag, dbopt, ps, user, ll_level, sys_tables, 0, special_k, strt, endp)
    else:
        max_workers = min(8, os.cpu_count() or 1, len(parsed))
        chunk_size = max(1, (len(parsed) + max_workers - 1) // max_workers)
        chunks = [parsed[i:i + chunk_size] for i in range(0, len(parsed), chunk_size)]

        if iqt:
            special_k = randint(0, len(chunks) - 1)

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    hanly, chunk, checksum, cdiag, dbopt, ps, user, ll_level, sys_tables, i, special_k, strt, endp
                )
                for i, chunk in enumerate(chunks)
            ]

            for future in as_completed(futures):
                try:
                    results, sys_records = future.result()
                    if results:
                        all_results.extend(results)
                    if sys_records:
                        batch_incr.extend(sys_records)

                except Exception as e:
                    em = f"Worker error from hanly multiprocessing: {type(e).__name__} {e}"
                    print(f"{em} \n {traceback.format_exc()}")
                    logger.error(em, exc_info=True)

    # for future in futures:           original
    # 	try:
    # 		all_results.extend(future.result())
    # 	except Exception as e:
    #
    # 		logging.error("Worker error: %s\n%s", exc_info=True)

    logger_process(all_results, batch_incr, sys_tables, rout, scr, cerr, dbopt, ps, logger)
    gc.collect()
