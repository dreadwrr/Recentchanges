# Find downloads                                                            11/27/2025
#
# Using the directory cache use the mtime of the dir to find new files. At the end
# the cache file is up to date with any new dir mtimes.
# Note: reparse points were first added during indexing. Any future reparse points we
# dont care about as if there is a problem can just reindex. Windows only has certain junctions ect.
#
# Adding to much info or trying to maintain a cache ie removing deleted files can result in desync.
#
# # os.scandir recursion
import logging
import os
import traceback
from .dirwalkerfnts import get_dir_mtime
from .fsearchfnts import get_reparse_type
from .pyfunctions import setup_logger


def scan_created(chunk, basedir, CACHE_S, appdata_local, ll_level, i):

    setup_logger(ll_level, "scan_created", appdata_local)

    f_r = "process_directory"

    sys_data = []
    results = []

    cckSEEN = set()

    def process_directory(root, results, sys_data):

        x = 0

        entry = {"dirl": {}, "cfr_reparse": {}, "cfr_data": {}}

        if root in cckSEEN:
            return
        cckSEEN.add(root)  # recursion safety
        prev_entry = CACHE_S.get(root)  # skip known reparse
        if prev_entry and prev_entry.get("type"):
            return

        filename = None
        previous_mtime = None
        dirl = False
        scanf = True
        rtype = None

        modified_dt, modified_ep = get_dir_mtime(root, "scan")
        if not modified_ep:
            logging.debug("Skipped. Unable to access directory: %s no modified_ep mtime from %s scan created", root, f_r)
            return

        if prev_entry:
            entry["dirl"][root] = "entry"
            previous_mtime = prev_entry['modified_ep']

            if not previous_mtime or modified_ep > previous_mtime:
                dirl = True
            elif modified_ep <= previous_mtime:
                scanf = False
        else:
            dirl = True

            rtype = get_reparse_type(root)  # *ignore new reparse
            if rtype:
                # entry["cfr_reparse"][root] = {
                #     'modified_ep' : modified_ep,
                #     'file_count': "0",
                #     'max_depth': root.count(os.sep),
                #     'type': rtype,
                #     'target': os.path.realpath(root)
                # }
                # results.append(entry)
                # logging.debug(f"{f_f} folder was a reparse point: {root}")
                return
        try:

            with os.scandir(root) as entries:
                for record in entries:
                    if record.is_dir(follow_symlinks=False):
                        if root != basedir:
                            process_directory(record.path, results, sys_data)
                    if not scanf:
                        continue

                    if record.is_file():
                        filename = record.path

                        x += 1
                        file_mtime = record.stat().st_mtime
                        if previous_mtime is None or file_mtime > previous_mtime:
                            sys_data.append((filename, file_mtime))  # new file found

                if dirl:
                    if prev_entry:
                        entry_data = prev_entry.copy()
                    else:
                        entry_data = {
                            'idx_count': '',
                            'max_depth': root.count(os.sep),
                            'type': '',
                            'target': ''
                        }

                    entry_data.update({
                        'modified_time': modified_dt if modified_dt else '',
                        'modified_ep': modified_ep,
                        'file_count': str(x)
                    })
                    entry["cfr_data"][root] = entry_data
                if entry["cfr_reparse"] or entry["dirl"] or entry["cfr_data"]:
                    results.append(entry)

        except (ValueError, TypeError) as e:
            emsg = f"file loop error detected {f_r} : dir: {root}, file: {filename} {type(e).__name__} {e} \n{traceback.format_exc()}"
            print(emsg)
            logging.error(emsg, exc_info=True)
            return
        except OSError as e:
            logging.debug(f"file loop error detected {f_r} : dir: {root}, file: {filename} {type(e).__name__} {e} \n", exc_info=True)
            return

    f = 0

    for root in chunk:
        f += 1
        process_directory(root, results, sys_data)

    return sys_data, results
