# Find downloads                                                           03/08/2026
#
# Using the directory cache use the mtime of the dir to find new files. At the end
# the cache file is up to date with any new dir mtimes.
# Note: reparse points were first added during indexing. Any future reparse points we
# dont care about as if there is a problem can just reindex. Windows only has certain junctions ect.
#
# Adding to much info or trying to maintain a cache ie removing deleted files can result in desync.
#
# # os.scandir recursion
import os
import traceback
from pathlib import Path
from .dirwalkerfunctions import get_stat
from .fileops import find_link_target
from .fsearchfunctions import is_reparse_point
from .logs import write_logs_to_logger
from .pyfunctions import epoch_to_str


def scan_created(chunk, basedir, EXCLDIRS_FULLPATH, filter_tup, CACHE_S, root_count, i, num_chunks, show_progress=False, logger=None, strt=0, endp=0):

    sys_data = []
    results = []
    log_entries = []
    cckSEEN = set()

    def process_directory(root, root_path):

        x = 0

        try:
            entry = {"dirl": {}, "cfr_reparse": {}, "cfr_data": {}}

            if root_path in cckSEEN:
                return

            cckSEEN.add(root_path)  # recursion safety
            prev_entry = CACHE_S.get(root_path)  # skip known reparse
            if prev_entry and prev_entry.get("type"):
                return

            rtype = target = None
            symlink = False
            junction = False

            previous_mtime = None
            dirl = False
            scanf = True

            stat_info = get_stat(root, log_entries=log_entries, logger=logger)
            if not stat_info:
                return

            modified_ep = stat_info.st_mtime
            modified_dt = epoch_to_str(modified_ep)

            if prev_entry:
                entry["dirl"][root_path] = "entry"
                previous_mtime = prev_entry['modified_ep']

                if not previous_mtime or modified_ep > previous_mtime:
                    dirl = True
                elif modified_ep <= previous_mtime:
                    scanf = False
            else:
                dirl = True

                symlink = root.is_symlink()
                if symlink:
                    rtype = "symlink"
                if not symlink and root.is_junction():
                    junction = True
                    rtype = "junction"
                elif not (symlink or junction):
                    if is_reparse_point(stat_info):
                        rtype = "reparse"
                # new reparse
                if rtype:
                    target = find_link_target(root_path, log_entries=log_entries, logger=logger)
                    entry["cfr_reparse"][root_path] = {
                        'modified_time': modified_dt if modified_dt else '',
                        'modified_ep': modified_ep,
                        'file_count': 0,
                        'idx_count': 0,
                        'max_depth': root_path.count(os.sep),
                        'type': rtype,
                        'target': target
                    }
                    results.append(entry)
                    msg = f"process_directory folder was a reparse point: {root_path}"
                    if logger:
                        logger.debug(msg)
                    else:
                        log_entries.append(("DEBUG", msg))
                    return

            with os.scandir(root) as entries:
                for record in entries:

                    full_path = record.path

                    try:

                        if record.is_dir():
                            if full_path in EXCLDIRS_FULLPATH:
                                continue

                            if full_path != basedir:
                                process_directory(record, full_path)
                        if not scanf:
                            continue

                        if record.is_file():
                            x += 1

                            stat_info = get_stat(record, log_entries=log_entries, logger=logger)
                            if not stat_info:
                                continue

                            file_mtime = stat_info.st_mtime

                            if previous_mtime is None or file_mtime > previous_mtime:
                                if not full_path.lower().startswith(filter_tup):
                                    sys_data.append((full_path, file_mtime))  # new file found

                    except OSError as e:
                        emsg = f"error could not stat file: {full_path} {type(e).__name__} {e}"
                        if logger:
                            logger.debug(emsg)
                        else:
                            log_entries.append(("DEBUG", emsg))
                        continue
                if dirl:
                    if prev_entry:
                        entry_data = prev_entry.copy()
                    else:
                        entry_data = {
                            'idx_count': 0,
                            'idx_bytes': 0,
                            'max_depth': root_path.count(os.sep),
                            'type': '',
                            'target': ''
                        }

                    entry_data.update({
                        'modified_time': modified_dt if modified_dt else '',
                        'modified_ep': modified_ep,
                        'file_count': x
                    })
                    entry["cfr_data"][root_path] = entry_data
                if entry["dirl"] or entry["cfr_data"]:
                    results.append(entry)

        except PermissionError as e:
            em = f"process_directory: {root_path} error: {e}"
            if logger:
                logger.debug(em)
            else:
                log_entries.append(("DEBUG", em))
        except OSError as e:
            em = f"chunk {i} of {num_chunks} file loop error detected process_directory : dir: {root_path} {type(e).__name__} {e} \n{traceback.format_exc()}"
            if logger:
                logger.error(em)
            else:
                log_entries.append(("ERROR", em))

    f = 0
    scale = (endp - strt) / root_count
    current_step = 0
    steps = sorted(set(int((r / 10) * root_count) for r in range(1, 11)))
    step_len = len(steps)

    try:
        for root in chunk:

            f += 1
            process_directory(Path(root), root)

            if show_progress:
                if current_step < step_len and f >= steps[current_step]:
                    prog_v = strt + (f * scale)
                    print(f"Progress: {prog_v:.2f}%", flush=True)
                    current_step += 1
        if show_progress and current_step <= len(steps) - 1:
            print(f"Progress: {endp:.2f}%", flush=True)
    except Exception as e:
        em = f"file loop error {i}\\{num_chunks}, detected scan_created line {f} of {root_count} : dir: {root} {type(e).__name__} {e}"
        if logger:
            logger.error(em)
        else:
            log_entries.append(("ERROR", em))

        write_logs_to_logger(log_entries, logger)
        raise

    return sys_data, results, log_entries, f
