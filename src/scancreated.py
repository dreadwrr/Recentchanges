# Find downloads                                                           03/16/2026
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
from .fileops import is_reparse_point
from .logs import emit_log
from .logs import write_logs_to_logger
from .pyfunctions import epoch_to_str


def scan_created(chunk, basedir, EXCLDIRS_FULLPATH, filter_tup, CACHE_S, root_count, i, num_chunks, show_progress=False, logger=None, strt=0, endp=0):

    sys_data = []
    results = []
    log_entries = []
    cckSEEN = set()
    if logger:
        log_entries = None

    def process_directory(record, root):

        x = 0

        try:
            entry = {"dirl": {}, "cfr_reparse": {}, "cfr_data": {}}

            if root in cckSEEN:
                return

            cckSEEN.add(root)  # recursion safety
            prev_entry = CACHE_S.get(root)  # skip known reparse
            if prev_entry and prev_entry.get("type"):
                return

            previous_mtime = None
            dirl = False
            scanf = True

            stat_info = get_stat(record, log_entries=log_entries, logger=logger)
            if not stat_info:
                return

            modified_ep = stat_info.st_mtime
            modified_dt = epoch_to_str(modified_ep)

            if prev_entry:
                entry["dirl"][root] = "entry"
                previous_mtime = prev_entry['modified_ep']

                if not previous_mtime or modified_ep > previous_mtime:
                    dirl = True
                elif modified_ep <= previous_mtime:
                    scanf = False
            else:
                dirl = True

            with os.scandir(root) as entries:
                for record in entries:

                    rtype = target = None
                    junction = False
                    symlink = False

                    path = record.path

                    try:

                        if record.is_dir(follow_symlinks=False):
                            if path in EXCLDIRS_FULLPATH:
                                continue

                        if dirl:
                            stat_info = get_stat(record, log_entries=log_entries, logger=logger)
                            if not stat_info:
                                continue
                            symlink = record.is_symlink()
                            if symlink:
                                rtype = "symlink"
                            if not symlink and record.is_junction():
                                junction = True
                                rtype = "junction"
                            if not (symlink or junction):
                                if is_reparse_point(stat_info):
                                    rtype = "reparse"
                            # new reparse
                            if rtype:
                                m_epoch = stat_info.st_mtime
                                m_time = epoch_to_str(m_epoch)
                                target = find_link_target(path, log_entries=log_entries, logger=logger)
                                entry["cfr_reparse"][path] = {
                                    'modified_time': m_time if m_time else '',
                                    'modified_ep': m_epoch,
                                    'file_count': 0,
                                    'idx_count': 0,
                                    'idx_bytes': 0,
                                    'max_depth': path.count(os.sep),
                                    'type': rtype,
                                    'target': target
                                }
                                emit_log("DEBUG", f"process_directory folder was a reparse point: {path}", log_entries=log_entries, logger=logger)
                                continue

                            if root != basedir:
                                process_directory(record, path)
                        if not scanf:
                            continue

                        if record.is_file():
                            x += 1

                            stat_info = get_stat(record, log_entries=log_entries, logger=logger)
                            if not stat_info:
                                continue

                            file_mtime = stat_info.st_mtime

                            if previous_mtime is None or file_mtime > previous_mtime:
                                if not path.lower().startswith(filter_tup):
                                    sys_data.append((path, file_mtime))  # new file found

                    except OSError as e:
                        emit_log("DEBUG", f"error could not stat file: {path} {type(e).__name__} {e}", log_entries=log_entries, logger=logger)
                        continue
                if dirl:
                    if prev_entry:
                        entry_data = prev_entry.copy()
                    else:
                        entry_data = {
                            'idx_count': 0,
                            'idx_bytes': 0,
                            'max_depth': root.count(os.sep),
                            'type': '',
                            'target': ''
                        }

                    entry_data.update({
                        'modified_time': modified_dt if modified_dt else '',
                        'modified_ep': modified_ep,
                        'file_count': x
                    })
                    entry["cfr_data"][root] = entry_data
                if entry["dirl"] or entry["cfr_data"] or entry["cfr_reparse"]:
                    results.append(entry)

        except PermissionError as e:
            emit_log("DEBUG", f"process_directory: {root} error: {e}", log_entries=log_entries, logger=logger)
        except OSError as e:
            emit_log("DEBUG", f"chunk {i} of {num_chunks} file loop error detected process_directory : dir: {root} {type(e).__name__} {e} \n{traceback.format_exc()}", log_entries=log_entries, logger=logger)

    f = 0
    prog_v = 0
    scale = current_step = 0
    steps = step_len = 0

    if show_progress:
        scale = (endp - strt) / root_count
        steps = sorted(set(int((r / 10) * root_count) for r in range(1, 11)))
        step_len = len(steps)

    try:
        for dir_path in chunk:

            f += 1
            process_directory(Path(dir_path), dir_path)

            if show_progress:
                if current_step < step_len and f >= steps[current_step]:
                    prog_v = strt + (f * scale)
                    print(f"Progress: {prog_v:.2f}%", flush=True)
                    current_step += 1
        if show_progress and current_step <= len(steps) - 1:
            print(f"Progress: {endp:.2f}%", flush=True)
    except Exception as e:
        emit_log("DEBUG", f"file loop error {i}\\{num_chunks}, detected scan_created line {f} of {root_count} : dir: {dir_path} {type(e).__name__} {e}", log_entries=log_entries, logger=logger)
        if log_entries:
            write_logs_to_logger(log_entries, logger)
        raise

    return sys_data, results, log_entries, f
