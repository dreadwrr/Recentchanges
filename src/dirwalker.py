#   build first to find the files then distribute round-robin to multiprocessing            03/23/2026
# to hash. This was found to be the fastest as other methods have too much overhead

# scan the important files for modified with same mtime or spoofed timestamp
# this is done with os.scandir recursion multiprocessing. If caching is enabled the
# system directory mtimes are stored in gpg cache file.

# find created or downloads button use the cache to find files created or downloaded
# for fast search results of new files on the system
#
import logging
import gc
import multiprocessing
import os
import queue
import random
import sys
import sqlite3
import time
import threading
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime
from pathlib import Path
from .buildindex import build_index
from .config import dump_toml
from .config import set_json_settings
from .dirwalkerfunctions import check_specified_paths
from .dirwalkerfunctions import chunk_split
from .dirwalkerfunctions import decr_cache
from .dirwalkerfunctions import get_base_folders
from .dirwalkerfunctions import get_extension_tup
from .dirwalkerfunctions import get_filter_tup
from .dirwalkerfunctions import get_stat
from .dirwalkerfunctions import is_regular_executable
from .dirwalkerfunctions import is_shared_object
from .dirwalkerfunctions import resolve_profile_link
from .dirwalkerfunctions import shared_executable
from .dirwalkerlinux import get_config_data
from .dirwalkerfunctions import to_spec
from .dirwalkerparser import build_dwalk_parser
from .dirwalkersrg import create_new_index
from .dirwalkersrg import db_sys_changes
from .dirwalkersrg import hardlinks
from .dirwalkersrg import save_db
from .dirwalkersrg import sync_db
from .fileops import find_dir_link_target
from .fileops import find_link_target
from .gpgcrypto import encr
from .gpgcrypto import encrm
from .gpgcrypto import dict_string
from .gpgcrypto import dict_to_list_sys
from .logs import emit_log
from .logs import init_process_worker
from .logs import logging_worker
from .logs import logs_to_queue
from .logs import setup_logger
from .logs import write_logs_to_logger
from .pyfunctions import cprint
from .pyfunctions import epoch_to_str
from .pysql import find_symmetrics
from .qtdrivefunctions import get_idx_tables
from .qtdrivefunctions import parse_systimeche
from .rntchangesfunctions import change_perm
from .rntchangesfunctions import cnc
from .rntchangesfunctions import name_of
from .rntchangesfunctions import porteus_linux_check
from .scancreated import scan_created
from .scanindex import scan_index
from .xzmprofile import XzmProfile

# Globals
fmt = "%Y-%m-%d %H:%M:%S"


def scan_files(basedir, layer, xzm_obj, is_exec, is_sym, logger):
    ''' XzmProfile shield os.scandir '''
    non_matches = {}
    matches = {}
    cckSEEN, idx_bytes = set(), set()
    try:

        def scan_dir(root, current_depth=0, r=0, j=0):

            ix = 0
            if root in cckSEEN:
                return r, j

            cckSEEN.add(root)

            with os.scandir(root) as entries:
                relative = os.path.relpath(root, layer)
                base = os.path.join(basedir, relative)
                for entry in entries:

                    path = entry.path
                    found = False
                    in_binary = False
                    target = None

                    try:

                        if entry.is_dir(follow_symlinks=False):
                            if path != basedir:
                                r, j = scan_dir(path, current_depth + 1, r, j)

                        elif entry.is_file():
                            filename = entry.name
                            symlink = entry.is_symlink()

                            if not is_sym and symlink:
                                continue
                            stat_info = get_stat(entry, logger=logger)
                            if not stat_info:
                                continue
                            j += 1

                            full_path = os.path.join(base, filename)

                            if full_path.startswith(xzm_obj.path_tup):
                                found = True
                                if is_exec:
                                    if is_shared_object(file_name=filename.lower()):
                                        if not shared_executable(path, logger):
                                            found = False
                                    elif not is_regular_executable(stat_info):
                                        found = False
                            else:
                                in_library = full_path.startswith(xzm_obj.library_tup) and is_shared_object(file_name=filename.lower())
                                in_binary = not in_library and full_path.startswith(xzm_obj.binary_tup)
                                if in_library:
                                    found = True
                                    if is_exec and not shared_executable(path, logger):
                                        found = False
                                        logger.debug(f"scan_dir skipping on is_exec flag for .so file: {path}")
                                elif in_binary:
                                    if is_regular_executable(stat_info):
                                        found = True
                            if symlink:
                                target = resolve_profile_link(path, base, logger)

                            sze = stat_info.st_size
                            dev = stat_info.st_dev
                            ino = stat_info.st_ino
                            if found:
                                r += 1
                                if ino != 0:
                                    key = (dev, ino)
                                    if key not in idx_bytes:
                                        ix += sze
                                        idx_bytes.add((dev, ino))
                                else:
                                    ix += sze
                                matches[full_path] = (full_path, path, stat_info, symlink, target, True, ino)
                            else:
                                non_matches[full_path] = (full_path, path, stat_info, symlink, target, False, ino)

                    except OSError as e:
                        logger.error(f"scan_dir Exception scanning file {path}: {type(e).__name__} {e}", exc_info=True)

            return r, j

        r, j = scan_dir(layer)

    except Exception as e:
        emsg = f"scan_files: {type(e).__name__} {e}"
        print(emsg)
        logger.error(emsg, exc_info=True)
        return None, None, 0, 0

    return non_matches, matches, r, j


def collect_files(basedir, EXCLDIRS_FULLPATH, filter_tup, is_xzm_profile, matches, extn_tup, paths_tup, is_noextension, is_shared_library, is_exec, is_sym, logger):
    ''' proteusEXTN shield os.scandir '''
    all_entries = []
    log_entries = []
    dir_data = {}
    cckSEEN, idx_bytes = set(), set()
    try:

        def collect_scan(root, root_modified_dt=None, root_modified_ep=None, current_depth=0, max_depth=0, r=0, j=0):

            x = 0
            ix = 0
            idx_files = 0
            rtype = None
            try:

                if root in cckSEEN:
                    return max_depth, r, j
                cckSEEN.add(root)

                max_depth = max(max_depth, current_depth)

                with os.scandir(root) as entries:
                    for entry in entries:

                        rtype = None
                        symlink = False
                        target = None
                        shared_object = False
                        found = False
                        is_path_match = False
                        path = entry.path

                        modified_dt = None
                        modified_ep = None

                        try:

                            if entry.is_symlink():
                                symlink = True

                            if entry.is_dir():

                                if path in EXCLDIRS_FULLPATH:
                                    continue
                                stat_info = get_stat(entry, logger=logger)
                                if not stat_info:
                                    continue

                                if symlink:
                                    rtype = "symlink"

                                modified_ep = stat_info.st_mtime
                                modified_dt = epoch_to_str(modified_ep)

                                if not rtype:
                                    if path != basedir:
                                        max_depth, r, j = collect_scan(path, modified_dt, modified_ep, current_depth + 1, max_depth, r, j)
                                else:
                                    target = find_link_target(path, logger=logger)

                            elif entry.is_file():

                                if not (symlink and not is_sym):
                                    filename = entry.name
                                    x += 1
                                    j += 1

                                    if is_xzm_profile:
                                        if path in matches:
                                            found = True
                                            idx_files += 1
                                            r += 1
                                            entry = matches.get(path)
                                            stat_info = entry[2] if entry else None
                                            if stat_info:
                                                sze = stat_info.st_size
                                                ix += sze
                                    else:

                                        if path.lower().startswith(filter_tup):
                                            continue

                                        elif path.startswith(paths_tup):
                                            is_path_match = True
                                            found = True
                                        else:
                                            if is_noextension:
                                                if "." not in filename or (filename.startswith(".") and filename.count(".") == 1):
                                                    found = True
                                            if not found:
                                                filename_lower = filename.lower()
                                                if filename_lower.endswith(extn_tup):
                                                    found = True
                                                elif is_shared_library:
                                                    if is_shared_object(filename_lower):
                                                        shared_object = True
                                                        found = True

                                        if found:
                                            stat_info = get_stat(entry, logger=logger)
                                            if not stat_info:
                                                continue
                                            if not is_path_match and not to_spec(path, stat_info, shared_object, is_shared_library, is_exec, logger):
                                                continue

                                            if symlink:
                                                target = find_link_target(path, logger=logger)

                                            idx_files += 1
                                            r += 1
                                            sze = stat_info.st_size
                                            dev = stat_info.st_dev
                                            ino = stat_info.st_ino
                                            if stat_info.st_nlink > 1:
                                                if ino != 0:
                                                    key = (dev, ino)
                                                    if key not in idx_bytes:
                                                        idx_bytes.add(key)
                                                        ix += sze
                                                else:
                                                    ix += sze
                                            else:
                                                ix += sze

                                            all_entries.append((path, path, stat_info, symlink, target, found, ino))
                            else:
                                if symlink:
                                    target = find_dir_link_target(path, logger=logger)
                                    if target:
                                        rtype = "symlink"
                                        stat_info = get_stat(entry, logger=logger)
                                        if not stat_info:
                                            logger.debug(f"could not stat broken dir symlink {path}")
                                            continue
                                        modified_ep = stat_info.st_mtime
                                        modified_dt = epoch_to_str(modified_ep)
                            if rtype:

                                entry_data = {
                                    'modified_time': modified_dt if modified_dt else '',
                                    'modified_ep': modified_ep,
                                    'file_count': 0,
                                    'idx_count': 0,
                                    'idx_bytes': 0,
                                    'max_depth': path.count(os.sep),
                                    'type': rtype,
                                    'target': target
                                }
                                dir_data[path] = entry_data

                        except OSError as e:
                            logger.error(f"collect_scan Exception scanning {'symlink' if symlink else ''} {path}: {type(e).__name__} {e}", exc_info=True)

                    entry_data = {
                        'modified_time': root_modified_dt if root_modified_dt else '',
                        'modified_ep': root_modified_ep,
                        'file_count': x,
                        'idx_count': idx_files,
                        'idx_bytes': ix,
                        'max_depth': root.count(os.sep),
                        'type': '',
                        'target': ''
                    }
                    dir_data[root] = entry_data

            except PermissionError:
                logger.debug(f"collect_scan Permission denied scanning: {root}")
            except OSError as e:
                logger.error(f"collect_scan Exception scanning {root}: {type(e).__name__} {e}", exc_info=True)

            return max_depth, r, j

        root_stat = os.stat(basedir)
        modified_ep = root_stat.st_mtime
        modified_dt = epoch_to_str(modified_ep)

        max_depth, r, j = collect_scan(basedir, modified_dt, modified_ep)

    except OSError as e:
        print(f"Couldnt stat unable to access drive {basedir}: {e}")
        return None, None, None, 0, 0, 0
    except Exception as e:
        emsg = f"collect_files Exception: {type(e).__name__} {e}"
        print(emsg)
        logger.error(f"{emsg}", exc_info=True)
        return None, None, None, 0, 0, 0

    return all_entries, dir_data, log_entries, max_depth, r, j


# Find downloads
#
# The following uses cache built from a system index to find created files or downloads. Potentially being faster than
# the find command or a powershell search. It will update the cache with new directory modified times. Also, any new directories. The
# cache is a list of all directories on the system. The directory mtime is updated. dir mtime is updated when files are added, removed or renamed only.
#
# Drive index find downloads
# systimeche.gpg aka CACHE_S


def find_created(appdata_local, dbopt, dbtarget, basedir, user, mdltype, tempdir, gnupg_home, CACHE_S, dspEDITOR, dspPATH, email, ANALYTICSECT=True, compLVL=200):

    cfr_src = decr_cache(CACHE_S, user=user)
    if not cfr_src:
        print(f"Unable to retrieve cache file {CACHE_S} quitting.")
        return 1
    appdata_local = Path(appdata_local)
    config_data = get_config_data(appdata_local, user)  # mdltype is passed in for device from qt as driveTYPE

    home_dir = config_data.home_dir
    log_file = config_data.log_file
    uid = config_data.uid
    gid = config_data.gid
    config = config_data.config
    EXCLDIRS = config_data.EXCLDIRS
    nogo = config_data.nogo
    filterout_list = config_data.filterout_list
    ll_level = config_data.ll_level
    MODULENAME = config['paths']['MODULENAME']

    EXCLDIRS += nogo

    # sensitivity adjust

    # search_archive = appdata_local / f"{MODULENAME}_MDY_"  # windows
    # search_exclude = str(search_archive.relative_to(search_archive.anchor))
    # EXCLDIRS += search_exclude

    # dir_pth = os.path.join("/tmp", f"{MODULENAME}_MDY_*")  # linux
    # folders = glob.glob(dir_pth)
    # search_exclude = [Path(fld).name for fld in folders]
    # EXCLDIRS += search_exclude

    filterout_list = [os.path.join(basedir, d) for d in filterout_list]

    if basedir == "/":
        MODULENAME = config['paths']['MODULENAME']
        home_dir = config_data.home_dir
        download_results = os.path.join(home_dir, "Downloads", MODULENAME + 'x')
        pst_data = home_dir / ".local" / "share" / "recentchanges"
        flth_frm = pst_data / "flth.csv"  # filter hits
        CACHE_F_frm = os.path.join(pst_data, "ctimecache.gpg")
        CACHE_S_frm, _ = parse_systimeche(basedir, CACHE_S)
        CACHE_S_frm = os.path.join(pst_data, CACHE_S_frm)
        filterout_list.append(str(flth_frm))
        filterout_list.append(download_results)
        filterout_list.append(CACHE_F_frm)
        filterout_list.append(CACHE_S_frm)

    EXCLDIRS_FULLPATH = set(os.path.join(basedir, d) for d in EXCLDIRS)
    filter_tup = get_filter_tup(filterout_list)

    base_folders, root_count = get_base_folders(basedir, EXCLDIRS_FULLPATH)
    if root_count == 0:
        print(f"Unable to read base folders of drive {basedir} the drive could be empty or check permissions")
        return 1

    all_sys = []  # results
    systime_results = []  # actions/custom msg
    all_logs = []

    rlt = strt = prog_v = 0
    endp = 80
    incr = 10

    logging_values = (appdata_local, ll_level, tempdir)
    logroot = setup_logger(log_file, logging_values[1], "DOWNLOADS")
    change_perm(log_file, uid, gid)

    if mdltype.lower() == "hdd":

        show_progress = True
        start = time.time()
        try:
            i = num_chunks = 1

            all_sys, systime_results, all_logs, _ = scan_created(
                base_folders, basedir, EXCLDIRS_FULLPATH, filter_tup, cfr_src, root_count, i, num_chunks, show_progress, logroot, strt, endp
            )

            prog_v = endp
        except Exception as e:
            emsg = f"find_created error in scan_created while finding downloads serially: {e} {type(e).__name__}"
            print(emsg)
            logroot.error(emsg, exc_info=True)
            return 1

    else:
        random.shuffle(base_folders)
        len_basefolders = len(base_folders)
        # num_chunks = max(1, min(len_basefolders, multiprocessing.cpu_count(), 8))
        # chunks = [list(map(str, c)) for c in np.array_split(base_folders, num_chunks)]

        # manual. numpy is already used by pandas and available
        # max_workers = min(8, os.cpu_count() or 4)
        min_chunk_size = 2
        max_workers = max(1, min(8, os.cpu_count() or 4, len_basefolders // min_chunk_size))
        chunk_size = max(1, (len_basefolders + max_workers - 1) // max_workers)
        chunks = [base_folders[i:i + chunk_size] for i in range(0, len_basefolders, chunk_size)]
        num_chunks = len(chunks)

        deltav = endp - strt
        done = 0

        start = time.time()

        with ProcessPoolExecutor(max_workers=num_chunks) as executor:

            futures = [
                executor.submit(
                    scan_created, chunk, basedir, EXCLDIRS_FULLPATH, filter_tup, cfr_src, root_count, i, num_chunks, False
                )
                for i, chunk in enumerate(chunks)
            ]
            for future in as_completed(futures):  # for future in futures:
                try:
                    sys_data, dirl, log_, r = future.result()
                    if sys_data:
                        all_sys.extend(sys_data)
                    if dirl:
                        systime_results.extend(dirl)
                    if log_:
                        all_logs.extend(log_)

                    done += r
                    percent = done / len_basefolders
                    prog_v = round(strt + percent * (deltav), 2)

                    print(f"Progress: {prog_v:.2f}%", flush=True)
                except BrokenProcessPool as e:
                    print("find created failed in mc")
                    logroot.error("unable to build IDX. %s", e, exc_info=True)
                    rlt = 1
                    break
                except Exception as e:
                    emsg = f"find_created Worker error: {e} {type(e).__name__}"
                    print(emsg)
                    logroot.error(emsg, exc_info=True)
                    rlt = 1
                    break

        write_logs_to_logger(all_logs, logroot)  # logs_to_queue(logs, queue)
    prog_v += incr
    end = time.time()

    if rlt == 0:
        if ANALYTICSECT:
            el = end - start
            print(f'Search took {el:.3f} seconds')

        if systime_results:

            try:
                cfr_data = {}  # delta . changed folder mtime or new folders
                cfr_insert = {}  # reparse points
                # dirl_add = {}  # all cache hits

                for entry in systime_results:
                    cfr_data.update(entry.get("cfr_data", {}))
                    cfr_insert.update(entry.get("cfr_reparse", {}))
                    # dirl_add.update(entry.get("dirl", {}))

                # if dirl_add:
                # keys_to_delete = set(cfr_src) - set(dirl_add) # original - all hits    which is dirl_add
                # key_rm = []
                # for key in keys_to_delete: # remove stale entries from the cache
                #     del cfr_src[key]
                #     key_rm.append(key)
                # del_keys = [(key,) for key in key_rm]
                if cfr_insert:         # add new reparse points after deltas
                    cfr_data.update(cfr_insert)
                    # if doing a seperate db update for reparse
                    #     key_ins =  []
                    #     key_ins = flatten_dict(cfr_insert)
                if cfr_data:  # update the cache with delta

                    for root, data in cfr_data.items():
                        cfr_src[root] = data

                    data_to_write = dict_to_list_sys(cfr_src)

                    for row in data_to_write:
                        for k, v in row.items():
                            if not isinstance(v, str):
                                row[k] = str(v)
                    ctarget = dict_string(data_to_write)

                    print(f"Progress: {prog_v:.2f}%")
                    prog_v += incr

                    # update database
                    key_upt = []
                    for folder, data in cfr_data.items():
                        key_upt.append((
                            data.get('modified_time'),
                            folder,
                            data.get('file_count'),
                            data.get('idx_bytes'),
                            data.get('max_depth'),
                            data.get('type'),
                            data.get('target')
                        ))
                    # insert/update database
                    # del_keys is to remove db entries for deleted folders if wanting to maintain but no need
                    if sync_db(dbopt, basedir, CACHE_S, None, None, None, key_upt, from_idx=True):
                        nc = cnc(dbopt, compLVL)
                        if encr(dbopt, dbtarget, email, no_compression=nc, dcr=True):
                            nc = cnc(CACHE_S, compLVL)
                            if encrm(ctarget, CACHE_S, email, no_compression=nc):

                                print(f"Progress: {prog_v:.2f}%", flush=True)
                            else:
                                rlt = 1
                                print(f"Cache reencryption failed {CACHE_S} find_created dirwalker.py")
                            change_perm(dbtarget, uid, gid, 0o644)
                            change_perm(CACHE_S, uid, gid, 0o644)
                        else:
                            rlt = 1
                    else:
                        rlt = 1
            except Exception as e:
                err_m = f'Unhandled exception in find_created during cache processing {type(e).__name__} {e}'
                print(err_m)
                logging.error(err_m, exc_info=True)

        # output results
        t = 0
        if all_sys:

            # 3 files used by find created. results file, database gpg (dbtarget) and a gpg file for exclusions
            output_file = f'{MODULENAME}xcreated.txt'
            # temp_dir = tempfile.mkdtemp()
            temp_f = os.path.join(tempdir, output_file)

            local_gpg = os.path.join(gnupg_home, "random_seed")

            all_sys.sort(key=lambda x: x[1])

            with open(temp_f, "w", encoding="utf-8") as f:
                for entry in all_sys:
                    if len(entry) >= 2:
                        full_path = entry[0]

                        if full_path not in (temp_f, dbopt, dbtarget, local_gpg):
                            t += 1
                            mod_time = epoch_to_str(entry[1])
                            print(f'{mod_time} {full_path}', file=f)
                            print(full_path, mod_time)

            if t > 0:
                print(f"RESULT: {temp_f}")
                # display(dspEDITOR, temp_f, True, dspPATH)  # disabled
            else:
                print("No results or no new files found")
        else:
            print("There were no results")
    if rlt == 0:
        print("Progress: 100.00%", flush=True)
    return rlt


#  Build IDX system profile
#
# uses os.scandir to first find the applicable files then split and send to workers
# to hash the system profile. A cache file systimeche.gpg is made of all the directories
# on the system.
#
# System profile and cache file. or index drive for cache file
#
# How dirwalker was developed
# 1 base_folders = get_base_folders() random.shuffle(base_folders). bad load balancing
# 2 get all directories randomize sort split. was found to be same and slower. Also bad load balancing.
# all_dirs = collect_dirs()
# all_dirs.sort(key=lambda x: x[1], reverse=True)
# chunks = split_dirs_for_workers(all_dirs, num_chunks)
# chunks = [ [dir_path for dir_path, _ in chunk] for chunk in chunks ]
# num_chunks = max(1, multiprocessing.cpu_count())
# chunks = split_dirs_for_workers(all_dirs, num_chunks)
#
# 3
def index_system(appdata_local, dbopt, dbtarget, basedir, user, CACHE_S, email, ANALYTICSECT=False, idx_drive=False, gnupghome=None, compLVL=200, iqt=False, strt=0, endp=100):

    appdata_local = Path(appdata_local)
    config_data = get_config_data(appdata_local, user)

    toml_file = config_data.toml_file
    json_file = config_data.json_file
    log_file = config_data.log_file
    uid = config_data.uid
    gid = config_data.gid
    config = config_data.config
    EXCLDIRS = config_data.EXCLDIRS
    nogo = config_data.nogo
    filterout_list = config_data.filterout_list
    driveTYPE = config_data.driveTYPE
    ll_level = config_data.ll_level
    is_xzm_profile = config['shield']['xzm']
    extension = config['shield']['proteusEXTN']
    configured_paths = config['shield']['proteusPATH']
    is_exec = config['shield']['exec']
    is_sym = config['shield']['sym']

    if is_xzm_profile:
        res = porteus_linux_check()
        if not res:
            if res is None:
                print("inconclusive if distro is porteus ")
            print(".xzm not supported using default specifications.")
            # update_toml_values({'shield': {'xzm': False}}, toml_file)
            config['shield']['xzm'] = False
            dump_toml(None, config, toml_file)
        if not res or basedir != "/":
            is_xzm_profile = False

    rlt = 0

    xzm_obj = None
    is_noextension = False  # ""
    is_shared_library = False  # *.so.*

    deltav = endp - strt
    proval = deltav * .15 + strt
    if iqt:
        print(f"Progress: {strt}%", flush=True)  # so gui knows not to prompt user for password

    parsedsys = []
    dir_data = {}

    all_files = []
    matches, non_matches = {}, {}

    paths_tup, extn_tup = (), ()

    paths_tup, _ = check_specified_paths(basedir, configured_paths, "proteusPATHS", suppress=False)  # add basedir to paths and any paths that dont exist pull out and tell user
    # exec_tup # windows
    extn_tup, is_noextension, is_shared_library = get_extension_tup(extension)  # set flags

    # handle inclusions EXCLDIRS filterout_list get converted to tuples after
    EXCLDIRS += nogo

    # handle exclusions
    # Linux temp folder
    exclude_temp = "tmp"
    if exclude_temp not in EXCLDIRS:
        EXCLDIRS.append('tmp')
    # filter out
    filterout_list = [os.path.join(basedir, d) for d in filterout_list]
    if basedir == "/":
        # biggest exclude is .gnupg/random_seed and any runtime files
        #
        # Note:
        #
        #
        xdg_runtime = config_data.xdg_runtime
        home_dir = config_data.home_dir
        file_out = xdg_runtime / "file_output"
        pst_data = home_dir / ".local" / "share" / "recentchanges"
        MODULENAME = config['paths']['MODULENAME']

        filterout_list.append(str(file_out))
        if not is_xzm_profile:
            download_results = os.path.join(home_dir, "Downloads", MODULENAME + "x")
            filterout_list.append(download_results)
            if '.gpg' in extension:

                CACHE_F_frm = os.path.join(pst_data, "ctimecache.gpg")
                CACHE_S_frm, _ = parse_systimeche(basedir, CACHE_S)
                CACHE_S_frm = os.path.join(pst_data, CACHE_S_frm)

                filterout_list.append(CACHE_F_frm)
                filterout_list.append(CACHE_S_frm)
                filterout_list.append(dbtarget)

            if ".csv" in extension:

                flth_frm = pst_data / "flth.csv"
                filterout_list.append(str(flth_frm))

            if ".db" in extension:
                filterout_list.append(dbopt)

        if is_noextension and gnupghome:

            file_exclude = os.path.join(gnupghome, "random_seed")
            if file_exclude not in filterout_list:
                filterout_list.append(file_exclude)

    EXCLDIRS_FULLPATH = set(os.path.join(basedir, d) for d in EXCLDIRS)
    filter_tup = get_filter_tup(filterout_list)

    logging_values = (log_file, ll_level, appdata_local)
    rootlogger = setup_logger(log_file, logging_values[1], "BUILDIDX")
    change_perm(log_file, uid, gid)

    if is_xzm_profile:

        # Search for the files in scan_files efficiently as its a seperate function. Build a matched dict of root paths to efficiently build the directory cache
        # afterwards in collect_files. When complete multiprocess just has to hash.

        xzm_obj = XzmProfile(basedir, True)
        layers = xzm_obj.layers
        logger = logging.getLogger("SCANFILES")

        start = time.time()

        prog_v = proval / 3  # 33% of deltav

        r = j = 0
        for x, layer in enumerate(layers):
            not_found, found_files, h, e = scan_files(basedir, layer, xzm_obj, is_exec, is_sym, logger)
            r += h
            j += e
            if not_found:
                for key, value in not_found.items():
                    if key not in non_matches:
                        non_matches[key] = value
            elif not_found is None:
                print(f"loop count {x} An error occurred while initially indexing {layer}.")
                return 1
            if found_files:
                for key, value in found_files.items():
                    if key not in matches:
                        matches[key] = value
                    else:
                        r -= 1
            else:
                print(f"loop count {x} No files found while searching {layer}.")

        if not matches:
            print("No matches for xzm profile exit")
            return 1

        all_files.extend(non_matches.values())
        all_files.extend(matches.values())
        if iqt:
            print(f"Progress: {prog_v:.2f}%")

        # print("Found files ", r)

        logger = logging.getLogger("COLLECTFILES")
        start = time.time()
        _, dir_data, log_entries, max_depth, r, j = collect_files(
            basedir, EXCLDIRS_FULLPATH, filter_tup, is_xzm_profile, matches, extn_tup, paths_tup,
            is_noextension, is_shared_library, is_exec, is_sym, logger
        )  # 66% of deltav

        # print("On system ", r)

    else:

        # Regular proteus shield it is a custom profile from config. use collect_files to find the files and then build the directory cache at the same time

        logger = logging.getLogger("COLLECTFILES")
        start = time.time()
        all_files, dir_data, log_entries, max_depth, r, j = collect_files(
            basedir, EXCLDIRS_FULLPATH, filter_tup, is_xzm_profile, matches, extn_tup, paths_tup,
            is_noextension, is_shared_library, is_exec, is_sym, logger
        )

    end = time.time()
    if log_entries:
        write_logs_to_logger(log_entries, logger)

    if all_files is None:
        print(f"An error occurred while initially indexing {basedir}")
        return 1
    elif not all_files:  # if j == 0
        print(f"No files found while {'building directory index' if is_xzm_profile else 'searching'} {basedir}.")
        print("if exec setting is True it can filter too many results try to adjust or set different extns to include more results")
        return 1

    if iqt:
        print(f"Progress: {proval:.2f}%")  # 15 %

    prog_v = proval
    el = end - start
    if ANALYTICSECT:
        print(f'\nCache indexing took {el:.3f} seconds\n')
        print(f'Total files during search: {j}')
        print("Found files ", r)
        if dir_data:
            dr = len(dir_data)
            print(f'Directory count: {dr}')
        print(f'Max depth {max_depth}')

    # if its a drive index make it and return early
    if idx_drive:
        res = create_new_index(
            dbopt, dbtarget, basedir, CACHE_S, email, user, None, dir_data, idx_drive=idx_drive, compLVL=compLVL,
            dcr=True, error_message="Reencryption failed drive idxcache not saved."
        )  # weigh is 60%
        prog_v = deltav * .60 + proval  # 75%
        if iqt:
            print(f"Progress: {prog_v:.2f}%")
        if res == 0:
            change_perm(dbtarget, uid, gid, 0o644)
            change_perm(CACHE_S, uid, gid, 0o644)
            print(f'{"Drive index" if idx_drive else "System profile"} complete')
            if iqt:
                print("Progress: 100%", flush=True)
        elif res == 4:
            return 52  # likely encryption failure database integrity is fine
        return res

    if r == 0:
        print("failed to build profile an error occured there were no matched files. exitting")
        return 1

    endval = deltav * .90 + strt

    cprint.cyan('\nRunning checksum.')

    logger = logging.getLogger("BUILDIDX")

    total = len(all_files)
    batch_size = 500

    show_progress = False
    if iqt:
        show_progress = True

    if total < batch_size or driveTYPE.lower() == "hdd":

        start = time.time()
        # queue = LoggingQueue(logger)
        log_q = queue.SimpleQueue()
        init_process_worker(log_q)
        try:
            i = num_chunks = 1

            tlog = threading.Thread(target=logging_worker, args=(log_q, total, prog_v, endval, show_progress, rootlogger), daemon=True)
            tlog.start()

            parsedsys, logs, _ = build_index(all_files, i, num_chunks, show_progress, prog_v, endval)
            if logs:
                logs_to_queue(logs, log_q)
        except Exception as e:
            emsg = f"Error occurred index_system while building index serially: {type(e).__name__} : {e}"
            print(emsg)
            emit_log("ERROR", f"{emsg} \n{traceback.format_exc()}", log_q)
            return 1

        finally:
            log_q.put(None)
            tlog.join()

    else:

        chunks = chunk_split(all_files, total, batch_size=batch_size)
        num_chunks = len(chunks)
        max_workers = max(1, min(8, multiprocessing.cpu_count() or 1, num_chunks))

        sys_data = []

        start = time.time()

        # with multiprocessing.Manager() as manager:
        # queue = manager.Queue()
        # logging_thread = threading.Thread(target=logging_worker, args=(queue, logger))
        # logging_thread.start()

        ctx = multiprocessing.get_context()
        log_q = ctx.Queue(maxsize=4096)
        log_t = threading.Thread(target=logging_worker, args=(log_q, total, prog_v, endval, show_progress, rootlogger), daemon=True)
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
                        build_index, chunk, i, num_chunks, show_progress
                    )
                    for i, chunk in enumerate(chunks)
                ]

                for future in as_completed(futures):
                    try:
                        sys_data, logs, _ = future.result()

                        if sys_data:
                            parsedsys.extend(sys_data)
                        if logs:
                            logs_to_queue(logs, log_q)

                        # done += processed
                        # if iqt:
                        #     percent = prog_v + round((deltav) * done / total)
                        #     print(f"Progress: {percent}%", flush=True)
                    except BrokenProcessPool as e:
                        print("build IDX failed in mc")
                        emit_log("ERROR", f"unable to build IDX. {e} \n{traceback.format_exc()}", log_q)
                        rlt = 1
                        break
                    except Exception as e:
                        emsg = f"Worker error occurred index_system: {type(e).__name__} : {e}"
                        print(emsg)
                        emit_log("ERROR", f"{emsg} \n{traceback.format_exc()}", log_q)
                        rlt = 1
                        break

        finally:
            log_q.put(None)
            log_t.join()
            log_q.close()
            log_q.join_thread()
        # finally:
        #     queue.put(('STOP', None))
        #     logging_thread.join()

    end = time.time()
    proval = endval
    if rlt == 0:

        # save system profile
        if parsedsys:
            if ANALYTICSECT:
                el = end - start
                print(f'Search took {el:.3f} seconds')

            # flatten dict of dicts and store. save cache file and store in db
            rlt = create_new_index(
                dbopt, dbtarget, basedir, CACHE_S, email, user, parsedsys, dir_data, idx_drive=False, compLVL=compLVL,
                dcr=True, error_message="Reencryption failed sys idxcache not saved."
            )
            if rlt == 0:

                if iqt:
                    print(f"Progress: {endp}%", flush=True)
                else:
                    _, suffix = parse_systimeche(basedir, CACHE_S)
                    if xzm_obj:
                        extn = xzm_obj.create_xzm_baseline(suffix, json_file)
                    else:
                        extn = extension + configured_paths
                    set_json_settings({"proteusEXTN": extn}, drive=suffix, filepath=str(json_file))

                print("System profile complete")
            elif rlt == 4:
                rlt = 52
        else:
            rlt = 1
            print("Index failed to build. no results from multiprocessing.")

        # its encrypted as the user with this script run as root. change perm to user anyway incase any problem happened so not creating another problem.
        change_perm(dbtarget, uid, gid, 0o644)
        change_perm(CACHE_S, uid, gid, 0o644)  # cache_idx could be false

    gc.collect()
    return rlt

# Scan IDX
#
# get the index from sys table recent.db and find differences


def scan_system(appdata_local, dbopt, dbtarget, basedir, user, difffile, CACHE_S, email, ANALYTICSECT=True, showDiff=False, compLVL=200, dcr=False, iqt=False, strt=0, endp=100):

    if not os.path.isfile(dbopt):
        print(f"scan_system Unable to locate {dbopt}")
        return 1

    rlt = 0
    appdata_local = Path(appdata_local)
    config_data = get_config_data(appdata_local, user)

    log_file = config_data.log_file
    uid = config_data.uid
    gid = config_data.gid
    driveTYPE = config_data.driveTYPE
    ll_level = config_data.ll_level

    config = config_data.config
    is_sym = config['shield']['sym']

    sys_tables, cache_table, _ = get_idx_tables(basedir, CACHE_S)

    if iqt:
        print(f"Progress: {strt}%")

    recent_sys = db_sys_changes(dbopt, sys_tables)  # retrieve profile from db

    if recent_sys is None:  # error
        print("\nThere was no return retrieving profile from db_sys_changes in scan_system indicating a problem. if having problems delete recent.gpg")
        return 1
    elif recent_sys is False:  # commandline for recentchangessearch.py there is no profile yet
        return 7
    elif not recent_sys:  # empty query retrieve 0 rows
        print(f"No results querying {', '.join(sys_tables)} from db_sys_changes in scan_system")
        return 0

    print("Finding differences running checksum.", flush=True)

    all_sys = []  # changed file info\meta
    link_diff = []  # symlink target changes
    nfs_records = []
    x = 0
    y = 0

    logging_values = (appdata_local, ll_level)
    logger = setup_logger(log_file, logging_values[1], "SCANIDX")
    change_perm(log_file, uid, gid)

    total = len(recent_sys)
    batch_size = 500
    delta_p = endp - strt
    endval = delta_p * .9 + strt

    show_progress = False
    if iqt:
        show_progress = True

    if total < batch_size or driveTYPE.lower() == "hdd":

        log_q = queue.SimpleQueue()
        init_process_worker(log_q)

        start = time.time()
        try:
            i = num_chunks = 1

            tlog = threading.Thread(target=logging_worker, args=(log_q, total, strt, endval, show_progress, logger), daemon=True)
            tlog.start()

            all_sys, link_diff, nfs_records, log_entries, x, y, _ = scan_index(recent_sys, is_sym, i, num_chunks, show_progress, strt, endval)
            if log_entries:
                logs_to_queue(log_entries, log_q)

        except Exception as e:
            emsg = f"scan_system exception in scan_index while scanning serially: {type(e).__name__} {e}"
            print(emsg)
            emit_log("ERROR", f"{emsg} \n{traceback.format_exc()}", log_q)
            return 1
        finally:
            log_q.put(None)
            tlog.join()

    else:

        chunks = chunk_split(recent_sys, total, batch_size=batch_size)
        num_chunks = len(chunks)
        max_workers = max(1, min(8, multiprocessing.cpu_count() or 1, num_chunks))

        start = time.time()
        # deltav = endval - strt

        ctx = multiprocessing.get_context()
        log_q = ctx.Queue(maxsize=4096)
        log_t = threading.Thread(target=logging_worker, args=(log_q, total, strt, endval, show_progress, logger), daemon=True)
        log_t.start()

        try:
            with ProcessPoolExecutor(
                max_workers=max_workers,
                mp_context=ctx,
                initializer=init_process_worker,
                initargs=(log_q,)
            ) as executor:

                futures = [
                    executor.submit(scan_index, chunk, is_sym, i, num_chunks, show_progress)
                    for i, chunk in enumerate(chunks)
                ]

                for future in as_completed(futures):

                    try:
                        sys_data, link_data, results, log_entries, x_c, y_c, _ = future.result()
                        if sys_data:
                            all_sys.extend(sys_data)
                        if link_data:
                            link_diff.extend(link_data)
                        if results:
                            nfs_records.extend(results)
                        if log_entries:
                            logs_to_queue(log_entries, log_q)
                        x += x_c
                        y += y_c

                        # if iqt:
                        #     percent = strt + round((deltav) * done / total)
                        #     print(f"Progress: {percent}%", flush=True)

                    except BrokenProcessPool as e:
                        emit_log("ERROR", f"fault while scanning idx. aborted {e} \n{traceback.format_exc()}", log_q)
                        rlt = 1
                        break
                    except Exception as e:
                        emsg = f"scan_system Worker error: {type(e).__name__} {e}"
                        print(emsg)
                        emit_log("ERROR", f"{emsg} \n{traceback.format_exc()}", log_q)
                        rlt = 1
                        break

        finally:
            log_q.put(None)
            log_t.join()
            log_q.close()
            log_q.join_thread()

    end = time.time()

    recent_files = []
    dir_diff = []
    new_diff = []
    cmsg = ""

    current_time = None

    if rlt == 0:

        if showDiff:
            systimeche = name_of(CACHE_S)
            dir_diff, new_diff = find_symmetrics(dbopt, cache_table, systimeche)

        if ANALYTICSECT:
            el = end - start
            print(f'Search took {el:.3f} seconds\n')
        if x != 0:
            p = (y / x) * 100
            if p > 30:
                cmsg = f"\nThe sys index had over 30% miss rate recommend rebuild index: {p:.2f}%"

        # output terminal
        if all_sys:

            # symmetric differences
            # show sylinks that have new targets
            # show the files that no longer exist from the miss rate
            for record in all_sys:
                record_str = ' '.join(map(str, record))
                recent_files.append(record_str)

            # Insert changes

            if save_db(dbopt, dbtarget, basedir, CACHE_S, email, user, None, None, all_sys, keys=None, idx_drive=False, compLVL=compLVL, dcr=dcr):
                change_perm(dbtarget, uid, gid, 0o644)
            else:
                rlt = 1
                print(f"Failed to insert profile changes into {sys_tables[1]} table in scan_system")
        else:
            print(f'No results found for sys index scan system{' multiprocessing' if driveTYPE.lower() == "ssd" else ''}.')
    else:
        print("Scan index failed scan_system dirwalker.py.")

    hdr1 = 'System index scan'
    mode = 'a' if os.path.isfile(difffile) else 'w'
    write_type = "appended" if mode == 'a' else "written"
    hdr2 = "The following files from sys index have changes by checksum\n"
    fstr = "timestamp,filename,creationtime,inode,accesstime,checksum,filesize,symlink,user,group,mode,casmod,target,lastmodified,hardlinks,count,mtimeus"
    current_time = datetime.now().strftime("MDY_%m-%d-%y-TIME_%H_%M")
    is_all_results = len(recent_files) > 0
    are_symmetrics = link_diff or nfs_records or dir_diff or new_diff
    # output at bottom diff file
    change_perm(difffile, 0)
    with open(difffile, mode) as f:
        if is_all_results:

            f.write("\n")
            print()
            print(hdr1, file=f)
            print(hdr2, file=f)
            print(fstr, file=f)
            print(hdr2)

            for record in recent_files:
                f.write(record + '\n')
                print(record)

            if cmsg:
                print(cmsg, file=f)
                print(cmsg)

            print(f"\nChanges {write_type} to difference file {difffile}")
            if showDiff and not are_symmetrics:
                print(current_time, file=f)

        # symmetric differences
        # symlink target change and files no longer present
        # show directories that had 0 files at indexing but now have files
        # show new directories since profile was created
        if showDiff and are_symmetrics:

            if not is_all_results:
                print("Directory differences found")
                f.write("\n")
                print()
                print(hdr1, file=f)

            if link_diff:
                link_header = "symlink(s) with changed target"
                f.write("\n")
                print(link_header, file=f)
                for i in range(0, len(link_diff), 2):
                    tup = link_diff[i]  # file record
                    if i+1 < len(link_diff):
                        second_tup = link_diff[i+1]  # old target new target
                        tup_str = " ".join(map(str, tup)) + " " + ">".join(map(str, second_tup))
                    else:
                        tup_str = " ".join(map(str, tup))
                    f.write(tup_str + "\n")

            if nfs_records:
                header = "following profile files no longer exist"
                f.write("\n")
                print(header, file=f)
                for tup in nfs_records:
                    tup_str = " ".join(map(str, tup))
                    f.write(tup_str + "\n")

            if dir_diff:
                diff_header = "Directory had 0 files when profile created but now has files"
                f.write("\n")
                print(diff_header, file=f)
                for tup in dir_diff:
                    f.write(" ".join(map(str, tup)) + "\n")

            if new_diff:
                p = len(new_diff)
                f.write('\n')
                print(f'{p} new directories since profile was created', file=f)
                for d in new_diff:
                    f.write(d + "\n")

            if is_all_results:
                print("Differences included")
            print(f"{write_type} to difference file {difffile}")
        elif showDiff:
            print("no symmetric differences found.")

    if rlt == 0:
        if iqt:
            print(f"Progress: {endp}%", flush=True)
    gc.collect()
    return rlt


# update the hardlink state for all files in the logs table. Any files that no longer exist are NULL and
# is useful to see that those file dont exist in the database viewer

def set_hardlinks(appdata_local, dbopt, dbtarget, basedir, user, uid, gid, tempdir, email, compLVL=200):
    """ Update hardlinks """

    appdata_local = Path(appdata_local)
    config_data = get_config_data(appdata_local, user)
    log_file = config_data.log_file
    ll_level = config_data.ll_level
    # tempdir = Path(tempdir)
    logging_values = (appdata_local, ll_level, tempdir)
    logger = setup_logger(log_file, logging_values[1], "HARDLINKS")
    # change_perm(log_file, uid, gid)

    rlt = 1

    if os.path.isfile(dbopt):

        with sqlite3.connect(dbopt) as conn:
            cur = conn.cursor()

            sts = hardlinks(basedir, dbopt, dbtarget, conn, cur, email, user, compLVL, logger)
            if sts:
                print("Progress: 100.00%", flush=True)
                rlt = 0
            change_perm(dbtarget, uid, gid, 0o644)

    else:
        print("dirwalker.py could not find dbopt: ", dbopt)

    return rlt


def main_entry(argv):
    parser = build_dwalk_parser()
    args = parser.parse_args(argv)

    if args.action == "hardlink":
        calling_args = [
            args.appdata, args.dbopt, args.dbtarget, args.basedir, args.user, args.uid, args.gid,
            args.tempdir, args.email, args.compLVL
        ]
        sys.exit(set_hardlinks(*calling_args))

    if args.action == "scan":
        calling_args = [
            args.appdata, args.dbopt, args.dbtarget, args.basedir, args.user, args.difffile, args.CACHE_S,
            args.email, args.ANALYTICSECT, args.showDiff, args.compLVL, args.dcr, args.iqt, args.strt,
            args.endp
        ]
        sys.exit(scan_system(*calling_args))

    elif args.action == "build":
        calling_args = [
            args.appdata, args.dbopt, args.dbtarget, args.basedir, args.user, args.CACHE_S, args.email,
            args.ANALYTICSECT, args.idx_drive, args.gnupghome, args.compLVL, args.iqt,
            args.strt, args.endp
        ]
        sys.exit(index_system(*calling_args))

    elif args.action == "downloads":
        calling_args = [
            args.appdata, args.dbopt, args.dbtarget, args.basedir, args.user, args.mdltype, args.tempdir,
            args.gnupghome, args.CACHE_S, args.dspEDITOR, args.dspPATH, args.email,
            args.ANALYTICSECT, args.compLVL
        ]
        sys.exit(find_created(*calling_args))
