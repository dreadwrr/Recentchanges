#   build first to find the files then distribute round-robin to multiprocessing            12/08/2025
# to hash. This was found to be the fastest as other methods have too much overhead

# scan the important files for modified with same mtime or spoofed timestamp
# this is done with os.scandir recursion multiprocessing. If caching is enabled the
# system directory mtimes are stored in gpg cache file.

# find created or downloads button use the cache to find files created or downloaded
# for fast search results of new files on the system
#
# usage:
# dirwalker.py build dbopt, dbtarget, basedir, updatehlinks, CACHE_S, email, ANALYTICSECT=False, idx_drive=False, cache_idx=True, compLVL=200, iqt=False, strt=0, endp=100
# dirwalker.py scan dbopt, dbtarget, basedir, difffile, updatehlinks, CACHE_S, email, ANALYTICSECT=True, showDiff=False, cache_idx=False, compLVL=200, dcr=False, iqt=False, strt=0, endp=100
# dirwalker.py downloads dbopt, dbtarget, basedir, mdltype, tempdir, CACHE_S, dspEDITOR, dspPATH, email, ANALYTICSECT=True, compLVL=200
import logging
import gc
import multiprocessing
import numpy as np
import os
import random
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from .buildindex import build_index
from .dirwalkerfnts import chunk_split
from .dirwalkerfnts import decr_cache
from .dirwalkerfnts import get_base_folders
from .dirwalkerfnts import get_dir_mtime
from .dirwalkerfnts import find_symmetrics
from .dirwalkerfnts import none_if_empty
from .dirwalkerfnts import os_walk_error
from .dirwalkersrg import db_sys_changes
from .dirwalkersrg import index_drive
from .dirwalkersrg import syncdb
from .dirwalkersrg import save_db
from .dirwalkerparser import build_dwalk_parser
from .fsearchfnts import get_reparse_type
from .pyfunctions import setup_logger
from .pyfunctions import cprint
from .pyfunctions import dict_string
from .pyfunctions import dict_to_list_sys
from .pyfunctions import epoch_to_str
from .pyfunctions import load_config
from .pyfunctions import get_wdir
# script_path = os.path.abspath(__file__)  // filter.py originally beside main.py
# script_dir = os.path.dirname(script_path)
# parent_dir = os.path.dirname(script_dir)
# sys.path.insert(0, parent_dir)
from .rntchangesfunctions import encrm
from .rntchangesfunctions import encr
from .rntchangesfunctions import get_idx_tables
from .rntchangesfunctions import getnm
from .rntchangesfunctions import display
from .rntchangesfunctions import intst
from .scancreated import scan_created
from .scancreated_hdd import scan_created_hdd
from .scanindex import scan_index

# mac/os  ;linux
# def get_creation_time(file_path):
#     stat_info = os.stat(file_path)
#     try:
#         # On macOS, birth time is available as 'st_birthtime'
#         return stat_info.st_birthtime
#     except AttributeError:
#         # On Linux and other Unix systems, this attribute may not exist
#         print("Birth time not available on this system.")
#         return None

#   get_config(appdata_local, "filter.toml")

appdata_local = get_wdir()
toml_file = appdata_local / "config" / "config.toml"
config = load_config(toml_file)
ll_level = config['search']['logLEVEL']
setup_logger(ll_level, "dirwalker", appdata_local)

# Globals
EXCLDIRS = config['search']['EXCLDIRS']
modelTYPE = config['search']['modelTYPE']
fmt = "%Y-%m-%d %H:%M:%S"


def collect_files(cacheidx, base_dir, EXCLDIRS_FULLPATH, extn_tuple, iqt=False, strt=0, endp=100):

    f_f = "collect_files"
    all_entries = []
    dir_data = {}

    x = None
    y = 0
    j = 0
    delta_val = 0

    try:

        if iqt:

            # top level directories for progress counting
            dir_list = []
            for d in os.listdir(base_dir):
                full = os.path.join(base_dir, d)
                if os.path.isdir(full) and full not in EXCLDIRS_FULLPATH:
                    dir_list.append(full)

            dir_set = set(dir_list)
            x = len(dir_set)
            delta_val = endp - strt

        for dirpath, dirnames, filenames in os.walk(base_dir, followlinks=False, onerror=os_walk_error):
            if dirpath in EXCLDIRS_FULLPATH:
                dirnames[:] = []  # prevent descending into subfolders
                continue

            if x and dirpath in dir_set:
                y += 1
                dir_set.discard(dirpath)
                prog_v = (y / x) * delta_val + strt
                if prog_v >= endp:
                    prog_v = endp
                    x = None
                print(f"Progress: {prog_v:.2f}%", flush=True)

            # dirnames[:] = [d for d in dirnames if os.path.join(dirpath, d) not in EXCLDIRS_FULLPATH]

            modified_dt, modified_ep = get_dir_mtime(dirpath, f_f)
            if not modified_ep:
                logging.debug(f"{f_f} Cant get directory mtime no access? dir {dirpath}")
                continue

            rtype = get_reparse_type(dirpath)
            if rtype:
                dirnames[:] = []

            idx_files = 0
            if not rtype:
                for fl_name in filenames:
                    j += 1
                    if fl_name.lower().endswith(extn_tuple):
                        idx_files += 1
                        all_entries.append(os.path.join(dirpath, fl_name))

            if cacheidx:
                entry = {
                    'modified_time': modified_dt if modified_dt else '',
                    'modified_ep': modified_ep,
                    'file_count': len(filenames),
                    'idx_count': idx_files,
                    'max_depth': dirpath.count(os.sep),
                    'type': rtype if rtype else '',
                    'target': os.path.realpath(dirpath) if rtype else ''
                }
                # if rtype:
                #     entry['type'] = rtype
                #     entry['target'] = os.path.realpath(dirpath)
                dir_data[dirpath] = entry

    except Exception as e:
        emsg = f"{f_f} Exception: {type(e).__name__} {e} \n{traceback.format_exc()}"
        print(emsg)
        logging.error(f"{emsg}", exc_info=True)
        return None, None, 0

    return all_entries, dir_data, j

# Find downloads
#
# The following uses cache built from a system index to find created files or downloads. Potentially being faster than
# the find command or a powershell search. It will update the cache if with a new directory modified time. Also, any new directories. The
# cache is a list of all directories on the system. The directory mtime is updated. This is updated when files are added, removed or renamed only.
#
# Drive index find downloads
# systimeche or systimeche_s <- CACHE_S


def find_created(dbopt, dbtarget, basedir, mdltype, tempdir, CACHE_S, dspEDITOR, dspPATH, email, ANALYTICSECT=True, compLVL=200):

    cfr_src = {}
    rlt = 0

    cfr_src = decr_cache(CACHE_S)
    if not cfr_src:
        print(f"Unable to retrieve cache file {CACHE_S} quitting.")
        return 1
    MODULENAME = config['paths']['MODULENAME']
    EXCLDIRS_FULLPATH = [os.path.join(basedir, d) for d in EXCLDIRS]

    base_folders, root_count = get_base_folders(basedir, EXCLDIRS_FULLPATH)
    if root_count == 0:
        print(f"Unable to read base folders of drive {basedir} the drive could be empty or check permissions")
        return 1

    all_sys = []  # results
    systime_results = []  # actions/custom msg

    strt = 0
    endp = 80
    prog_v = 0

    incr = 10

    if mdltype.lower() == "hdd":

        start = time.time()
        try:

            all_sys, systime_results = scan_created_hdd(base_folders, basedir, cfr_src, appdata_local, ll_level, root_count, strt=strt, endp=endp)
            prog_v = endp
        except Exception as e:
            emsg = f"find_created error in scan_created_hdd while finding downloads serially: {e} {type(e).__name__} \n{traceback.format_exc()}"
            rlt = 1
            print(emsg)
            logging.error(emsg, exc_info=True)
    else:
        random.shuffle(base_folders)

        num_chunks = max(1, min(len(base_folders), multiprocessing.cpu_count(), 8))
        chunks = [list(map(str, c)) for c in np.array_split(base_folders, num_chunks)]

        total_chunks = len(chunks)

        # min_chunk_size = 2
        # max_workers = max(1, min(8, os.cpu_count() or 4, len(base_folders) // min_chunk_size))

        # max_workers = min(8, os.cpu_count() or 4)      manual. numpy is already used by pandas and available
        # chunk_size = max(1, (len(base_folders) + max_workers - 1) // max_workers)
        # chunks = [base_folders[i:i + chunk_size] for i in range(0, len(base_folders), chunk_size)]

        b = 0

        start = time.time()
        with ProcessPoolExecutor(max_workers=total_chunks) as executor:

            futures = [
                executor.submit(
                    scan_created, chunk, basedir, cfr_src, appdata_local, ll_level, i
                )
                for i, chunk in enumerate(chunks)
            ]
            for future in as_completed(futures):  # for future in futures:
                try:
                    sys_data, dirl = future.result()
                    if sys_data:
                        all_sys.extend(sys_data)
                    if dirl:
                        systime_results.extend(dirl)
                except Exception as e:
                    emsg = f"find_created Worker error: {e} {type(e).__name__}"
                    rlt = 1
                    print(f'{emsg} \n{traceback.format_exc()}')
                    logging.error(emsg, exc_info=True)
                b += 1
                percent = b / total_chunks
                prog_v = round(strt + percent * (endp - strt), 2)
                print(f"Progress: {prog_v:.2f}%", flush=True)
    prog_v += incr
    end = time.time()
    if rlt == 0:
        if ANALYTICSECT:
            el = end - start
            print(f'Search took {el:.3f} seconds')

        if systime_results:

            try:
                cfr_data = {}  # delta . changed folder mtime or new folders
                # cfr_insert = {} # reparse points
                # dirl_add = {} # all cache hits

                for entry in systime_results:
                    cfr_data.update(entry.get("cfr_data", {}))
                    # cfr_insert.update(entry.get("cfr_reparse", {}))
                    # dirl_add.update(entry.get("dirl", {}))

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

                    # if cfr_insert:         new reparse points
                    #     key_ins =  []
                    #     for folder, data in cfr_insert.items():
                    #         key_ins.append(none_if_empty(data.get('modified_mtime')), folder, data.get('file_count'), data.get('max_depth'), none_if_empty(data.get('type')), none_if_empty(data.get('target')))

                    # if dirl_add:
                    # keys_to_delete = set(cfr_src) - set(dirl_add) # original - all hits    which is dirl_add

                    # key_rm = []
                    # for key in keys_to_delete: # remove stale entries from the cache
                    #     del cfr_src[key]
                    #     key_rm.append(key)

                    # del_keys = [(key,) for key in key_rm]

                    # update database
                    key_upt = []
                    for folder, data in cfr_data.items():
                        key_upt.append((
                            none_if_empty(data.get('modified_time')),
                            folder,
                            data.get('file_count'),
                            data.get('max_depth')
                        ))
                    # insert/update database
                    # del_keys is to remove db entries for deleted folders if wanting to maintain but no need
                    if syncdb(dbopt, basedir, CACHE_S, None, None, None, key_upt, from_idx=True):
                        nc_database = intst(dbopt, compLVL)
                        if encr(dbopt, dbtarget, email, nc_database, False):
                            nc_cfile = intst(CACHE_S, compLVL)
                            if encrm(ctarget, CACHE_S, email, nc_cfile, False):
                                print(f"Progress: {prog_v:.2f}%", flush=True)
                            else:
                                rlt = 1
                                print(f"Cache reencryption failed {CACHE_S} find_created dirwalker.py")
                        else:
                            rlt = 1
                    else:
                        rlt = 1
            except Exception as e:
                err_m = f'Unhandled exception in find_created during cache processing {type(e).__name__} {e}'
                print(err_m)
                logging.error(err_m, exc_info=True)

        # output results
        if all_sys:

            # 3 files used by find created. results file, database gpg (dbtarget) and a gpg file for exclusions
            output_file = f'{MODULENAME}xcreated.txt'
            local_gpg = os.path.join(appdata_local, "gpg", "gnupghome", "random_seed")

            # temp_dir = tempfile.mkdtemp()
            temp_f = os.path.join(tempdir, output_file)

            t = 0

            all_sys.sort(key=lambda x: x[1])

            with open(temp_f, "w", encoding="utf-8") as f:
                for entry in all_sys:
                    if len(entry) >= 2:
                        full_path = entry[0]

                        if full_path not in (temp_f, dbopt, local_gpg):
                            t += 1
                            mod_time = epoch_to_str(entry[1])
                            print(f'{mod_time} {full_path}', file=f)
                            print(full_path, mod_time)
            if t > 0:
                display(dspEDITOR, temp_f, True, dspPATH)
            else:
                print("No results or no new files found")
        else:
            print("No results or no new files found")

    if rlt == 0:
        print("Progress: 100.00", flush=True)
    return rlt

#  Build IDX system profile
#
# uses os.walk to first find the applicable files and then randomizes based on count. Then split and sent to workers
# to hash the system profile. A cache file ctimecache.gpg is made of all the directories on the system.
#
# System profile and cache file. or index drive for cache file
#
# 1 base_folders = get_base_folders() random.shuffle(base_folders). bad load balancing
#
# 2 get all directories randomize sort split. was found to be same and slower. Also bad load balancing.
# all_dirs = collect_dirs()
# all_dirs.sort(key=lambda x: x[1], reverse=True)
# chunks = split_dirs_for_workers(all_dirs, num_chunks)
# chunks = [ [dir_path for dir_path, _ in chunk] for chunk in chunks ]
# num_chunks = max(1, multiprocessing.cpu_count())
# chunks = split_dirs_for_workers(all_dirs, num_chunks)
#
# 3


def index_system(dbopt, dbtarget, basedir, updatehlinks, CACHE_S, email, ANALYTICSECT=False, idx_drive=False, cache_idx=True, compLVL=200, iqt=False, strt=0, endp=100):

    extension = config['diagnostics']['proteus_EXTN']
    extn_tuple = tuple(extension)
    EXCLDIRS_FULLPATH = [os.path.join(basedir, d) for d in EXCLDIRS]

    def create_new_index():

        print("Progress: 80.00%", flush=True)
        if dir_data:

            # flatten dict of dicts to flat tuples
            for fldr, key_meta in dir_data.items():
                parsedidx.append((
                    none_if_empty(key_meta.get('modified_time')),
                    fldr,
                    key_meta.get('file_count'),
                    key_meta.get('idx_count'),
                    key_meta.get('max_depth'),
                    none_if_empty(key_meta.get('type')),
                    none_if_empty(key_meta.get('target'))
                ))

            # encrypt the cache and then save in database
            return index_drive(dbopt, dbtarget, basedir, None, parsedidx, dir_data, CACHE_S, "Reencryption failed drive idxcache not saved.", email, idx_drive=idx_drive, compLVL=compLVL, dcr=False)

        return 1

    rlt = 0
    parsedidx = []
    dir_data = {}

    if idx_drive:
        endp = 60
    else:
        endp = 15

    st_tmn = time.time()
    all_files, dir_data, j = collect_files(cache_idx, basedir, EXCLDIRS_FULLPATH, extn_tuple, iqt, strt, endp)  # flat_files = [t[0] for t in all_files]
    e_val = time.time()
    el = e_val - st_tmn

    if all_files is None:
        print(f"An error occurred while initially indexing {basedir}.")
        return 1
    elif not all_files:
        print(f"No files found while searching {basedir}.")
        return 1

    if ANALYTICSECT:
        print(f'Cache indexing took {el:.3f} seconds')
        print(f'Total files during search: {j}')
        if dir_data:
            dr = len(dir_data)
            print(f'Directory count: {dr}')

    # if its a drive index get it and return early
    if idx_drive:
        res = create_new_index()
        if res == 0:
            print("Drive indexed")
            print("Progress: 100%", flush=True)
            return 0
        elif res == 4:
            return 52  # likely encryption failure database integrity is fine
        return res

    prog_v = endp

    parsedsys = []  # List for sys (file metadata)

    cprint.cyan('Running checksum.')

    special_k = -1

    if len(all_files) < 80 or modelTYPE.lower() == "hdd":
        if iqt:
            special_k = 0
        start = time.time()
        try:
            parsedsys = build_index(all_files, updatehlinks, appdata_local, ll_level, prog_v, 90, 0, special_k)
        except Exception as e:
            rlt = 1
            emsg = f"Error occurred index_system while building index serially: {type(e).__name__} : {e}"
            print(f"{emsg}\n{traceback.format_exc()}")
            logging.error(emsg, exc_info=True)

    else:

        chunks = chunk_split(all_files, batch_size=25, max_workers=8)
        num_chunks = len(chunks)

        sys_data = []

        if iqt:
            special_k = random.randint(0, num_chunks - 1)

        start = time.time()
        with ProcessPoolExecutor(max_workers=num_chunks) as executor:
            futures = [
                executor.submit(
                    build_index, chunk, updatehlinks, appdata_local, ll_level, prog_v, 90, i, special_k
                )
                for i, chunk in enumerate(chunks)
            ]

            for future in as_completed(futures):
                try:
                    sys_data = future.result()
                    if sys_data:
                        parsedsys.extend(sys_data)

                except Exception as e:
                    rlt = 1
                    emsg = f"Worker error occurred index_system: {type(e).__name__} : {e}"
                    print(f"{emsg}\n{traceback.format_exc()}")
                    logging.error(emsg, exc_info=True)
    end = time.time()
    if rlt == 0:

        # save system profile
        if parsedsys:
            if ANALYTICSECT:
                el = end - start
                print(f'Search took {el:.3f} seconds')

            if dir_data:

                # flatten
                for folder, data in dir_data.items():
                    parsedidx.append((none_if_empty(data.get('modified_time')), folder, data.get('file_count'), data.get('idx_count'), data.get('max_depth'), none_if_empty(data.get('type')), none_if_empty(data.get('target'))))
            else:
                if cache_idx:
                    print("No directories to cache. the cache file was empty")
            # store
            rlt = index_drive(dbopt, dbtarget, basedir, parsedsys, parsedidx, dir_data, CACHE_S, "Reencryption failed sys idxcache not saved.", email, idx_drive=False, compLVL=compLVL, dcr=False)  # save cache file and store in db
            if rlt == 0:
                print("Progress: 100%", flush=True)
                print("System profile complete")
            elif rlt == 4:
                rlt = 52
        else:
            rlt = 1
            print("Index failed to build. no results from multiprocessing.")

    gc.collect()
    return rlt

# Scan IDX
#
# calls scan_index above
# get the index from sys table recent.db and find differences


def scan_system(dbopt, dbtarget, basedir, difffile, updatehlinks, CACHE_S, email, ANALYTICSECT=True, showDiff=False, cache_idx=False, compLVL=200, dcr=False, iqt=False, strt=0, endp=100):

    if not os.path.isfile(dbopt):
        print(f"scan_system Unable to locate {dbopt}")
        return 1

    rlt = 0

    sys_tables, cache_table = get_idx_tables(basedir)
    recent_sys = db_sys_changes(dbopt, sys_tables)

    if recent_sys is None:  # an error occured
        print("\nThere was no return retrieving profile from db_sys_changes in scan_system indicating a problem. if having problems delete recent.gpg")
        return 1
    elif recent_sys is False:  # commandline use to tell recentchangessearch.py that does not exist or there is no profile yet
        return 7
    elif not recent_sys:  # empty query retrieve 0 rows
        print(f"No results querying {', '.join(sys_tables)} from db_sys_changes in scan_system")
        return 0

    print("Finding differences running checksum.", flush=True)

    special_k = -1

    all_sys = []  # file info or changes
    nfs_records = []  # appends or deletions to handle after loop
    x = 0
    y = 0

    if len(recent_sys) < 80 or modelTYPE.lower() == "hdd":

        if iqt:
            special_k = 0
        start = time.time()
        try:
            all_sys, nfs_records, x, y = scan_index(recent_sys, updatehlinks, appdata_local, ll_level, strt, 90, 0, special_k)

        except Exception as e:
            rlt = 1
            emsg = f"scan_system exception in scan_index while scanning serially: {type(e).__name__} {e}"
            print(f"{emsg} \n{traceback.format_exc()}")
            logging.error(emsg, exc_info=True)

    else:

        chunks = chunk_split(recent_sys, batch_size=500, max_workers=8)
        num_chunks = len(chunks)

        # num_chunks = min(8, multiprocessing.cpu_count() or 1) without batch size
        # total_items = len(recent_sys)
        # chunk_size = math.ceil(total_items / num_chunks)

        # chunks = [
        #     recent_sys[i:i + chunk_size]
        #     for i in range(0, total_items, chunk_size)
        # ]

        if iqt:
            special_k = random.randint(0, num_chunks - 1)
        start = time.time()

        with ProcessPoolExecutor(max_workers=num_chunks) as executor:

            futures = [

                executor.submit(

                    scan_index, chunk, updatehlinks, appdata_local, ll_level, strt, 90,
                    i, special_k
                )

                for i, chunk in enumerate(chunks)

            ]

            for future in as_completed(futures):

                try:
                    sys_data, results, x_c, y_c = future.result()
                    if sys_data:
                        all_sys.extend(sys_data)
                    if results:
                        nfs_records.extend(results)
                    x += x_c
                    y += y_c
                except Exception as e:
                    rlt = 1
                    emsg = f"scan_system Worker error: {type(e).__name__} {e}"
                    print(f"{emsg} \n{traceback.format_exc()}")
                    logging.error(emsg, exc_info=True)
    end = time.time()
    if rlt == 0:

        cmsg = ""

        if x != 0:
            p = (y / x) * 100
            if p > 30:
                cmsg = f"\nThe sys index had over 30% miss rate recommend rebuild index: {p:.2f}%"

        # output terminal
        if all_sys:
            if ANALYTICSECT:
                el = end - start
                print(f'Search took {el:.3f} seconds')

            hdr1 = 'System index scan'
            hdr2 = "The following files from sys index have changes by checksum\n"
            fstr = "timestamp,filename,creationtime,inode,accesstime,checksum,filesize,symlink,owner,domain,mode,casmod,lastmodified,hardlinks,count"

            # symmetric differences  show directories that had 0 files at indexing but now have files
            dir_diff = []
            new_diff = []
            if iqt and showDiff and cache_idx:
                systimeche = getnm(CACHE_S)
                dir_diff, new_diff = find_symmetrics(dbopt, cache_table, systimeche)

            recent_files = []
            for record in all_sys:
                record_str = ' '.join(map(str, record))
                recent_files.append(record_str)

            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Insert changes
            mode = 'a' if os.path.isfile(difffile) else 'w'
            if save_db(dbopt, dbtarget, basedir, CACHE_S, email, None, None, all_sys, keys=None, idx_drive=False, compLVL=compLVL, dcr=dcr):

                # output at bottom diff file
                if len(recent_files) > 0:
                    with open(difffile, mode) as f:
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

                        if iqt and showDiff:

                            if nfs_records:
                                header = "following profile files no longer exist"
                                f.write("\n")
                                print(header, file=f)
                                for tup in nfs_records:
                                    tup_str = " ".join(map(str, tup))
                                    f.write(tup_str + "\n")

                            if cache_idx:
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

                            print(current_time, file=f)

                    write_type = "appended" if mode == 'a' else "written"

                    print(f"\n Changes {write_type} to difference file {difffile}")
                    if showDiff and (nfs_records or dir_diff):
                        print("Differences included")
                    elif showDiff:
                        print("No symmetric differences found.")
            else:
                print(f"Failed to insert profile changes into {sys_tables[1]} table in scan_system")
        else:
            print('No results found for sys index scan system multiprocessing.')
    else:
        print("Scan index failed scan_system dirwalker.py.")

    if rlt == 0:
        print(f"Progress: {endp}%", flush=True)
    gc.collect()
    return rlt


def main_entry(argv):
    parser = build_dwalk_parser()
    args = parser.parse_args(argv)

    if args.action == "scan":
        calling_args = [
            args.dbopt, args.dbtarget, args.basedir, args.difffile, args.updatehlinks, args.CACHE_S,
            args.email, args.ANALYTICSECT, args.showDiff, args.cache_idx, args.compLVL, args.dcr,
            args.iqt, args.strt, args.endp
        ]
        sys.exit(scan_system(*calling_args))

    elif args.action == "build":
        calling_args = [
            args.dbopt, args.dbtarget, args.basedir, args.updatehlinks, args.CACHE_S, args.email,
            args.ANALYTICSECT, args.idx_drive, args.cache_idx, args.compLVL, args.iqt,
            args.strt, args.endp
        ]
        sys.exit(index_system(*calling_args))

    elif args.action == "downloads":
        calling_args = [
            args.dbopt, args.dbtarget, args.basedir, args.mdltype, args.tempdir, args.CACHE_S,
            args.dspEDITOR, args.dspPATH, args.email, args.ANALYTICSECT, args.compLVL
        ]
        sys.exit(find_created(*calling_args))


# if __name__ == "__main__":
#     main_entry(sys.argv[1:])
