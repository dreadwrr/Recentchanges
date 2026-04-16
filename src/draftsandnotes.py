# os.walk files search

# qtdrivefunctions read&write test

# store pandas for mft parsing
# notes left in here as if removed from project often disapear
# calibrate search using Mft

def find_mft_mftecmd(RECENT, COMPLETE, init, cfr, search_start_dt, user_setting, logging_values, end, cstart, search_time, iqt=False, strt=20, endp=60):

    p = search_time * 60

    # compt = (datetime.now(timezone.utc) - timedelta(seconds=p))
    compt = search_start_dt.astimezone(timezone.utc)

    delta_value = (endp - strt)
    endval = strt + (delta_value / 2)
    logger = logging.getLogger("search_Mft")

    exec_path = logging_values[2] / "bin" / "MFTECmd.exe"

    csv_data = read_mftmem(str(exec_path), 'C:\\$MFT', compt, search_start_dt, iqt, strt, endval)  # search
    if csv_data is None:
        print("Error read Mft data in IOString from MFTECmd.exe. exiting.")
        sys.exit(1)
    if len(csv_data.getvalue()) == 0:
        print("No files returned from read_mftmem from reading Mft. exiting.")
        sys.exit(1)

    prog_v = endval

    file_entries = search_Mft(csv_data, compt, logger)  # convert csv to list of tuples
    end = time.time()
    if not file_entries:
        print(f"No new files from results of search in Mft search time {p} seconds. exiting")
        sys.exit(1)

    if user_setting["FEEDBACK"]:
        for entry in file_entries:
            if len(entry) >= 11:
                file_path = entry[10]
                print(file_path, flush=True)
    if init and user_setting["checksum"]:
        cprint.cyan('\nRunning checksum.')
        cstart = time.time()

    filetype = None
    RECENT, COMPLETE = process_lines(process_mft, file_entries, filetype, search_start_dt, "FSEARCHMFT", user_setting, logging_values, cfr, iqt, prog_v, endp)  # multiprocess
    return RECENT, COMPLETE, end, cstart


"""
 Read a parsed mft csv into pandas to diff and process. Return list for recentchangessearch
 MFTECmd
"""


def search_Mft(csv_p, compt, logger, iqt=False):  # tmn  csv            dec13/2025

    time_field = "LastModified0x10"
    ctime_field = "Created0x10"
    atime_field = "LastAccess0x10"
    lmtime_field = "LastModified"

    bool_columns = ["InUse", "IsDirectory", "IsAds"]

    local_tz = datetime.now().astimezone().tzinfo

    columns = [
        "EntryNumber", "SequenceNumber", "InUse", "ParentEntryNumber", "ParentSequenceNumber", "ParentPath", "FileName", "Extension",
        "FileSize", "ReferenceCount", "ReparseTarget", "IsDirectory", "HasAds", "IsAds", "SI<FN", "uSecZeros", "Copied", "SiFlags", "NameType",
        "Created0x10", "Created0x30", "LastModified0x10", "LastModified0x30", "LastRecordChange0x10", "LastRecordChange0x30", "LastAccess0x10",
        "LastAccess0x30", "UpdateSequenceNumber", "LogfileSequenceNumber", "SecurityId", "ObjectIdFileDroid", "LoggedUtilStream", "ZoneIdContents",
        "SourceFile"
    ]
    try:

        csv_p.seek(0)
        df = pd.read_csv(
            csv_p, names=columns,
            low_memory=False,
            converters={col: str_to_bool for col in bool_columns}
        )

        dt_cols = [time_field, ctime_field, atime_field]

        for col in dt_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize('UTC')

        df = df.dropna(subset=[time_field])

        recent_files = df[
            (df['InUse']) &
            (~df['IsDirectory']) &
            (~df['IsAds']) &
            ((df[time_field] >= compt) | (df[ctime_field] >= compt))
        ].copy()

        recent_files['cam'] = None
        mask_cam = recent_files[ctime_field] > recent_files[time_field]
        recent_files.loc[mask_cam, 'cam'] = 'y'

        recent_files['LastModified'] = None
        recent_files.loc[mask_cam, 'LastModified'] = recent_files.loc[mask_cam, time_field]

        original_mtime = recent_files.loc[mask_cam, time_field].copy()
        recent_files.loc[mask_cam, time_field] = recent_files.loc[mask_cam, ctime_field]
        recent_files.loc[mask_cam, ctime_field] = original_mtime

        recent_files["mtime_us"] = (recent_files[time_field].astype("int64") // 1_000).astype("int64")

        seq = pd.to_numeric(recent_files["SequenceNumber"], errors="coerce")
        ent = pd.to_numeric(recent_files["EntryNumber"], errors="coerce")
        mask = seq.notna() & ent.notna()
        recent_files = recent_files.loc[mask].copy()

        recent_files["inode"] = (
            seq[mask].astype(object) * (1 << 48) + ent[mask].astype(object)
        )

        recent_files = recent_files.dropna(subset=['ParentPath', 'FileName'])
        recent_files = build_mftec_path(recent_files)

        for col in dt_cols + [lmtime_field]:
            recent_files[col] = pd.to_datetime(recent_files[col], errors="coerce", utc=True).dt.tz_convert(local_tz)

        recent_files[time_field] = (
            recent_files[time_field].dt.tz_convert(local_tz).dt.tz_localize(None).to_numpy(dtype="datetime64[us]").astype(object)
        )

        for col in (ctime_field, atime_field, lmtime_field):
            recent_files[col] = recent_files[col].dt.strftime("%Y-%m-%d %H:%M:%S")
            recent_files[col] = recent_files[col].where(recent_files[col].notna(), None)

        recent_files['ReferenceCount'] = pd.to_numeric(recent_files['ReferenceCount'], errors='coerce')
        recent_files["FileSize"] = pd.to_numeric(recent_files["FileSize"], errors="coerce").map(
            lambda v: None if pd.isna(v) else int(v)
        )

        remaining_col = [
            "mtime_us", "FileSize", "SiFlags", "ReferenceCount", "inode", "cam", "FullPath"
        ]

        for col in remaining_col:
            recent_files[col] = recent_files[col].astype(object).where(pd.notna(recent_files[col]), None)

        result = list(zip(
            recent_files[time_field],
            recent_files['mtime_us'],
            recent_files[ctime_field],
            recent_files[atime_field],
            recent_files['FileSize'],
            recent_files['LastModified'],
            recent_files['SiFlags'],
            recent_files['ReferenceCount'],
            recent_files['inode'],
            recent_files['cam'],
            recent_files['FullPath']
        ))
        return result
    except Exception as e:
        print("Error reading converting data frame to tuple list search_Mft rntchangesfunctions. quitting")
        print(f"Error processing MFT data in search_Mft func rntchangesfunctions.py: {type(e).__name__} :{e}")
        logger.error(f"Error processing MFT data: {type(e).__name__} {e}", exc_info=True)
        sys.exit(1)


def output_results_exit(RECENT, argone, is_calibrate, iswsl, fmt):
    """ calibrate powershell and find command and see what the mft says """
    file_nm = f"PwshOutput{argone}.txt"
    if is_calibrate:
        file_nm = f"MftOutput{argone}.txt"
    elif iswsl:
        file_nm = f"WSLOutput{argone}.txt"

    flnm_frm, ext = os.path.splitext(file_nm)
    outpath = flnm_frm + "_sample" + ext
    i = 1
    while os.path.exists(outpath):
        outpath = f"{flnm_frm}_sample_{i}{ext}"
        i += 1

    with open(outpath, 'w') as f:
        for entry in RECENT:
            if entry[1].startswith("C:\\Windows"):
                continue
            tss = entry[0].strftime(fmt)
            fp = entry[1]
            f.write(f'{tss} {fp}\n')
    print("\n Sample output complete:", outpath)
    sys.exit(1)


# os.walk version of above
#
# was found to be 2x slower than os.scandir
# def files_search(base_dir, search_start_dt, FEEDBACK, EXCLDIRS, logger, iqt=False, strt=0, endp=100):

#     all_entries = []

#     x = None
#     y = 0
#     delta_val = 0

#     cutoff = search_start_dt.timestamp()

#     EXCLDIRS_FULLPATH = set(os.path.join(base_dir, d) for d in EXCLDIRS)  # EXCLDIRS_FULLPATH = os.path.join(basedir, entry.lstrip("\\/"))

#     try:

#         if iqt:

#             ## top level directories for progress counting
#             dir_list = []
#             for d in os.listdir(base_dir):
#                 full = os.path.join(base_dir, d)
#                 if os.path.isdir(full) and full not in EXCLDIRS_FULLPATH:
#                     dir_list.append(full)

#             dir_set = set(dir_list)
#             x = len(dir_set)
#             delta_val = endp - strt

#         ## verification logging os.walk err handler
#         handler = ErrorHandler(logger)

#         for dirpath, dirnames, filenames in os.walk(base_dir, followlinks=False, onerror=handler):
#             if dirpath in EXCLDIRS_FULLPATH:
#                 dirnames[:] = []  # prevent descending into subfolders
#                 continue

#             if x and dirpath in dir_set:
#                 y += 1
#                 dir_set.discard(dirpath)
#                 prog_v = (y / x) * delta_val + strt
#                 if prog_v >= endp:
#                     prog_v = endp
#                     x = None
#                 print(f"Progress: {prog_v:.2f}%", flush=True)

#             for filename in filenames:
#                 full_path = os.path.join(dirpath, filename)
#                 st = goahead(full_path, logger=logger)
#                 if st == "Nosuchfile":
#                     continue
#                 elif st:
#                     mtime = st.st_mtime
#                     c_time = st.st_birthtime
#                     atime = st.st_atime

#                     if (mtime >= cutoff or c_time >= cutoff):
#                         if FEEDBACK:
#                             print(full_path)
#                         all_entries.append((str(mtime), str(atime), full_path))  # mod_time, access_time, _, ino, symlink, hardlink, size, _, _, _, file_path = line  # changetime, owner, domain, mode
#                 else:
#                     logger.debug(f"Skipping couldnt stat in scan_files file: {full_path} \n")
#                     continue

#     except Exception as e:
#         emsg = f"scan_files Exception: {type(e).__name__} {e} \n{traceback.format_exc()}"
#         print(emsg)
#         logger.error(f"{emsg}", exc_info=True)
#         return None

#     return all_entries


# if mmode and speedMB:
#     if mmode == "read":
#         set_json_settings({"read_speed": speedMB}, drive=basedir, filepath=user_json)
#     elif mmode == "write":
#         set_json_settings({"write_speed": speedMB}, drive=basedir, filepath=user_json)

# print("Running speed test")
# if dtype == "SSD":
#     drive_type = dtype
# elif dtype is None:
#     mmode = "read"
#     speedMB = measure_read_speed(basedir)

#     if speedMB is None:
#         mmode = "write"
#         if lclapp_data and not idx_drive:
#             target_path = lclapp_data
#         else:
#             target_path = basedir
#         speedMB = measure_write_speed(basedir, target_path, WRITE_MB=200)

#     if speedMB is None:
#         mmode = None
#         print("Couldnt determine speed of drive defaulting to HDD for serial fsearch and ha")
#     elif speedMB > 300:
#         drive_type = "SSD"

# def collect_files(root, min_size_mb=100):
#     skip_dirs = {"appdata", "windows"}
#     file_list = []
#     for dirpath, dirnames, filenames in os.walk(root, followlinks=False, onerror=os_walk_error):
#         dirnames[:] = [d for d in dirnames if d.lower() not in skip_dirs]
#         for name in filenames:
#             path = os.path.join(dirpath, name)
#             try:
#                 if os.path.isfile(path) and os.path.getsize(path) >= min_size_mb*1024*1024:
#                     file_list.append(path)
#             except Exception:
#                 pass
#     return file_list


# def measure_read_speed(root_dir="C:\\", target_gb=1):
#     TARGET_BYTES = target_gb * 1024 * 1024 * 1024
#     BLOCK_SIZE = 1024 * 1024
#     files = collect_files(root_dir)
#     if not files:
#         print("No large files found to read for speed test")
#         return None
#     total_read = 0
#     start = time.time()
#     for file_path in random.sample(files, len(files)):  # for file_path in files:
#         if total_read >= TARGET_BYTES:
#             break
#         try:
#             with open(file_path, "rb", buffering=0) as f:
#                 remaining = TARGET_BYTES - total_read
#                 while remaining > 0:
#                     chunk_size = min(BLOCK_SIZE, remaining)
#                     chunk = f.read(chunk_size)
#                     if not chunk:
#                         break
#                     total_read += len(chunk)
#                     remaining -= len(chunk)
#         except Exception:
#             pass
#     end = time.time()
#     elapsed = end - start
#     if elapsed == 0:
#         print("Elapsed time is zero; cannot measure read speed.")
#         return None
#     total_mb = total_read / (1024 * 1024)
#     speedMB = total_mb / elapsed
#     print(f"Read {total_mb:.2f} MB in {elapsed:.2f} seconds")
#     print(f"Average speed: {speedMB:.1f} MB/s")
#     return speedMB


# def measure_write_speed(ROOT_DIR="C:\\", target_path="C:\\", WRITE_MB=200):

#     def write_file(write_path):
#         with open(write_path, "wb") as f:
#             f.write(b"A" * WRITE_MB*1024*1024)
#             f.flush()
#             os.fsync(f.fileno())

#     try:
#         drive_info = get_disk_and_volume_for_drive(ROOT_DIR)
#         if drive_info:
#             device_id, _ = drive_info

#             pd = f"PhysicalDrive{device_id}"
#             io1 = psutil.disk_io_counters(perdisk=True)[pd]
#             start = time.time()
#             write_path = os.path.join(target_path, "speedtest.bin")
#             write_file(write_path)
#             end = time.time()
#             io2 = psutil.disk_io_counters(perdisk=True)[pd]

#             written_bytes = io2.write_bytes - io1.write_bytes

#             speed_MBps = written_bytes / (1024*1024) / (end - start)

#         else:  # could be a virtual disk or ram drive and not listed. Its path was verified earlier

#             write_path = os.path.join(target_path, "speedtest.bin")
#             start = time.time()
#             write_file(write_path)
#             end = time.time()

#             speed_MBps = WRITE_MB / (end - start)
#         print(f"Write speed: {speed_MBps:.2f} MB/s")
#         removefile(write_path)
#         return speed_MBps
#     except (RuntimeError, ValueError, TypeError):
#         return None
