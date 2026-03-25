import os
import sqlite3
import wmi
import traceback
from .config import get_json_settings
from .config import dump_j_settings
from .config import set_json_settings
from .config import update_dict
from .config import update_toml_values
from .gpgcrypto import decr
from .gpgcrypto import encr
from .pysql import clear_conn
from .pysql import table_exists
from .pyfunctions import cnc
from .rntchangesfunctions import name_of
from .rntchangesfunctions import removefile


def parse_drive(basedir):
    return basedir.split(":", 1)[0].lower()


def parse_key(basedir, cache_file=None, idx_suffix=None):
    if idx_suffix:
        return idx_suffix
    elif cache_file:
        if "_" in cache_file:
            part = name_of(cache_file)
            return part.split("_", 1)[-1]
    return parse_drive(basedir)


def parse_systimeche(basedir, CACHE_S):
    """ get systimeche table from actual cache file. just in one less step"""
    systimeche = name_of(CACHE_S)
    key = "c"
    if basedir != "C:\\":
        if "_" not in systimeche:
            raise TypeError("idx_suffix requires for drive", basedir)
        _, key = systimeche.split("_", 1)
    return systimeche, key


# c:\ has systimeche.gpg, systimeche table
# any other has systimeche_n.gpg, systimeche_n table
def get_cache_s(basedir, cache_file, idx_suffix=None):
    """ initial setup """
    # C:\\ has systimeche.gpg for CACHE_S and systimeche for cache table
    # for S:\\ systimeche_s.gpg for CACHE_S and systimeche_s table for cache table
    prefix = name_of(cache_file)
    CACHE_S = cache_file
    systimeche = prefix
    key = "c"
    if basedir != "C:\\":
        key = parse_key(basedir, cache_file, idx_suffix)
        CACHE_S = prefix + f"_{key}.gpg"
        app_path = os.path.dirname(cache_file)
        CACHE_S = os.path.join(app_path, CACHE_S)
        systimeche = prefix + f"_{key}"
    return CACHE_S, systimeche, key

# cache_s/cache_s2/cache_n table has the directory
# structure at the time of the system profile
#
# c:\ has sys, cache_s
# any other has sys_n, sys2_n and cache_n
#
# eg drive is s:\ its sys_s, sys2_s and cache_s2
# eg drive r:\ sys_r, sys2_r, cache_r


def get_idx_tables(basedir, cache_file=None, idx_suffix=None):
    """ pass actual cache_file or key """
    # returns profile sys_a , changes sys_b, profile cache table
    # get the key from actual cache file
    sys_a = ""
    cache_table = "cache_s"
    key = "c"
    if basedir != "C:\\":
        delim = ""
        key = parse_key(basedir, cache_file, idx_suffix)
        if key == "s":
            delim = "2"
        sys_a = f"_{key}"
        cache_table = f"cache_{key}{delim}"
    sys_b = "sys2" + sys_a
    sys_a = "sys" + sys_a
    return (sys_a, sys_b), cache_table, key


def get_mount_partguid(basedir: str) -> str | None:
    drive = basedir.rsplit("\\", 1)[0]  # "C:\\" -> "C:"
    try:
        c = wmi.WMI()
        vols = c.Win32_Volume(DriveLetter=drive)
        if not vols:
            return None
        device_id = vols[0].DeviceID  # \\?\Volume{guid}\
        parts = device_id.split("Volume", 1)
        guid = parts[1].strip("\\") if len(parts) > 1 else ""
        return guid or None
    except Exception:
        return None


def get_drive_from_partguid(partguid: str) -> str | None:
    """
    partguid examples accepted:
    - "{xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx}"
    - "Volume{xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx}"
    - "\\\\?\\Volume{xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx}\\"
    Returns drive like "C:\\" or None.
    """
    # g = (partguid or "").strip("\\").lower()
    # if "Volume" in g:
    #     g = g.split("Volume", 1)[1]
    # if not g.startswith("{"):
    #     g = "{" + g + "}"
    # if not g.endswith("}"):
    #     g = g + "}"
    g = partguid
    device_id = r"\\?\Volume" + g + "\\"
    try:
        c = wmi.WMI()
        vols = c.query(f"SELECT * FROM Win32_Volume WHERE DeviceID = '{device_id}'")
        # for p in c.Win32_Volume.properties:
        #     print(p)
        # for v in c.Win32_Volume():
        #     print(repr(v.DeviceID))
        if not vols:
            return None

        drive = vols[0].DriveLetter
        return f"{drive}\\" if drive else None
    except Exception as e:
        print(e)
        return None


# def get_mount_partguid(basedir: str) -> str | None:
#     # drive = parse_drive(basedir)
#     # drive = drive.upper() + ":"
#     drive = basedir.rsplit("\\", 1)[0]
#     cmd = f"(Get-CimInstance Win32_Volume -Filter \"DriveLetter = '{drive}'\").DeviceID"
#     try:
#         volume_guid = subprocess.check_output(
#             ["powershell", "-NoProfile", "-Command", cmd],
#             text=True
#         ).strip()
#     except subprocess.SubprocessError:
#         return None
#     parts = volume_guid.split("Volume", 1)
#     guid = parts[1].strip("\\") if len(parts) > 1 else ""
#     return guid or None


# def get_drive_from_partguid(part_guid: str):
#     g = part_guid.strip().lower()
#     if not g.startswith("{"):
#         g = "{" + g + "}"
#     if not g.endswith("}"):
#         g = g + "}"

#     c = wmi.WMI()
#     for v in c.Win32_Volume():
#         dev = (v.DeviceID or "").lower()
#         if g in dev:
#             return v.DriveLetter + "\\"
#     return None


def get_drive_type(basedir, driveTYPE, CACHE_S, json_file):
    # _, suffix = parse_systimeche(basedir, CACHE_S)
    di = get_json_settings(None, basedir, json_file) or {}
    dtype = di.get("drive_type")
    if dtype in ("HDD", "SSD"):
        return dtype
    else:
        print("Warning entry for", basedir, "is malformed in json file:", json_file, "using default", driveTYPE)
    return driveTYPE


def is_model_ssd(model: str) -> bool:
    SSD_KEYWORDS = [
        "SSD", "NVME", "NVM", "M.2", "EVO",
        "SOLID", "FLASH", "V-NAND", "3D NAND"
    ]
    if not model:
        return False
    m = model.upper()
    return any(keyword in m for keyword in SSD_KEYWORDS)


def current_drive_type_model_check(ROOT_DIR="C:\\"):
    try:
        drive_id_model = "Unknown"
        model_type = "Unknown"
        drive_type = None

        drive_letter = parse_drive(ROOT_DIR).upper()
        drive = drive_letter + ":"

        c = wmi.WMI()

        partitions = c.query(
            f"ASSOCIATORS OF {{Win32_LogicalDisk.DeviceID='{drive}'}} "
            "WHERE AssocClass = Win32_LogicalDiskToPartition"
        )
        if not partitions:
            return "Ramdisk", model_type, "SSD"  # Ram Disk as not listed as logical drives

        logical = c.Win32_LogicalDisk(DeviceID=drive)[0]
        if not logical:
            return None

        partition = partitions[0]
        # disk_number = partition.DiskIndex
        # volume_number = partition.Index

        # print("disk_number, volume_number", disk_number, volume_number)
        disk = partition.associators("Win32_DiskDriveToDiskPartition")[0]
        if not disk:
            return None

        if disk.Model:
            drive_id_model = disk.Model  # name

        info = {
            "device_name": disk.Name,
            "interface": disk.InterfaceType,
            "pnp_id": disk.PNPDeviceID,
            "serial": getattr(disk, "SerialNumber", None),
            "manufacturer": getattr(disk, "Manufacturer", None),
            "media_type": (disk.MediaType or "").lower()
        }
        # for key, value in info.items():
        #     print(f"{key}: {value}")
        if info["manufacturer"]:
            model_type = info["manufacturer"]

        # disks = c.Win32_DiskDrive()
        # disk_full = next((d for d in disks if d.DeviceID == disk.DeviceID), None)
        # if disk_full is None:
        #     print("Disk not found!")
        # else:
        #     for prop in disk_full.properties:
        #         print(f"{prop}: {getattr(disk_full, prop)}")

        # see if its an SSD possibly newer hard disk with RotationRate in wmi

        if is_model_ssd(disk.Model) or is_model_ssd(info["pnp_id"]):
            drive_type = "SSD"
        pnp = (info["pnp_id"] or "").lower()
        iface = (info["interface"] or "").lower()
        is_usb = iface == "usb" or "usb" in pnp or "usbstor" in pnp
        # if "usb" in media_type.lower():
        if is_usb:
            drive_type = "SSD"
            model_type = "USB"

        if drive_type != "SSD":
            if hasattr(disk, "RotationRate"):
                rotation = disk.RotationRate
                if rotation == 0:
                    drive_type = "SSD"
                elif rotation is not None:
                    drive_type = "HDD"
            else:
                print("Tried to use RotationRate and not available, fallback needed")

    except Exception:
        return None
    return drive_id_model, model_type, drive_type


# check by model type, pnp description or rotation. if not run read test fall back to write test. if all fails set to HDD.
# user can set in config file config.toml for basedir. user can set in usrprofile.toml for index drive.
# Newer HDD drives have RotationRate in wmi. Older or legacy drives do not.
def setup_drive_settings(basedir, key, driveTYPE, toml_file, user_json=None, j_settings=None, idx_drive=False, lclapp_data=None):

    if driveTYPE:
        return driveTYPE

    print("Determining drive type by model or speed test")
    drive_info = current_drive_type_model_check(basedir)
    if not drive_info:
        return None

    drive_id_model, model_type, drive_type = drive_info
    if drive_type is None:
        print("Couldnt determine speed defaulting to HDD. change in config.toml to SSD", toml_file)
        drive_type = "HDD"

    if basedir == "C:\\" and toml_file and not idx_drive:
        update_toml_values({'search': {'driveTYPE': drive_type}}, toml_file)  # update config.toml the basedir

    # config.toml is where basedir ie C:\\ info is stored. the 'modelTYPE' HDD or SSD
    # if its a basedir we only want to put the info in the usrprofile.toml if we have it. This is used for diagnostics to return more info about settings in ui.
    # if we were to put the wrong info in usrprofile.toml and config.toml the user would have to update two config files which is unlikely.
    #
    # if its an idx_drive we need this info regardless as usrprofile.toml is where its info is stored. 'drive_type' and 'drive_model'
    if user_json:
        if idx_drive or model_type:
            if model_type is None:
                model_type = "Unknown"
            if key and j_settings is not None:

                update_dict({"idx_suffix": key, "drive_id_model": drive_id_model, "mount_of_index": basedir, "model_type": model_type, "drive_type": drive_type}, j_settings, basedir)
                dump_j_settings(j_settings, user_json)
            elif key:
                set_json_settings({"idx_suffix": key, "drive_id_model": drive_id_model, "mount_of_index": basedir, "model_type": model_type, "drive_type": drive_type}, drive=basedir, filepath=user_json)

    print(f"model {drive_id_model}")
    print(f"model_type {model_type}")
    print(f"drive_type {drive_type}")

    return drive_type


def get_cache_files(basedir, dbopt, dbtarget, CACHE_S, json_file, user, email, compLVL, j_settings=None, partguid=None, iqt=False):

    suffix = "c"
    cache_file = systimeche = None

    # qt gui initial load json
    # this avoids loading json unnecessarily for commandline if basedir is "/"
    # which is what it would be set to m ost of the time

    if iqt:
        if isinstance(j_settings, dict) and not j_settings:  # iqt
            jdata = get_json_settings(None, None, json_file)
            j_settings.update(jdata)

    if basedir != "C:\\":

        # command line
        if not iqt:
            if j_settings is None:
                j_settings = get_json_settings(None, None, json_file)  # original left for legacy
            elif not j_settings:
                jdata = get_json_settings(None, None, json_file)
                j_settings.update(jdata)

        if not os.path.exists(basedir):
            print("setup_drive_setting setting drive:", basedir)
            print("unable to find drive")
            return None, None, None

        try:
            guid = partguid
            if not partguid:
                guid = get_mount_partguid(basedir)
                if not guid:
                    print(f"couldnt find guid for {basedir} mount point")
                    return None, None, None

            drive_suffix = parse_drive(basedir)  # basedir.split('/')[-1]

            x = 0
            drive = suffix = drive_info = None

            found = False
            for key, di in j_settings.items():
                if not isinstance(di, dict):
                    continue
                drive_partguid = di.get("drive_partguid")
                if not found and drive_partguid and drive_partguid == guid:
                    drive = key
                    suffix = di.get("idx_suffix")
                    drive_info = di

                    found = True

                elif isinstance(key, str) and basedir == key:  # key.endswith(drive_suffix): . linux uses device name windows drive ltter
                    x += 1

            if suffix:

                cache_file, systimeche, _ = get_cache_s(basedir, CACHE_S, suffix)

                # if the mountpoint changed for the guid update json, move cache file and db tables
                #
                if drive_suffix != suffix:

                    # old
                    old_cache_s = cache_file

                    # new
                    # drive_suffix = ('x' * x) + drive_suffix
                    new_cache_s, new_systimeche, _ = get_cache_s(basedir, CACHE_S, drive_suffix)

                    # rename any cache file. after database query

                    # if from cmd line get db
                    if not os.path.isfile(dbopt):
                        if os.path.isfile(dbtarget):
                            res = decr(dbtarget, dbopt)
                            if not res:
                                if res is None:
                                    print(f"There is no key for {dbtarget}.")
                                else:
                                    print("Decryption failed.")

                    # rename any database tables
                    if os.path.isfile(dbopt):
                        sys_tables, cache_table, _ = get_idx_tables(basedir, None, suffix)
                        sys_a, sys_b = sys_tables
                        sys_tables, cache_table2, _ = get_idx_tables(basedir, None, drive_suffix)
                        sys_a2, sys_b2 = sys_tables
                        table_list = [
                            (sys_a, sys_a2),
                            (sys_b, sys_b2),
                            (cache_table, cache_table2),
                            (systimeche, new_systimeche)
                        ]
                        conn = cur = None
                        try:
                            if drive_info:
                                moi = drive_info.get("mount_of_index")
                                if moi:

                                    conn = sqlite3.connect(dbopt)
                                    cur = conn.cursor()

                                    for table in table_list:
                                        table_name = table[0]
                                        if table_exists(conn, table_name):
                                            cur.execute(f"""
                                                UPDATE {table_name}
                                                SET filename = REPLACE(filename, ?, ?)
                                                WHERE filename LIKE ?;
                                            """, (moi, basedir, moi + "%"))
                                            cur.execute(f"""
                                                UPDATE {table_name}
                                                SET target = REPLACE(target, ?, ?)
                                                WHERE target LIKE ?;
                                            """, (moi, basedir, moi + "%"))

                                    for old_table, new_table in table_list:
                                        if table_exists(conn, old_table):
                                            cur.execute(f"ALTER TABLE {old_table} RENAME TO {new_table};")
                                    conn.commit()
                                    cur.close()
                                    conn.close()
                                    cur = conn = None

                                    nc = cnc(dbopt, compLVL)
                                    if encr(dbopt, dbtarget, email, no_compression=nc, dcr=iqt):  # leave open for gui
                                        # rename any cache file
                                        if os.path.isfile(old_cache_s):
                                            os.rename(old_cache_s, new_cache_s)
                                        update_dict(None, j_settings, drive)  # remove the old
                                    else:
                                        removefile(dbopt)
                                        print(f"Reencryption failed on updating guid for drive {basedir}.\n")
                                        print("If unable to resolve reset json file and clear gpgs")

                        except sqlite3.Error as e:
                            if conn:
                                conn.rollback()
                            removefile(dbopt)
                            print(f"Database error get_cache_files while moving tables db {dbopt} err: {e}")
                        except Exception as e:
                            removefile(dbopt)
                            print(f"err {type(e).__name__}: {e}\ncontinuing")
                        finally:
                            clear_conn(conn, cur)

                    drive_info["mount_of_index"] = basedir
                    drive_info["idx_suffix"] = drive_suffix
                    j_settings[basedir] = drive_info  # add the new now that nothing went wrong
                    dump_j_settings(j_settings, json_file)

                    suffix = drive_suffix
                    cache_file = new_cache_s
                    systimeche = new_systimeche

            else:
                if x > 0:
                    update_dict(None, j_settings, basedir)  # remove any existing
                update_dict({"drive_partguid": guid}, j_settings, basedir)
                dump_j_settings(j_settings, json_file)
                suffix = drive_suffix

        except Exception as e:
            print(f"Error getting cache files for drive {basedir} err: {type(e).__name__} {e} \n{traceback.format_exc()}")
            return None, None, None

    if not cache_file:
        cache_file, systimeche, _ = get_cache_s(basedir, CACHE_S, suffix)

    return cache_file, systimeche, suffix


def setup_drive_cache(basedir, appdata_local, dbopt, dbtarget, json_file, toml_file, CACHE_S, driveTYPE, USR, email, compLVL, j_settings=None, partguid=None, iqt=False):

    if driveTYPE:
        if driveTYPE.lower() not in ('hdd', 'ssd'):
            print(f"Incorrect setting driveTYPE: {driveTYPE} in config: {toml_file}")
            return None, None, None, None

    CACHE_S, systimeche, suffix = get_cache_files(basedir, dbopt, dbtarget, CACHE_S, json_file, USR, email, compLVL, j_settings, partguid, iqt)  # confirm the guid and build the CACHE_S and suffix
    if not suffix:
        return None, None, None, None

    if driveTYPE in ("HDD", "SSD"):
        return CACHE_S, systimeche, suffix, driveTYPE

    if j_settings:
        drive = j_settings.get(basedir, {})
        dt = drive.get("drive_type")
        if dt in ("HDD", "SSD"):
            return CACHE_S, systimeche, suffix, dt
        if dt:
            print("Malformed json defaulting to HDD for drive", basedir, "in json:", json_file)
            j_settings[basedir]["drive_type"] = "HDD"
            dump_j_settings(j_settings, json_file)
            return CACHE_S, systimeche, suffix, "HDD"

    driveTYPE = setup_drive_settings(basedir, suffix, driveTYPE, toml_file, json_file, j_settings, False, appdata_local)
    if driveTYPE is None:
        print(f"An error occured set SSD or HDD in {toml_file} for {basedir}")
        return None, None, None, None
    elif driveTYPE.lower() not in ('hdd', 'ssd'):
        print(f"Incorrect setting driveTYPE: {driveTYPE} in config: {toml_file}")
        return None, None, None, None

    return CACHE_S, systimeche, suffix, driveTYPE


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
