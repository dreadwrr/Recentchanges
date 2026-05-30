import csv
import io
import os
import re
import subprocess
import traceback
from pathlib import Path
from .rntchangesfunctions import removefile
# 05/29/2026


TICKS_BTWN_1601_1970 = 11644473600000000
TICKS_BTWN_1601_1970_NS = 11644473600000000000
MAX_NAME = 1024


# parsec.exe
def output_mft(exe_path: str, target: str):
    """ to build the directories on the system to be able to read usn journal. also secondary ctime of all files
        to use for ctime search """

    proc = subprocess.Popen(
        [exe_path, target],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    assert proc.stdout is not None
    assert proc.stderr is not None

    try:

        records = build_tuple(proc)

    finally:
        proc.stdout.close()

    stderr_data = proc.stderr.read()
    proc.stderr.close()
    rc = proc.wait()

    if rc != 0:
        err = stderr_data.decode("utf-8", errors="replace")
        raise RuntimeError(f"Parser exited with code {rc}: {err}")

    return records


def ntfs_to_us(value):
    try:
        return (int(value) // 10) - TICKS_BTWN_1601_1970
    except (ValueError, TypeError):
        return None


def ntfs_to_ns(value):
    try:
        return (int(value) * 100) - TICKS_BTWN_1601_1970_NS
    except (ValueError, TypeError):
        return None


def frn_to_entry(frn):
    record_num = frn & 0xFFFFFFFFFFFF
    sequence_num = (frn >> 48) & 0xFFFF
    return record_num, sequence_num


def entry_to_frn(record_num, sequence_num):
    return (sequence_num << 48) | record_num


def build_tuple(proc):
    """ full fmt for pandas qt app """
    entries = []
    csv_started = False

    for line in iter(proc.stdout.readline, ''):
        if not line.strip():
            continue
        if not csv_started:
            if "recno,sequence,parent_recno" in line:
                csv_started = True
            continue
        if ',' not in line:
            continue
        try:
            record = next(csv.reader(io.StringIO(line)))
        except Exception:
            continue
        if len(record) > 15:
            if record[12] == "[DIR]":
                continue
            try:
                recno = int(record[0])
                sequence_num = int(record[1])
                parent_recno = int(record[2])
                parent_sequence = int(record[3])
                in_use = int(record[4])
                file_attribs = int(record[11])
            except ValueError:
                continue

            in_use = bool(in_use)

            size = record[5]
            hardlinks = record[6]

            has_ads = record[13] == "1"

            creation_time = record[8]
            mod_time = record[7]
            mft_mod = record[9]
            access_time = record[10]

            last_usn = record[14]
            name = record[15]
            path = record[16]

            entries.append((recno, sequence_num, in_use, parent_recno, parent_sequence, path, name, size, hardlinks, has_ads, file_attribs, creation_time, mod_time, mft_mod, access_time, last_usn))

    return entries


def mft_entrycount():
    KB = 1024
    MB = KB**2
    GB = KB**3
    byte_s = None
    cmd = ['fsutil', 'fsinfo', 'ntfsinfo', 'C:']
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = proc.communicate()
        output = (stdout + stderr).lower()
        if "access is denied" in output:
            print("Error: Access denied. Please run as administrator.")
            return None
        elif proc.returncode != 0:
            print("Command failed with return code", proc.returncode)
            print("err", stderr.strip())
            return None
        else:
            for line in stdout.splitlines():
                line = line.strip()
                if line.startswith("Mft Valid Data Length"):
                    match = re.search(r"([\d\.]+)\s*(GB|MB|KB|bytes)", line, re.IGNORECASE)
                    if match:
                        value = float(match.group(1))
                        unit = match.group(2).upper()
                        if unit == "GB":
                            byte_s = value * GB
                        elif unit == "MB":
                            byte_s = value * MB
                        elif unit == "KB":
                            byte_s = value * KB
                        else:
                            byte_s = value
        if byte_s is None:
            print("Unable to read MFT entry count")
        return byte_s
    except subprocess.SubprocessError as e:
        print(f"Error in subprocess execution mft_entrycount: {type(e).__name__}: {e}")
        print(traceback.format_exc())
        return None


# alternative for function below. was used for recentchangessearch.py
# See if its the right version .NET 9 check version from stdout
def mftec_is_cutoff(lclappdata):

    exec_path = os.path.join(lclappdata, "bin", "MFTECmd.exe")
    cmd = [exec_path, '-f', 'C:\\$MFT', '--dt', 'yyyy-MM-dd HH:mm:ss.ffffff', '--cutoff', '2025-11-10T07:48:46Z']  # , '--csv', 'C:\\', '--csvf', 'myfile2.csv' # '.\\bin\\MFTECmd.exe'
    # print('Running command:', ' '.join(cmd))
    # mesg = 'Running command:' + ' '.join(f'"{c}"' for c in cmd)
    try:
        cver = False
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for line in iter(proc.stdout.readline, ''):
            if line.strip():
                if "--cutoff" in line:
                    cver = True
                    break

        proc.stdout.close()
        rlt = proc.wait()
        proc_stderr = proc.stderr.read()
        if rlt != 0:
            if proc_stderr:
                print("MFTECmd stderr:")
                print(proc_stderr)
            return False

        return cver

    except FileNotFoundError:
        print(f"MFTECmd {exec_path} not found")
    except Exception as e:
        print(f"Failed to verify MFTECmd version mftec_cutoff function. {type(e).__name__} {e} \n{traceback.format_exc()}")
    return None


# Used in Qt for mftec check
# See if its the right version .NET 9 check version from file
# same as above but print the arg list to a file only works in certain environments.
def mftec_version(exe_path, tempdir):  # Qt

    fn = "cutoff"
    c_args = "--" + fn
    temp_path = Path(tempdir)
    version_file = temp_path / "version.txt"

    try:

        result = "mftec"

        # Run MFTECmd and redirect the output to version.txt
        # .\bin\MFTECmd.exe
        subprocess.run(rf'"{exe_path}" > {version_file}', shell=True)

        if not version_file.is_file():
            return None

        with version_file.open("r", encoding="utf-8") as f:
            for line in f:
                if c_args in line:
                    result = "mftec_cutoff"
        removefile(version_file)

    except FileNotFoundError:
        result = None
        print(f"{exe_path} not found")
    except Exception as e:
        result = None
        print(f"mftec_ver exception {type(e).__name__} {e} \n {traceback.format_exc()}")

    return result


# to possiblly increase efficiency but overhead is not an issue. maybe if an error shows up
# this is for below read_mftmem
# proc = subprocess.Popen(
#     cmd,
#     stdout=subprocess.PIPE,
#     stderr=subprocess.PIPE,
#     bufsize=1024*1024,
#     text=True,
#     encoding="utf-8",
#     errors="replace"
# )
#
# buffer = ""
#
# while True:
#     chunk = proc.stdout.read(1024*1024)   # 1MB
#     if not chunk:
#         break
#
#     buffer += chunk
#     lines = buffer.split("\n")
#     buffer = lines.pop()
#
#     for line in lines:
#         process_line(line)


def read_mft_progress(cmd, csv_data, byte_s, strt, endp, show_progress=False, logger=None):

    total_e = (int(byte_s) // 1024)

    num_steps = 32
    step_size = total_e / (num_steps - 1)
    steps = [int(round(step_size * i)) for i in range(num_steps)]
    current_step_index = 0

    csv_started = False
    x = 0

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
    for line in iter(proc.stdout.readline, ''):
        if not line.strip():
            continue
        x += 1
        if show_progress:

            if current_step_index < len(steps) and x >= steps[current_step_index]:
                progress = float(current_step_index) / max(num_steps - 1, 1) * 100
                progress = round(strt + (endp - strt) * (progress / 100), 2)

                if logger:

                    logger(int(progress))
                else:
                    print(f'Progress: {progress}%')

                current_step_index += 1

        if ',' not in line:
            continue
        if not csv_started:
            if "EntryNumber,SequenceNumber,InUse" in line:  # if line.startswith("EntryNumber,SequenceNumber,InUse"): weird char at start BOM character discard header and rebuild later
                csv_started = True
                continue
            else:
                continue

        csv_data.write(line)

    proc.stdout.close()
    rlt = proc.wait()
    err_output = proc.stderr.read()
    return rlt, err_output


def build_mftec_path(df):

    df = df[df['ParentPath'].notna() & df['FileName'].notna()].copy()  # get rid of warnings by making a copy

    df['ParentPath'] = df['ParentPath'].fillna('').astype(str).str.replace(r'^\.(\\)?', r'C:\\', regex=True)
    df['FileName'] = df['FileName'].fillna('').astype(str)

    df['FullPath'] = df['ParentPath'].str.rstrip('\\') + '\\' + df['FileName'].str.lstrip('\\')

    return df


def build_parsec_path(df):
    df = df[df['ParentPath'].notna() & df['FileName'].notna()].copy()  # get rid of warnings by making a copy
    df['ParentPath'] = df['ParentPath'].fillna('').astype(str).str.replace(r'^(\\)?', r'C:\\', regex=True)  # .str.replace(r'^\.(\\)?', r'C:\\', regex=True)
    df['FileName'] = df['FileName'].fillna('').astype(str)
    df['FullPath'] = df['ParentPath'].str.rstrip('\\') + '\\' + df['FileName'].str.lstrip('\\')
    df['FullPath'] = df['ParentPath']
    return df
