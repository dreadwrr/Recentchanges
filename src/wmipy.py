# 11/21/2025
# See foot notes for extracting MFT and UsnJrnl.
#
# Notes
# Sleuthkit utilities. author Brian carrier
#
# mft procedure
# Check for mft
# 'fls -f ntfs -r disk.img | grep \$MFT' linux
# '.\fls -f ntfs \\.\C: | findstr MFT'   windows # directories non recurse
#
# 1. '.\fsstat.exe \\.\C:' verify NTFS and suitability
# 2. '.\istat -f ntfs \\.\C: 0 > istat_mft.txt' read top of file for suitable location
# Output mft:  '.\icat -f ntfs \\.\C: 0 > D:\Adobe\MFT.raw'
#
# '.\icat -f ntfs \\.\C: 0 > D:\Adobe\MFT.raw'
# UsnJrnl
# '.\fls -F -r \\.\C:' | findstr "Usn"
#
# 112989 inode number
# '.\istat -f ntfs \\.\C:' 112989 > stats to verify Standard_Information. $UsnJrnl data stream
# locate $UsnJrnl and attribute 128-890 $J datastream.
# Once confirmed. Output UsnJrnl:    '.\icat -f ntfs \\.\C: 112989-128-890 > D:\Adobe\J'
# Parse with MFTECmd.exe along with $MFT to build paths.
#
#
# usn jrnl parsing
#
#   fsutil usn readJournal C: csv
#   fsutil usn readjournal c: csv | findstr /i /C:"file delete" >> “log.log”
#
#
# cmd = ['.\\bin\\MFTECmd.exe', '-dt', 'yyyy-MM-dd HH:mm:ss.ffffff', '-f', usn, '-m', mft, '--csv', outp, '--csvf', outf]  # fullpaths for usn
#
# .\ntfstool.x86.exe mft.dump disk=2 volume=3 output=MFT.bin
# .\MFTECmd.exe --dt "yyyy-MM-dd HH:mm:ss.ffffff" -f MFT.bin --csv Y:\ --csvf mft.txt
# Alternative to sleuthkid. makes zip archive
# # 'kape.exe --tsource C: --target FileSystem --test c:\Users\demo\Desktop\ --vhdx demo'         saves MFT, USN to vhdx
# import re
import subprocess
import traceback
import wmi
from .rntchangesfunctions import parse_drive
''' icat '''

# ok to proceed NTFS volume/correct location
# warn user. offer option to import MFT


def validmft(fsstat_path=None):

    def vrange(mft_range):
        try:
            start, end = map(int, mft_range.split(":")[1].strip().split("-"))
            if start != 0:
                print("MFT range starts from src.an unexpected cluster. Aborting.")
                return False
            return True
        except Exception as e:
            print(f"Error parsing range: {e}")
        return False

    rlt = False

    exe = r'.\bin\fsstat.exe'
    if fsstat_path:
        exe = fsstat_path

    cmd = [exe, '\\\\.\\C:']

    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        stdout, stderr = process.communicate()
        if process.returncode != 0:
            print(f"fsstat.exe failed with error code {process.returncode}")
            if stderr:
                print("stderr output:")
                print(stderr)
            return False

        rlines = stdout.splitlines()
        if len(rlines) >= 14:
            res = vrange(rlines[13].strip())
            if res:
                rlt = True
                # print("NTFS filesystem. MFT verified:", rlines[13].strip())

    except FileNotFoundError:
        rlt = False
        print(f"fsstat.exe not found unable to locate {exe} in \\bin")
    except Exception as e:
        rlt = False
        print(f"Error in verifying ntfs filesystem fsstat.exe: {type(e).__name__} {e} traceback:\n {traceback.format_exc()}")
    return rlt

# output mft to target from C:


def gmft(opt, icat_path=None):
    exe = r'.\bin\icat.exe'
    if icat_path:
        exe = icat_path
    cmd = [exe, '-f', 'ntfs', '\\\\.\\C:', '0']
    try:
        with open(opt, 'wb') as f:
            process = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=False)

        if process.returncode != 0:
            err = process.stderr.decode(errors="ignore")
            print("Error running icat:")
            print(err)
            return 1
        return 0
    except FileNotFoundError:
        print(f"icat {exe} not found in \\bin.")
    except Exception as e:
        print(f"gmft function copying mft error {type(e).__name__} {e} traceback:\n {traceback.format_exc()}")
    return 1


''' NTFStools '''

#   '.\ntfstool.x86.exe info'
#   '.\ntfstool.x86.exe info disk=2'
#   '.\ntfstool.x86.exe info disk=2 volume=3'
#   '.\ntfstool.x86.exe usn.dump disk=2 volume=3 output=d:\Adobe\usn.dat'
#   '.\ntfstool.x86.exe mft.dump disk=2 volume=3 output=d:\Adobe\MFT.bin'


def get_disk_and_volume_for_drive(drive_letter='C:'):
    try:
        c = wmi.WMI()
        # basedir = basedir.rstrip('\\/').upper()
        drive = parse_drive(drive_letter).upper() + ":"

        # Get partition associated with drive
        partitions = c.query(
            f"ASSOCIATORS OF {{Win32_LogicalDisk.DeviceID='{drive}'}} WHERE AssocClass = Win32_LogicalDiskToPartition"
        )
        if not partitions:
            raise RuntimeError(f"No partition found for drive {drive_letter}")

        partition = partitions[0]

        # parsing works but could fail on different versions of windows / languages ect
        # m = re.search(r'Disk\s*#?(\d+).*Partition\s*#?(\d+)', partition.DeviceID)
        # if not m:
        #     raise RuntimeError(f"Could not parse disk and partition from DeviceID: {partition.DeviceID}")
        # disk_number = int(m.group(1))
        # volume_number = int(m.group(2))

        disk_number = partition.DiskIndex
        volume_number = partition.Index

        return disk_number, volume_number
    except wmi.x_wmi as e:
        print(f"WMI error: {e} \n{traceback.format_exc()}")
    except RuntimeError as e:
        print(f"Runtime error: {e} \n{traceback.format_exc()}")
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__} {e} \n{traceback.format_exc()}")
    return None


def get_mounted_partitions(drive, target, ntfs_path=None):

    exe = r'.\bin\ntfstool.x86.exe'
    if ntfs_path:
        exe = ntfs_path

    cmd = [exe, 'info', target]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        output = result.stdout
        partitions = []
        in_table = False
        for line in output.splitlines():
            line = line.strip()
            if line.startswith('|') and 'Id' in line:
                in_table = True
                continue
            if in_table and line.startswith('+') and '-' in line:
                continue
            if in_table and line.startswith('|'):
                fields = [f.strip() for f in line.strip('|').split('|')]
                if len(fields) >= 4:
                    part_id = fields[0]
                    mount = fields[3]
                    if mount.upper() == drive.upper():
                        return {'id': part_id, 'mount': mount}
    except subprocess.CalledProcessError as e:
        print("Error running command:", e.stderr)
    except FileNotFoundError:
        print(f"ntfstool.x86.exe at {exe} not found in \\bin.")
    except Exception as e:
        print(f"Unexpected error get_mounted_partitions: {type(e).__name__} {e} traceback:\n {traceback.format_exc()}")
    return None

# cmd = ['.\\bin\\ntfstool.x86.exe', 'mft.dump', 'disk=n', 'volume=n', 'output=d:\mft.raw']
# cmd = ['.\\bin\\ntfstool.x86.exe', 'mft.dump', 'disk=n', 'volume=n', 'output=d:\mft.csv', 'format=csv']


def ntfsdump(tgt, volume, opt, ntfs_path=None):
    try:
        exe = r'.\bin\ntfstool.x86.exe'
        if ntfs_path:
            exe = ntfs_path

        cmd = [exe, 'mft.dump', tgt, volume, opt]
        print(cmd)

        # Use Popen to stream output without hanging
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        stdout_lines = []
        stderr_lines = []

        # Read stdout line by line
        for line in process.stdout:
            print(line, end='')  # optional: print live
            stdout_lines.append(line)

        # Read stderr completely
        for line in process.stderr:
            print(line, end='')  # optional: print live
            stderr_lines.append(line)

        process.wait()

        return {
            'returncode': process.returncode,
            'stdout': ''.join(stdout_lines),
            'stderr': ''.join(stderr_lines)
        }

    except FileNotFoundError as e:
        return {
            'returncode': 1,
            'stdout': '',
            'stderr': f"Unable to find ntfstool.x86.exe {exe}: {e}"
        }
    except Exception as e:
        return {
            'returncode': 1,
            'stdout': '',
            'stderr': f"Unexpected error: {e}\n{traceback.format_exc()}"
        }
        # original can hang for unknown reason
        # .run
        # process = subprocess.run(
        #     cmd,
        #     stdout=subprocess.PIPE,
        #     stderr=subprocess.PIPE,
        #     text=True,
        #     check=True
        # )
    #     return {
    #         'returncode': process.returncode,
    #         'stdout': process.stdout,
    #         'stderr': process.stderr
    #     }
    # except subprocess.CalledProcessError as e:
    #     return {
    #         'returncode': e.returncode,
    #         'stdout': e.stdout,
    #         'stderr': e.stderr
    #     }
    # except FileNotFoundError as e:
    #     return {
    #         'returncode': 1,
    #         'stdout': '',
    #         'stderr': f"Unable to find ntfstool.x86.exe {exe}: {e}"
    #     }
    # except Exception as e:
    #     return {
    #         'returncode': 1,
    #         'stdout': '',
    #         'stderr': f"Unexpected error: {e}\n{traceback.format_exc()}"
    #     }

# Default used for parsing or imported Mft
# MFTECmd - Used for mft output from C:\\ to csv


def mftecparse(mftf, outp, outf, mftec_path=None):  # writing to csvopt
    try:
        exe = mftec_path if mftec_path else r'.\bin\MFTECmd.exe'
        cmd = [exe, '--dt', 'yyyy-MM-dd HH:mm:ss.ffffff', '-f', mftf, '--csv', outp, '--csvf', outf]  # default format is 7digits yyyy-MM-dd HH:mm:ss.fffffff
        print('Running command:', ' '.join(cmd))
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if res.returncode == 0:
            return True
        print(res.stdout)
        print("MFTECmd.exe error:")
        print(res.stderr)
    except (FileNotFoundError, PermissionError):
        print(f'Unable to find MFTECmd.exe {exe} in \\bin')
    except Exception as e:
        print(f'Unexpected err in parsemft {type(e).__name__} {e} traceback:\n {traceback.format_exc()}')
    return False
