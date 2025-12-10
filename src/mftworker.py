import gc
import logging
import os
import pandas as pd
import re
import subprocess
import sys
import threading
import traceback
import time
from datetime import datetime, timedelta, timezone
from io import StringIO
from mft import PyMftParser, PyMftAttributeX10, PyMftAttributeX30  # type: ignore[attr-defined]
from .pyfunctions import cprint
from .qtfunctions import Worker
from .rntchangesfunctions import get_full_path
from .rntchangesfunctions import read_mft_progress
from .rntchangesfunctions import read_mft_default
from .rntchangesfunctions import removefile
from .rntchangesfunctions import mft_entrycount
from .rntchangesfunctions import str_to_bool
from .wmipy import ntfsdump
from .wmipy import mftecparse
# 12/08/2025


# Qobject
class MftWorker(Worker):

    def __init__(self, lclapp_data, log_label, mmin, method, output_f, csvnm, flnm, OLDSORT, flnmout, flnmdffout, drive, USRDIR, disk=None, volume=None, mft=None):
        super().__init__(None)

        self.logger = logging.getLogger(log_label)

        self.fmt = "%Y-%m-%d %H:%M:%S"

        self.lclhome = lclapp_data
        self.exe_path = self.lclhome / "bin"

        self.method = method  # action/tool
        self.mft = mft  # start its a Mtf.raw ^
        self.output_f = output_f  # middle make a Mtf.raw >
        self.csvnm = csvnm  # end parsed Mtf V
        self.csvopt = os.path.join(drive, csvnm)  # for different tool parameters/args NTFS tools

        self.flnm = flnm  # output filename
        self.flnmout = flnmout  # output path
        self.flnmdffout = flnmdffout  # output diff path
        self.OLDSORT = OLDSORT  # old output to compare. Moved to AppData

        self.drive = str(drive)  # workdir
        self.USRDIR = USRDIR  # destination dirDesktop
        self.filepath = os.path.join(USRDIR, flnm)  # destination dirFullpath

        self.stop_progress = False

        # ntfstools args
        self.disk = disk
        self.volume = volume

        self.mmin = mmin  # Hrs
        p = 3600 * mmin  # TotalSecondsBack

        self.df = (datetime.now(timezone.utc) - timedelta(seconds=p))  # Range # .strftime(fmt)

        self.progress.emit(20)

        # 20 - 30   dump the mft

        # method    "mftec"
        # parsemft 30 - 55   to csv
        # get_mftecdf 55 - 91 read csv

        # method    "mftdump"
        #  mft_dumphook 30 - 55  mft python rust hooks  55 - 91   from diff file

        #

        self.mftec_command = None
        self.icat_command = None
        self.ntfs_command = None
        self.fsstat_command = None

    def set_task(self, mftec_command, icat_command, fsstat_command, ntfs_command):  # optional pass ins

        self.mftec_command = mftec_command
        self.icat_command = icat_command
        self.ntfs_command = ntfs_command
        self.fsstat_command = fsstat_command

    def clean_up(self, rlt):
        removefile(self.csvopt)
        removefile(self.output_f)
        self.complete.emit(rlt)

    def is_non_empty_df(self, df):
        return df is not None and isinstance(df, pd.DataFrame) and not df.empty

    def get_results(self, df, compt, time_field, ctime_field, not_dumphooks=False, prog_v=89):

        recent_files = pd.DataFrame()

        try:
            local_tz = datetime.now().astimezone().tzinfo

            dt_cols = [time_field, ctime_field]

            if not_dumphooks:  # mftec not timezone aware
                for col in dt_cols:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize('UTC')  # vectorized
                self.progress.emit(prog_v)

            else:  # mft hook timezone aware
                for col in dt_cols:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            df = df.dropna(subset=[time_field])

            recent_files = df[  # vectorized
                (df['InUse']) &
                (~df['IsDirectory']) &
                (~df['IsAds']) &
                ((df[time_field] >= compt) | (df[ctime_field] >= compt))
            ].copy()

            mask = recent_files[ctime_field] > recent_files[time_field]
            # set the modified time to ctime where ctime is greater than modified time.
            # for copied files with preserved mtime so they are not missed.
            recent_files.loc[mask, time_field] = recent_files.loc[mask, ctime_field]

            recent_files[time_field] = recent_files[time_field].dt.tz_convert(local_tz).dt.tz_localize(None)
            recent_files[time_field] = recent_files[time_field].dt.floor('s')
            recent_files.sort_values(by=time_field, ascending=True, inplace=True)

        except Exception as e:
            emesg = f'failure get_results filtering dataframe: {type(e).__name__}: {e}'
            self.log.emit(emesg)
            self.logger.error("%s Traceback:\n", emesg, exc_info=True)  # traceback.format_exc()
            recent_files = pd.DataFrame()

        return recent_files

    def get_mftecdf(self, compt, csv_data=None, csvopt=None, strt=55, endp=91, time_field="LastModified0x10", ctime_field="Created0x10"):  # Created0x10, ReferenceCount
        # def to_local_naive(dt):=
        #     if pd.isnull(dt):
        #         return pd.NaT
        #     if dt.tzinfo is None:
        #         dt = dt.tz_localize('UTC')
        #     local_tz = datetime.now().astimezone().tzinfo
        #     dt_local = dt.tz_convert(local_tz)
        #     return dt_local.replace(tzinfo=None)
        # recent_files[time_field] = recent_files[time_field].apply(to_local_naive) if mixed ornullvalues  too slow?
        self.log.emit("\nProcessing data frame")

        columns = [
            "EntryNumber", "SequenceNumber", "InUse", "ParentEntryNumber", "ParentSequenceNumber", "ParentPath", "FileName", "Extension",
            "FileSize", "ReferenceCount", "ReparseTarget", "IsDirectory", "HasAds", "IsAds", "SI<FN", "uSecZeros", "Copied", "SiFlags", "NameType",
            "Created0x10", "Created0x30", "LastModified0x10", "LastModified0x30", "LastRecordChange0x10", "LastRecordChange0x30", "LastAccess0x10",
            "LastAccess0x30", "UpdateSequenceNumber", "LogfileSequenceNumber", "SecurityId", "ObjectIdFileDroid", "LoggedUtilStream", "ZoneIdContents",
            "SourceFile"
        ]

        bool_columns = ["InUse", "IsDirectory", "IsAds"]

        ir = int(round((endp - strt) * 0.583))
        prog_v = strt + ir

        try:

            if csv_data:  # from I/O string memory
                csv_data.seek(0)
                df = pd.read_csv(
                    csv_data,
                    names=columns,
                    low_memory=False,
                    converters={col: str_to_bool for col in bool_columns}
                )

            elif csvopt:  # from file
                df = pd.read_csv(
                    csvopt,
                    low_memory=False,
                    converters={col: str_to_bool for col in bool_columns}
                )

            else:
                self.log.emit("No input get_mftecdf")
                return None, None

            self.progress.emit(prog_v)

            if self.is_non_empty_df(df):

                recent_files = self.get_results(df, compt, time_field, ctime_field, True, prog_v=endp)  # convert to system time and filter by search criteria
                if self.is_non_empty_df(recent_files):
                    self.progress.emit(endp)

                    recent_files = get_full_path(recent_files)
                    return df, recent_files
            else:
                if self.method == "mftec_cutoff":
                    self.log.emit("No output from IOString csv")
                else:
                    self.log.emit(f"No output df is empty. unable to read csv {csvopt} in get_mftecdf func")

        except Exception as e:
            emesg = f'failure get_mftecdf func reading csv to dataframe: {type(e).__name__}: {e} \n{traceback.format_exc()}'
            self.log.emit(emesg)
            self.logger.error(emesg)

        return None, None

    def process_dataframe(self, method, csvopt):

        def opt_dataframe(flnmout, recent_files, time_field, section_title=None, mode="w"):
            k = 0
            try:
                with open(flnmout, mode, encoding="utf-8") as f:
                    if section_title:
                        if mode == "a":
                            k += 2
                            f.write("\n\n")
                        f.write(f"{section_title}\n\n")
                        k += 1
                    for _, row in recent_files.iterrows():
                        dt = row[time_field]
                        full_path = row['FullPath']
                        k += 1
                        f.write(f"{dt.strftime(self.fmt)} {full_path}\n")

                return k
            except Exception as er:
                self.log.emit(f'failed to output to file: {flnmout} re opt_dataframe, process_dataframe: {type(er).__name__}: {er}')
            return 0

        df = pd.DataFrame()             # original csv data
        df_2 = pd.DataFrame()           # OLDSORT
        recent_files = pd.DataFrame()   # SORTCOMPLETE

        merged_df = pd.DataFrame()      # created files from usn jrnl

        isdiff = False
        isusn = False

        is_dumphook = False

        rlt = 1

        diff_c = 0  # cursor in the diff file
        # m_count = 0 # cursor after `Created` ln in diff file

        time_field = "LastModified0x10"
        ctime_field = "Created0x10"

        compt = self.df

        try:  # Old results?
            if self.OLDSORT:  # theprevious resultsare were savedin /save-changesnew/ from desktop
                with open(self.OLDSORT, 'r', encoding='utf-8') as file:
                    data = []
                    for line in file:
                        line = line.strip()
                        if not line:
                            continue
                        parts = line.split(" ", 2)
                        if len(parts) < 3:
                            continue
                        date_time = f"{parts[0]} {parts[1]}"
                        filename = parts[2]
                        data.append([date_time, filename])
                    df_2 = pd.DataFrame(data, columns=[time_field, "FullPath"])
                    df_2[time_field] = pd.to_datetime(df_2[time_field], errors='coerce')
                    # local_tz = datetime.now().astimezone().tzinfo    -  convert back to UTC
                    # df_2['LastModified_aware'] = df_2['LastModified_local_naive'].apply(
                    #     lambda dt: local_tz.localize(dt) if pd.notnull(dt) else pd.NaT
                    # ).dt.tz_convert('UTC')
                removefile(self.OLDSORT)
            self.progress.emit(55)
        except Exception as e:
            self.log.emit(f'failure to read old sort file: {type(e).__name__}: {e}')

        try:

            if method == "mftec":
                df, recent_files = self.get_mftecdf(compt, None, csvopt, strt=55, endp=91, time_field=time_field, ctime_field=ctime_field)  # already parsed to .csv

            elif method == "mftec_cutoff":
                self.log.emit("\nParsing and loading Mft data")
                csv_data = self.read_mftmem(compt, strt=55, endp=75)  # parse directly into memory
                if not csv_data:
                    return 1
                df, recent_files = self.get_mftecdf(compt, csv_data, None, strt=75, endp=91, time_field=time_field, ctime_field=ctime_field)  # now parsed into memory
            else:
                self.log.emit("\nParsing Mft with python hooks")
                is_dumphook = True
                df, recent_files = self.mft_dumphook(self.output_f, compt, time_field, ctime_field, strt=55, endp=91)  # read the mft file with mftdump pythonhook into memory 55% - 91%

            if self._should_stop:
                self.clean_up(7)
                return 7

            if self.is_non_empty_df(recent_files):

                rlt = 0

                # Output results
                opt_dataframe(self.flnmout, recent_files, time_field)  # rntfilesxMftchanges24.txt

                # Output any difffile
                if self.is_non_empty_df(df_2):

                    try:
                        self.log.emit('Analyzing and diffing previous results')

                        STRTTIME = recent_files[time_field].min()
                        ETMN_df2 = df_2[time_field].max()

                        if ETMN_df2 >= STRTTIME:

                            df_diff = pd.merge(recent_files, df_2, on=[time_field, "FullPath"], how='outer', indicator=True)
                            only_in_df_1 = df_diff[df_diff['_merge'] == 'left_only']
                            diff_c = len(only_in_df_1)

                            if diff_c > 0:

                                lines_diff = (only_in_df_1[time_field].astype(str) + " " + only_in_df_1["FullPath"]).tolist()

                                with open(self.flnmdffout, 'w', encoding='utf-8') as file:  # rntfilesxMftchangesDiffFromLastSearch24.txt
                                    file.write("\n".join(lines_diff))

                                isdiff = True

                        else:
                            diff_c = opt_dataframe(self.flnmdffout, df_2, time_field, "Below is not applicable to search it is the previous search")
                    except Exception as e:
                        self.log.emit(f'Error comparing old dataframe df2 to recent_files {type(e).__name__}: {e}')

                # Output created files by crossreferncing - USN Jrnl - append to bottom of diff file
                try:
                    del recent_files
                    gc.collect()

                    df_3 = pd.DataFrame()  # usn jrnl
                    df_cfiles = pd.DataFrame()  # filtered usn from df_3

                    mode = "w"
                    if isdiff:
                        mode = "a"
                    cmd = ['fsutil', 'usn', 'readjournal', 'c:', 'csv']  # | findstr /i /C:"`"File create`"" > “log.log”
                    res = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True
                    )
                    if res.returncode == 0:
                        all_lines = res.stdout.splitlines()

                        hdr = (
                            "Usn,File name,File name length,Reason #,Reason,Time stamp,"
                            "File attributes #,File attributes,File ID,Parent file ID,Source info #,"
                            "Source info,Security ID,Major version,Minor version,Record length,"
                            "Number of extents,Remaining extents,Extent,Offset,Length"
                        )
                        filterlines = [line for line in all_lines if '"File create"' in line]  # a voids shell=True
                        lines = [hdr] + filterlines

                        csv_data = "\n".join(lines)

                        df_3 = pd.read_csv(StringIO(csv_data))

                        df_3['Time stamp'] = pd.to_datetime(df_3['Time stamp'], errors='coerce').dt.tz_localize('UTC')
                        df_3 = df_3.dropna(subset=['File ID'])

                        df_cfiles = df_3[
                            (df_3['File attributes'] == 'Archive') &
                            (df_3['Time stamp'] >= compt)
                        ].copy()

                        def frn_to_entry(frn_str):
                            entry_id = frn_str[-16:]
                            entry_hex = entry_id[4:]
                            return int(entry_hex, 16)
                        df_cfiles['File ID'] = df_cfiles['File ID'].astype(str).str.strip()
                        df_cfiles['MFT Entry'] = df_cfiles['File ID'].apply(frn_to_entry)

                        df_cfiles['MFT Entry'] = pd.to_numeric(
                            df_cfiles['MFT Entry'],
                            errors='coerce',
                        )

                        df = df.dropna(subset=['EntryNumber'])
                        df_cfiles = df_cfiles.dropna(subset=['MFT Entry'])

                        df['EntryNumber'] = df['EntryNumber'].astype('int64')
                        df['FileName'] = df['FileName'].astype(str)
                        df_cfiles['MFT Entry'] = df_cfiles['MFT Entry'].astype('int64')
                        df_cfiles['File name'] = df_cfiles['File name'].astype(str)

                        df = df.loc[
                            (df['InUse']) &
                            (~df['IsDirectory']) &
                            (~df['IsAds'])
                        ]

                        # Join the usn jrnl to the mft to get the full path. the usn jrnl only has filename
                        merged_df = df_cfiles.merge(
                            df,
                            left_on=['MFT Entry', 'File name'],
                            right_on=['EntryNumber', 'FileName'],
                            how='inner',  # how='inner' only files that currently exist in the mft   how='left', all USN with nan for ParentPath, FullPath if not in mft
                            suffixes=('_cfiles', '')
                        )

                        if not is_dumphook:
                            merged_df = get_full_path(merged_df)  # its mftec not mft hook

                        merged_df.sort_values(by='Time stamp', ascending=True, inplace=True)  # merged_df[time_field] = merged_df[time_field].dt.floor('s')  strftime drops microseconds

                        if self.is_non_empty_df(merged_df):
                            opt_dataframe(self.flnmdffout, merged_df, 'Time stamp', "Created files", mode=mode)  # rntfilesxMftchangesDiffFromLastSearch24.txt
                            isusn = True
                        else:
                            self.log.emit('No created files found in USN Jrnl for created')
                        self.progress.emit(100)
                    else:
                        self.log.emit('Failed to read USN Jrnl for created')
                    # df_cfiles['Field ID'] = df_cfiles['Field ID'].apply(lambda x: int(x, 16))
                except Exception as e:
                    emesg = f'failure in usn block: {type(e).__name__}: {e}'
                    rlt = 1
                    self.logger.error("%s Traceback:\n", emesg, exc_info=True)  # traceback.format_exc()
                    self.log.emit(emesg)

                if rlt == 0:  # output results
                    if os.path.isfile(self.flnmout):
                        self.log.emit(f'\nComplete output to {self.flnmout}\n')
                        if os.path.isfile(self.flnmdffout):
                            if isdiff:
                                self.log.emit(f'Difference file output to {self.flnmdffout}')
                                y = "from cross referencing usn jrnl"
                                if isusn:
                                    if diff_c > 0:
                                        append_c = 2
                                        self.log.emit(f'created files listed on ln {diff_c + append_c} {y}')  # lines from the first part of the file plus 2 lines for the append
                                    else:
                                        self.log.emit(f'with created files {y}')
                            elif isusn:
                                self.log.emit(f'Created files output to difference file {self.flnmdffout}')

            else:
                self.log.emit(f'failure to acquire a dataframe with method {method} . exiting..')
        except Exception as e:
            emesg = f'general failure in process_dataframe: {type(e).__name__} :{e}'
            self.log.emit(f'{emesg} \n {traceback.format_exc()}')
            self.logger.error(emesg, exc_info=True)

        # cleanup
        if self.is_non_empty_df(df):
            del df
        if self.is_non_empty_df(merged_df):
            del merged_df
        gc.collect()

        return rlt

    def dump_mft(self):
        sts = 1
        self.log.emit("Copying mft..")

        # ntfstools
        if self.volume:

            disk = f'disk={self.disk}'
            volume = f'volume={self.volume}'
            opt = f"output={self.output_f}"

            k = disk + ' ' + volume + ' ' + opt
            self.log.emit(k)
            res = ntfsdump(disk, volume, opt, str(self.ntfs_command))

            rlt = res['returncode']
            if rlt == 1:
                self.log.emit(res['stdout'])
                self.log.emit(res['stderr'])
                self.log.emit("\nNo output from ntfstools re mft csv")
            else:
                sts = 0
                self.progress.emit(30)

        # icat
        else:

            cmd = [str(self.icat_command), '-f', 'ntfs', '\\\\.\\C:', '0']  # '.\\bin\\icat.exe'
            try:
                with open(self.output_f, 'wb') as f:
                    process = subprocess.Popen(cmd, stdout=f, stderr=subprocess.PIPE, text=False)
                    for line in iter(process.stderr.readline, b''):
                        decoded = line.decode('utf-8', errors='replace').strip()
                        if decoded:
                            self.log.emit(decoded)
                        else:
                            break
                    process.wait()
                    if process.returncode == 0:
                        sts = 0
                        self.progress.emit(30)
            except (FileNotFoundError, PermissionError) as e:
                self.log.emit(f"icat exe {self.icat_command} not found or permission error. err: {e}")
            except Exception as e:
                err = f"Unexpected Failure from icat dumping mft {type(e).__name__}: {e}"
                self.log.emit(f"{err} \n{traceback.format_exc()}")
                self.logger.error("%s Traceback: \n", err, exc_info=True)
        return sts

    def run(self):

        rlt = 1
        res = 1
        sts = 1

        method = self.method

        try:

            if method == "mftdump":
                sts = self.dump_mft()  # get system mft for step one

                if self._should_stop:
                    self.clean_up(7)
                    return 7

            else:
                sts = 0

            if sts == 0:

                self.log.emit(f"Using method: {method}")

                if method == "mftec":  # user has mftec installed

                    self.log.emit("\nParsing and loading Mft data")
                    # try to do it in one step
                    if self.parsemft():
                        res = 0
                        if not os.path.isfile(self.csvopt):
                            self.log.emit(f"failed to parse mft or couldnt find mft using mftecmd.exe : {self.output_f}")
                            res = 1
                    else:
                        self.log.emit('Failed to get parse mft with mftecmd.exe unable to continue. in mftecparse')

                elif method == "mftdump" or method == "mftec_cutoff":
                    res = 0
                else:
                    self.log.emit(f"Invalid method {method} quitting.")
                    self.clean_up(1)
                    return 1

                if res == 0:

                    try:

                        if self._should_stop:
                            self.clean_up(7)
                            return 7

                        rlt = self.process_dataframe(method, self.csvopt)

                    except Exception as e:
                        self.log.emit(f"Csv formatting error in pandas process_dataframe: {e}")
                        raise

        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.exception.emit(exc_type, exc_value, exc_traceback)
            rlt = 1

        if rlt != 7:
            self.clean_up(rlt)
        return rlt

    # MFTECmd default
    # self.output_f for a raw mft file. mftecmd can parse from base drive to csv to save space
    def parsemft(self):

        rlt = mftecparse('C:\\$MFT', self.drive, self.csvnm, str(self.mftec_command))
        if rlt:
            return True
        else:
            self.log.emit("Failed. Unable to output csv in parsemft")

        return False

    # used if MFTECmd not installed  -  #omerbenamram
    #
    def mft_dumphook(self, mft, end_df, time_field='LastModified0x10', ctime_field="Created0x10", strt=55, endp=91):
        r = 0
        incr = 10
        delta_v = endp - strt
        try:

            parser = PyMftParser(mft)

            total_e = parser.number_of_entries()
            self.log.emit(f"Total number of entries: {total_e}\n")
            steps = [int((i / 10) * total_e) for i in range(1, 11)]
            current_step = 0

            file_entries = []

            for entry_or_error in parser.entries():
                if isinstance(entry_or_error, RuntimeError):
                    continue
                entry_path = entry_or_error.full_path or ""
                if entry_path.startswith(".") or entry_path.startswith("$") or entry_path.startswith("[Unknown]") or not entry_path:
                    continue

                mtime = None
                ctime = None
                flags = None
                is_dir = False
                in_use = True
                is_ads = False

                is_continue = True

                r += 1
                if current_step < len(steps) and r >= steps[current_step]:
                    prog_i = (current_step + 1) * incr
                    self.progress.emit(round((delta_v * (prog_i / 100))) + strt)
                    current_step += 1

                entry_id = entry_or_error.entry_id
                full_path = "C:\\" + entry_path
                file_name = None

                # print(dir(entry_or_error)) shows avail attributes in stdout
                for attribute_or_error in entry_or_error.attributes():
                    if isinstance(attribute_or_error, RuntimeError):
                        continue
                    if not is_continue:
                        break
                    if "ALLOCATED" not in entry_or_error.flags:
                        in_use = False
                    entry_content = attribute_or_error.attribute_content
                    if entry_content:

                        if isinstance(entry_content, PyMftAttributeX10):  # STANDARDINFO

                            mtime = entry_content.modified
                            ctime = entry_content.created
                            if not mtime or (mtime < end_df and ctime < end_df):
                                is_continue = False

                        elif isinstance(entry_content, PyMftAttributeX30):   # FILE ATTRIB
                            file_name = entry_content.name
                            flags = entry_content.flags
                            if "FILE_ATTRIBUTE_IS_DIRECTORY" in flags:
                                is_dir = True

                if is_continue:
                    if mtime and file_name:
                        file_entries.append([entry_id, mtime, ctime, is_dir, in_use, is_ads, file_name, full_path])

            if current_step == len(steps) - 1:
                self.progress.emit(round((delta_v * (100 / 100))) + strt)

            if file_entries:

                try:

                    headers = ["EntryNumber", time_field, "Created0x10", "IsDirectory", "InUse", "IsAds", "FileName", "FullPath"]
                    df = pd.DataFrame(file_entries, columns=headers)

                    recent_files = self.get_results(df, end_df, time_field, ctime_field)  # by search criteria and convert to system time
                    if not self.is_non_empty_df(recent_files):
                        return None, None

                    return df, recent_files

                except Exception as e:
                    emesg = f"Error building dataframe from MyMftParser mft_dumphook: {type(e).__name__}: {e}"
                    self.log.emit(emesg)
                    self.logger.error(emesg, exc_info=True)
        except (FileNotFoundError, PermissionError) as e:
            self.log.emit(f"Error: mft_dumphook Could not find Mft {mft} err: {e}")
        except Exception as e:
            emesg = f'Unexpect error in MyMftParser mft_dumphook func: {type(e).__name__}: {e}'
            self.log.emit(f"{emesg} \n{traceback.format_exc()}", )
            self.logger.error("%s Traceback:", emesg, exc_info=True)

        return None, None

    # based on start % and polling on filesize
    #  to 100% or indicated to stop
    def progress_timer(self, output_f, start, end, interval=1, t_out=120):
        sv = 0
        s_tmn = time.time()
        while sv <= end and not self.stop_progress:
            if os.path.isfile(output_f):
                sv = os.path.getsize(output_f)
                progfr = min(sv / end, 1.0)
                prog = start + progfr * (100 - start)
                self.progress.emit(prog)
            if time.time() - s_tmn > t_out:
                break
            time.sleep(interval)

    # MFTECmd import
    def outputmft(self):
        # writing to csvopt
        try:

            MB = 1024 * 1024

            # initialize
            output_pth = os.path.join(self.drive, self.output_f)
            if os.path.isfile(output_pth):
                removefile(output_pth)
            sb = os.path.getsize(self.mft)

            if sb > 900 * MB:
                intv = 5
            elif sb > 600 * MB:
                intv = 4
            else:
                intv = 2

            te = sb / 2  # expected size

            mft_flnm = self.mft

            frmt = 'yyyy-MM-dd HH:mm:ss.ffffff'

            progress_thread = None

            cmd = [str(self.mftec_command), '--dt', frmt, '-f', mft_flnm, '--csv', self.drive, '--csvf', self.output_f]
            # mg = 'Running command: ' + ' '.join(f'"{c}"' for c in cmd)
            # self.log.emit(mg)
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            last_line = None
            is_start = False

            csv_pth = None

            for line in proc.stdout:

                if not is_start:
                    ctext = None
                    if "File type: Mft" in line:

                        ctext = "File type: \033[1;32mMft\033[0m"  # MFTECmd color
                        self.progress.emit(50)
                    elif "FILE records found:" in line:
                        rcfnd_mch = re.search(r"FILE records found:\s*([\d,]+)", line)
                        rcfree_mch = re.search(r"Free records:\s*([\d,]+)", line)
                        fl_sze_mch = re.search(r"File size:\s+(.*)", line)

                        rcfnd = rcfnd_mch.group(1) if rcfnd_mch else "0"
                        rcfree = rcfree_mch.group(1) if rcfree_mch else "0"
                        fl_sze = fl_sze_mch.group(1) if fl_sze_mch else "0"

                        if all(x != "0" for x in (rcfnd, rcfree, fl_sze)):
                            ctext = (
                                f"\033[36m{mft_flnm}\033[0m: "
                                f"FILE records found: \033[35m{rcfnd}\033[0m "
                                f"(Free records: \033[35m{rcfree}\033[0m) "
                                f"File size: \033[36m{fl_sze}\033[0m"
                            )
                        else:
                            ctext = cprint.magenta(line.rstrip('\n'))  # MFTECmd color

                        progress_thread = threading.Thread(target=self.progress_timer, args=(output_pth, 50, te, intv))
                        is_start = True
                        progress_thread.start()
                        # break
                    elif "Command line" in line:
                        ctext = f"Command line: \033[36m{' '.join(cmd[1:])}\033[0m"
                    elif "Processed" in line:
                        number = None
                        match = re.search(r"Processed \S+ in ([\d.]+) seconds", line)
                        if match:
                            number = float(match.group(1))
                        if number is None:
                            number = ""
                        ctext = f"Processed \033[36m{self.output_f}\033[0m in \033[35m{number}\033[0m seconds"
                    else:
                        mline = line.strip()
                        if mline:
                            ctext = f"\033[36m{mline} \033[0m"

                            if "MFTECmd" in mline or "https" in mline:
                                ctext += "\n"

                    if ctext:
                        self.log.emit(ctext)
                line = line.strip()
                if line:
                    last_line = line

            _, proc_stderr = proc.communicate()
            rlt = proc.returncode

            if last_line:
                # csv_pth = last_line.rsplit(" ", 1)[-1] doesnt work with spaces
                # joined_path = os.path.join(self.drive, self.output_f)

                ctext = "\tCSV output will be saved to \033[1;32m" + output_pth + "\033[0m"
                self.log.emit(ctext)

            self.stop_progress = True
            if progress_thread and progress_thread.is_alive():
                progress_thread.join(timeout=1)
            if rlt == 0:
                if not csv_pth:
                    fl_chk = os.path.join(self.drive, self.output_f)
                    if os.path.isfile(fl_chk):
                        self.log.emit(f'Csv output to {self.drive}\\{self.output_f}')
                self.progress.emit(100)
            else:
                if proc_stderr:
                    self.log.emit(proc_stderr)
                self.log.emit("Failed. Unable to output csv with mftecmd.exe")
        except (FileNotFoundError, PermissionError) as e:
            rlt = 1
            self.log.emit(f"Error: Could not find command or permission error {self.mftec_command}. {e}")
        except Exception as e:
            emesg = f'Unexpected err in outputmft func {type(e).__name__}: {e}'
            rlt = 1
            self.log.emit(f"{emesg} traceback:\n{traceback.format_exc()}")
            self.logger.error("%s traceback:", emesg, exc_info=True)
        self.complete.emit(rlt)

    def save_mft(self):
        self.progress.emit(66.66)
        res = self.dump_mft()
        if res == 0:
            self.log.emit(f'\nOutput to {self.output_f}')
            self.progress.emit(100)
        self.complete.emit(res)

    def read_mftmem(self, compt, strt=0, endp=100, mft="C:\\$MFT"):

        cutoff = compt.replace(microsecond=0)
        df = cutoff.isoformat().replace("+00:00", "Z")
        self.log.emit(f"Cutoff {df}")

        # .\MFTECmd.exe -f "C:\`$MFT" --cutoff 2025-10-19T13:45:30 --csv Y:\ --csvf myfile2.csv > Y:\myfile.csv  # default format is 7digits yyyy-MM-dd HH:mm:ss.fffffff           Note prints to stdout and is parsed
        cmd = [str(self.mftec_command), '-f', mft, '--dt', 'yyyy-MM-dd HH:mm:ss.ffffff', '--cutoff', df, '--csv', 'C:\\', '--csvf', 'myfile2.csv']  # '.\\bin\\MFTECmd.exe'

        # self.log.emit('Running command:' + ' '.join(f'"{c}"' for c in cmd))
        byte_s = mft_entrycount()

        csv_data = StringIO()

        try:
            if byte_s:
                rlt, std_err = read_mft_progress(cmd, csv_data, byte_s, strt, endp, self.progress.emit)
            else:
                self.log.emit("Unable to get entry count on Mft. exiting read_mftmem")

                rlt, std_err = read_mft_default(cmd, csv_data)

            if rlt == 0:
                if len(csv_data.getvalue()) != 0:

                    self.progress.emit(float(endp))
                    return csv_data

                else:
                    self.log.emit("No csv_data in read_mftmem mft_worker main.py")
            else:
                if std_err:
                    self.log.emit(f'Failed. Unable to output csv with mftecmd.exe: {std_err}')
        except (FileNotFoundError, PermissionError):
            self.log.emit(f'Unable to find MFTECmd.exe {self.mftec_command} or permission error \\bin')
        except Exception as e:
            emesg = f'error running cmd {cmd} {type(e).__name__} {e}'
            self.log.emit(f"{emesg} \n {traceback.format_exc()}")
            self.logger.error("%s traceback: \n", emesg, exc_info=True)
        return None
