import re
import time
from PySide6.QtCore import QObject, Signal, QProcess, QTimer


# QProcess scripts
class ProcessHandler(QObject):

    progress = Signal(float)
    log = Signal(str)
    error = Signal(str)
    status = Signal(str)
    complete = Signal(int, int)

    def __init__(self):
        super().__init__()
        self.process = QProcess(self)

        self.process.readyReadStandardOutput.connect(self.handle_stdout)
        self.process.readyReadStandardError.connect(self.handle_stderr)
        self.process.finished.connect(self.process_finished)

        self.ranges = {
            "normal": (20, 60),  # fsearch normal range
            "pstsrg": (65, 90)  # pstsrg normal range
        }

        self.should_stop = False
        self.prog_v = 0.0
        self.key_value_pattern = re.compile(r"^([a-zA-Z0-9\s]{10,})\s+:(\s*.*)$")  # (r"^([a-zA-Z0-9]+)\s*:(.*)$")

        self.database = None  # start_pyprocess      page_2 main label
        self.statusmsg = None
        self.is_search = False
        self.is_search = False
        self.is_postop = False
        self.is_scanIDX = False

        self.ANALYTICSECT = None  # start_powershell
        self.st_time = 0  # .

        self.rangeVALUE = None  # set_compress  ffsearch wsl/pwsh
        self.zipPROGRAM = None
        self.zipPATH = None
        self.USRDIR = None
        self.downloads = None

        self.tgt_file = None  # set_task for, popup text editor
        self.dspEDITOR = None  # 2.
        self.dspPATH = None
        self.temp_dir = None

    def set_compress(self, rangeVALUE, zipPROGRAM, zipPATH, USRDIR, downloads):  # For compress button. pass ins
        self.rangeVALUE = rangeVALUE
        self.zipPROGRAM = zipPROGRAM
        self.zipPATH = zipPATH
        self.USRDIR = USRDIR
        self.downloads = downloads

    def set_task(self, tgt_file, dspEDITOR, dspPATH, tmp_dir):  # Opening results in text editor. pass ins
        self.tgt_file = tgt_file  # output results. filepaths or for popup dspEDITOR
        self.dspEDITOR = dspEDITOR  # 2.
        self.dspPATH = dspPATH
        self.temp_dir = tmp_dir

    def set_mcore(self, ismcore):  # pass in # Handles not stopping if processes are running at certain range or stage in multicore
        self.ismcore = ismcore

    def is_running(self):
        return self.process.state() == QProcess.ProcessState.Running

    def stop(self):
        # pstsrg done normally  90%
        # POSTOP                85% pstsrg done
        # scanIDX               80% pstsrg done
        # POSTOP and scanIDX    75% pstsrg done

        if hasattr(self, "ismcore"):
            self.should_stop = True

            if not self.database:

                if self.is_postop and self.is_scanIDX:
                    self.ranges["pstsrg"] = (65, 75)
                elif self.is_postop:
                    self.ranges["pstsrg"] = (65, 85)
                elif self.is_scanIDX:
                    self.ranges["pstsrg"] = (65, 80)
            else:
                self.ranges = {
                    "build": (0, 100),
                    "scan": (0, 100)
                }

            self.wait_count = 0

            self.stop_timer = QTimer()
            self.stop_timer.timeout.connect(self._stop_poll)
            self.stop_timer.start(100)
        else:
            self._terminate_process()

    def _stop_poll(self):
        in_range = any(start <= self.prog_v <= end for start, end in self.ranges.values())

        if in_range and self.wait_count <= 30000:
            self.wait_count += 100
            return
        self.stop_timer.stop()
        if self.prog_v > self.ranges["pstsrg"][1] and not self.is_scanIDX:
            return
        self._terminate_process()
        if self.wait_count > 30000:
            self.log.emit("Process timeout; continuing termination.")

    def _terminate_process(self):
        if hasattr(self, "ismcore"):
            self.process.kill()
            self.process.waitForFinished(1000)
        else:
            if self.is_running():
                self.process.terminate()
                if not self.process.waitForFinished(3000):
                    self.log.emit("Process killed forcefully")
                    self.process.kill()
                    self.process.waitForFinished(1000)

        exit_code = 7
        exit_status = QProcess.ExitStatus.NormalExit.value
        self.complete.emit(exit_code, exit_status)

    def start_pyprocess(self, cmd, args=None, dbopt=None, status_message=None, is_search=False, is_postop=False, is_scanIDX=False):

        if args is None:
            args = []
        else:
            args = list(args)

        self.database = dbopt
        self.statusmsg = status_message
        self.is_search = is_search
        self.is_postop = is_postop
        self.is_scanIDX = is_scanIDX

        if "ffsearch.py" in cmd:
            if self.rangeVALUE is not None:
                args += [self.rangeVALUE, self.zipPROGRAM, self.zipPATH, self.USRDIR, self.downloads]

        args = [str(a) for a in args if a is not None]

        self.process.start("python", ["-u", cmd] + args)

    def start_powershell(self, cmd, args, ANALYTICSECT=None):
        if ANALYTICSECT:
            self.st_time = time.time()
            self.ANALYTICSECT = True

        command = [
            "powershell.exe",
            "-ExecutionPolicy", "Bypass",
            "-File", cmd] + args
        print(f"Running command: {command[0]} {command[1:]}")

        self.process.start(command[0], command[1:])

    def process_finished(self, exit_code, exit_status):

        if exit_code == 0:
            if self.ANALYTICSECT:  # powershell scripts
                el = time.time() - self.st_time
                self.log.emit(f'Search took {el:.3f} seconds')

            if self.database:  # sys index updates
                self.status.emit(f"{self.statusmsg} completed")

        else:
            if self.database:
                self.status.emit(f"{self.statusmsg} failed exit code {exit_code}")
        es_int = exit_status.value if isinstance(exit_status, QProcess.ExitStatus) else exit_status
        self.complete.emit(exit_code, es_int)

    def handle_progress(self, line):

        try:
            value_str = line.split("Progress:")[1].strip()
            percent_str = value_str.split('%')[0].strip()
            percent = float(percent_str)
            self.prog_v = percent
            self.progress.emit(percent)

            if percent >= 97.0 and self.database:
                self.status.emit("Waiting remaining worker(s) to finish")

        except ValueError:
            self.log.emit(f"Malformed progress line: {line}")

    def handle_stdout(self):

        def process_text(line):

            if "Progress:" in line:
                self.handle_progress(line)

            elif line.startswith("fsearch complete") or line.startswith("pstsrg complete"):
                if self.should_stop:
                    if line.startswith("pstsrg complete") and self.is_scanIDX:
                        self._terminate_process()
                    elif line.startswith("fsearch complete"):
                        self._terminate_process()
            else:
                self.log.emit(line)

        # start_time = time.time()           measure time to parse text for a script call
        data = self.process.readAllStandardOutput()
        # ; ignore \0
        text = bytes(data).decode("utf-8", errors="replace")  # .strip()  <-- can keep cleaner

        lines = text.splitlines(keepends=True)

        for raw_line in lines:
            line = raw_line.rstrip()

            if self.is_search:  # it is a powershell script so parse PSSQLite colored text from find_filesps1
                match = self.key_value_pattern.match(line)
                if match:
                    key = match.group(1)
                    value = match.group(2).strip()
                    ctext = f"\033[1;32m{key}:\033[0m {value}"
                    self.log.emit(ctext)
                else:
                    process_text(line)
            else:  # normal
                process_text(line)
        # end_time = time.time()
        # print(f"Processing time: {end_time - start_time:.4f} seconds")

    def handle_stderr(self):
        data = self.process.readAllStandardError()
        stderr = bytes(data).decode("utf-8")
        self.error.emit(stderr)
