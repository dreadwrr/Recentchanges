import os
import platform
from PySide6.QtGui import QColor
from PySide6.QtCore import QTimer, QTime, QElapsedTimer, Signal, QUrl, Qt
from PySide6.QtMultimedia import QMediaPlayer, QAudioOutput  # QSoundEffect
from PySide6.QtWidgets import QWidget, QLCDNumber, QInputDialog
from src.ui_alarmclock import Ui_AlarmClock

MODES = ["ALARM", "CRONO", "TIMER"]

MAX_CRONO_MS = (99 * 3600 + 59 * 60 + 59) * 1000 + 990  # 99:59:59:99


class ClickableLCD(QLCDNumber):
    clicked = Signal()

    def mousePressEvent(self, event):
        self.clicked.emit()
        super().mousePressEvent(event)


def windows_beep():
    import winsound
    # frequency Hz, ms
    winsound.Beep(1000, 500)


def linux_beep():
    print("\a", flush=True)


class AlarmClock(QWidget):

    soundValidated = Signal(bool, bool, object)

    def __init__(self, parent=None, theme=None, _24hformat=True, alarm_time=None, sound_file=None, sound_set_file=".\\Resources\\alarmt.ogg"):
        super().__init__(parent)

        self.ui = Ui_AlarmClock()
        self.ui.setupUi(self)

        # if needing only wav support and not needing to import QMediaPlayer
        # self.player = QSoundEffect()
        # self.player.setSource(QUrl.fromLocalFile("alarm.wav"))
        # self.player.setLoopCount(QSoundEffect.Infinite)
        # self.player.play()
        # self.player.stop()

        self.audio = QAudioOutput()
        self.player = QMediaPlayer()
        self.player.setAudioOutput(self.audio)
        self.player.errorOccurred.connect(self.player_on_error)
        self.player.mediaStatusChanged.connect(
            self.media_status_changed
        )
        self.play_on_load = False

        self.is_24h = _24hformat

        self.sound_file = None
        self.sound_set_file = None
        self.new_source = None

        self.valid_sound = self.is_set_sound = False

        if sound_set_file:
            if os.path.isfile(sound_set_file):
                self.sound_set_file = sound_set_file
            else:
                print(f"could locate alarm set sound file: {sound_file}")
        if sound_file:
            ext = os.path.splitext(sound_file)[1].lower()
            if ext in (".wav", ".mp3", ".ogg") and os.path.isfile(sound_file):
                self.sound_file = sound_file

            else:
                print(f"could locate alarm sound file: {sound_file}. setting to beep")

        self.validate_sounds()

        if platform.system() == "Windows":
            self.beep = windows_beep
        else:
            self.beep = linux_beep

        self.am_pm = False
        self.mode_index = 0

        # setup lcd
        d = 5
        if _24hformat:
            self.ui.apmlabel.hide()
            d = 8
        else:
            self.ui.apmlabel.setText("PM" if self.am_pm else "AM")
        self.ui.lcdNumber.setDigitCount(d)

        self.lcd = ClickableLCD(self)
        self.lcd.setDigitCount(d)
        self.ui.gridLayout.replaceWidget(self.ui.lcdNumber, self.lcd)

        mn = self.ui.lcdNumber.minimumSize()
        mx = self.ui.lcdNumber.maximumSize()
        self.ui.lcdNumber.deleteLater()

        self.ui.lcdNumber = self.lcd
        self.ui.lcdNumber.setMaximumSize(mx)
        self.ui.lcdNumber.setMinimumSize(mn)
        # initialize lcd
        self.set_format(theme)
        self.ui.alarmb.setText(MODES[self.mode_index])

        self.hide_alarm_controls()
        self.ui.setlabel.hide()

        self.blink_on = self.alarm_blink_on = False

        self.alarm_field = "hours"
        self.alarm_hour = 7
        self.alarm_minute = 0
        self.alarm_am_pm = False

        # alarm saved in 24 hour format parse to 12 hour if not 24h fmt
        if alarm_time:

            res = self.set_alarm_time(alarm_time)
            if res == 2:
                raise ValueError(f"Invalid alarm_time format: {alarm_time!r}, expected 'HH:mm'")
            elif res == 3:
                raise ValueError(f"alarm_time out of range: {alarm_time!r}")

        self.alarm_triggered = False
        self.alarm_on = self.alarm_armed = False
        self.editing_alarm = False

        self.timer_running = False
        self.timer_seconds = 0
        self.timer_initial_seconds = 90 * 60

        self.crono_running = False
        self.crono_elapsed_ms = 0
        self.crono_wall = QElapsedTimer()

        self.ui.alarmb.clicked.connect(self.handle_mode_button)
        self.ui.upb.clicked.connect(self.increment_field)
        self.ui.downb.clicked.connect(self.decrement_field)
        self.ui.setb.clicked.connect(self.advance_alarm_field)
        self.ui.snoozeb.clicked.connect(self.handle_snooze)
        self.ui.lcdNumber.clicked.connect(self.handle_lcd_click)

        self.timer = QTimer(self)
        self.timer.timeout.connect(self.display_clock)

        self.crono_timer = QTimer(self)
        self.crono_timer.timeout.connect(self.display_crono)
        self.crono_timer.setInterval(10)

        self.blink_timer = QTimer(self)
        self.blink_timer.timeout.connect(self.blink_field)
        self.blink_timer.setInterval(500)

        self.alarm_timer = QTimer(self)
        self.alarm_timer.timeout.connect(self.alarm_blink)
        self.alarm_timer.setInterval(500)

        self.timer.start(1000)

    @property
    def mode(self):
        return MODES[self.mode_index]

    def get_text(self, hour_str, minute_str, second_str, colon):
        if self.is_24h:
            return f"{hour_str}{colon}{minute_str}{colon}{second_str}"
        return f"{hour_str}{colon}{minute_str}"

    def crono_format(self, ms):
        total_cs = ms // 10
        h = total_cs // 360000
        m = (total_cs % 360000) // 6000
        s = (total_cs % 6000) // 100
        cs = total_cs % 100
        if h > 0:
            return f"{h:02d}:{m:02d}:{s:02d}:{cs:02d}"
        else:
            return f"{m:02d}:{s:02d}:{cs:02d}"

    def timer_format(self, total_seconds):
        h = total_seconds // 3600
        m = (total_seconds % 3600) // 60
        s = total_seconds % 60
        if h > 0:
            return f"{h:02d}:{m:02d}:{s:02d}"
        else:
            return f"{m:02d}:{s:02d}"

    # lcd screen update

    def display_clock(self):
        if self.mode == "TIMER":
            if not self.timer_running:
                return
            if self.timer_seconds > 0:
                self.timer_seconds -= 1
                self.display_timer()
            else:
                self.timer_running = False
                self.display_timer()
                self.trigger_alarm()
            return
        if self.mode == "CRONO":
            return

        if self.alarm_triggered:
            return

        if self.editing_alarm:
            self.ui.lcdNumber.display(
                self.get_text(f"{self.alarm_hour:02d}", f"{self.alarm_minute:02d}", "00", ":")
            )
            return

        now = QTime.currentTime()

        if self.alarm_on:
            if not self.is_24h:
                hour = int(now.toString("hh"))
                min = now.minute()
                am_pm = now.hour() >= 12
                if not (hour == self.alarm_hour and min == self.alarm_minute and am_pm == self.alarm_am_pm):
                    self.alarm_armed = True
                elif self.alarm_armed:
                    self.alarm_armed = False
                    self.trigger_alarm()
                    return

            else:
                now_mins = now.hour() * 60 + now.minute()
                alarm_mins = self.alarm_hour * 60 + self.alarm_minute
                if now_mins < alarm_mins:
                    self.alarm_armed = True
                elif self.alarm_armed and now_mins >= alarm_mins:
                    self.alarm_armed = False
                    self.trigger_alarm()
                    return

        colon = ":"

        if not self.is_24h:

            hour_str = now.toString("hh")

            am_pm = now.hour() >= 12  # ampm = now.toString("AP")
            if self.am_pm != am_pm:
                self.ui.apmlabel.setText("PM" if am_pm else "AM")
                self.am_pm = am_pm
        else:
            hour_str = f"{now.hour():02d}"

        # only blink colon if not 24 hour clock
        if not self.is_24h:
            colon = ":" if now.second() % 2 == 0 else " "

        self.ui.lcdNumber.display(
            self.get_text(hour_str, f"{now.minute():02d}", f"{now.second():02d}", colon)
        )

    def display_alarm(self):
        if self.alarm_on:
            self.ui.lcdNumber.display(
                self.get_text(f"{self.alarm_hour:02d}", f"{self.alarm_minute:02d}", "00", ":")
            )
        else:
            self.display_clock()

    def display_timer(self):
        self.ui.lcdNumber.display(self.timer_format(self.timer_seconds))

    def display_crono(self):
        ms = self.crono_elapsed_ms
        if self.crono_running:
            ms += self.crono_wall.elapsed()
            if ms >= MAX_CRONO_MS:
                ms = MAX_CRONO_MS
                self.crono_start_stop(False)
        self.ui.lcdNumber.setDigitCount(11 if ms >= 3600000 else 8)
        self.ui.lcdNumber.display(self.crono_format(ms))

    # end lcd screen update

    def crono_start_stop(self, running):
        if running == self.crono_running:
            return
        self.crono_running = running
        if running:
            self.crono_wall.start()
            self.crono_timer.start()
        else:
            if self.crono_wall.isValid():
                self.crono_elapsed_ms += self.crono_wall.elapsed()
            self.crono_timer.stop()

    def set_mode(self, mode):

        # stop any previous mode
        if self.mode == "CRONO":
            self.crono_start_stop(False)
        elif self.mode == "TIMER":
            self.ui.apmlabel.setText("PM" if self.am_pm else "AM")
            self.timer_running = False

        # advance mode
        self.mode_index = MODES.index(mode)
        self.ui.alarmb.setText(mode)

        # if it is the clock set to clock format
        self.ui.lcdNumber.setDigitCount(5 if mode == "ALARM" and not self.is_24h else 8)

        if mode == "ALARM":
            self.timer.start(1000)
            self.display_clock()
        elif mode == "CRONO":
            self.ui.apmlabel.setText("")
            self.timer.stop()
            self.switch_to_crono(reset=False)

        elif mode == "TIMER":
            self.ui.apmlabel.setText("")
            self.timer_running = False
            self.timer_seconds = self.timer_initial_seconds
            self.timer.start(1000)
            self.display_timer()

    def show_alarm_controls(self):
        self.ui.upb.show()
        self.ui.downb.show()
        self.ui.setb.show()

    def hide_alarm_controls(self):
        self.ui.upb.hide()
        self.ui.downb.hide()
        self.ui.setb.hide()

    def toggle_alarm_controls(self):
        self.editing_alarm = not self.editing_alarm
        if self.editing_alarm:
            self.ui.apmlabel.setText("PM" if self.alarm_am_pm else "AM")
            self.show_alarm_controls()
            self.alarm_field = "hours"
            self.blink_on = False
            self.blink_timer.start()
        else:
            self.ui.apmlabel.setText("PM" if self.am_pm else "AM")
            self.hide_alarm_controls()
            self.blink_timer.stop()
            self.display_alarm()

    def blink_field(self):
        if not self.editing_alarm:
            return
        self.blink_on = not self.blink_on
        hour_str = "  " if (self.alarm_field == "hours" and self.blink_on) else f"{self.alarm_hour:02d}"
        minute_str = "  " if (self.alarm_field == "minutes" and self.blink_on) else f"{self.alarm_minute:02d}"
        self.ui.lcdNumber.display(
            self.get_text(hour_str, minute_str, "00", ":")
        )

    def increment_field(self):
        if self.alarm_field == "hours":
            if self.is_24h:
                self.alarm_hour = (self.alarm_hour + 1) % 24
            else:
                self.alarm_hour = (self.alarm_hour % 12) + 1
                if self.alarm_hour == 12:
                    self.alarm_am_pm = not self.alarm_am_pm
                    self.ui.apmlabel.setText("PM" if self.alarm_am_pm else "AM")
        else:
            self.alarm_minute = (self.alarm_minute + 1) % 60

        self.ui.lcdNumber.display(
            self.get_text(f"{self.alarm_hour:02d}", f"{self.alarm_minute:02d}", "00", ":")
        )

    def decrement_field(self):
        if self.alarm_field == "hours":
            if self.is_24h:
                self.alarm_hour = (self.alarm_hour - 1) % 24
            else:
                # self.alarm_hour = (self.alarm_hour - 2) % 12 + 1
                self.alarm_hour -= 1
                if self.alarm_hour < 1:
                    self.alarm_hour = 12
                    self.alarm_am_pm = not self.alarm_am_pm
                    self.ui.apmlabel.setText("PM" if self.alarm_am_pm else "AM")
        else:
            self.alarm_minute = (self.alarm_minute - 1) % 60
        self.ui.lcdNumber.display(
            self.get_text(f"{self.alarm_hour:02d}", f"{self.alarm_minute:02d}", "00", ":")
        )

    def advance_alarm_field(self):
        if not self.editing_alarm:
            return
        if self.alarm_field == "hours":
            self.alarm_field = "minutes"
            self.blink_on = False
            self.ui.lcdNumber.display(
                self.get_text(f"{self.alarm_hour:02d}", f"{self.alarm_minute:02d}", "00", ":")
            )
        else:
            self.alarm_set()

    # Inputs

    def handle_mode_button(self):

        if self.mode == "ALARM":
            self.toggle_alarm_controls()

        elif self.mode == "CRONO":

            self.crono_start_stop(False)
            self.switch_to_crono(True)

        elif self.mode == "TIMER":

            # reset the timer
            if self.timer_running or self.timer_seconds != self.timer_initial_seconds:
                self.timer_running = False
                self.timer_seconds = self.timer_initial_seconds
                self.display_timer()

            # get user input in minutes
            else:
                minutes, ok = QInputDialog.getInt(
                    self, "Set Timer", "Minutes:",
                    self.timer_initial_seconds // 60, 1, 5400,
                )
                if ok:
                    self.timer_initial_seconds = minutes * 60
                    self.timer_seconds = self.timer_initial_seconds
                    self.display_timer()

    def handle_lcd_click(self):
        if self.alarm_triggered:
            return

        if self.mode == "TIMER":
            if self.timer_seconds == 0:
                self.timer_seconds = self.timer_initial_seconds
            self.timer_running = not self.timer_running
            self.display_timer()

        elif self.mode == "CRONO":
            self.crono_start_stop(not self.crono_running)
        elif self.mode == "ALARM":
            if not self.editing_alarm:
                visible = self.ui.alarmb.isVisible()
                self.ui.alarmb.setVisible(not visible)
                self.ui.snoozeb.setVisible(not visible)

    def handle_snooze(self):
        if self.alarm_triggered:
            self.alarm_stop()
            return

        if self.editing_alarm:
            self.alarm_set()
            return

        if self.alarm_on:
            self.alarm_on = False
            self.ui.setlabel.hide()
            self.display_clock()
            return

        # go to next mode
        next_index = (self.mode_index + 1) % len(MODES)
        self.set_mode(MODES[next_index])

    # Alarm

    def validate_sounds(self):
        self.new_source = None
        self.is_set_sound = False

        if not self.sound_file:
            self.is_set_sound = True
            if self.sound_set_file:
                self.player.setSource(QUrl.fromLocalFile(self.sound_set_file))
        else:
            self.player.setSource(QUrl.fromLocalFile(self.sound_file))

    def trigger_alarm(self):
        self.timer.stop()
        self.ui.alarmb.show()
        self.ui.snoozeb.show()
        self.alarm_triggered = True
        self.alarm_timer.start()

        self.ui.setlabel.show()
        self.alarm_blink_on = True
        self.blink_alarm_display()
        self.alarm_play()

    def blink_alarm_display(self):
        if self.mode == "TIMER":
            self.ui.lcdNumber.display("00:00")
        else:
            self.ui.lcdNumber.display(
                self.get_text(f"{self.alarm_hour:02d}", f"{self.alarm_minute:02d}", "00", ":")
            )

    def alarm_blink(self):
        if not self.alarm_triggered:
            self.alarm_timer.stop()
            return
        self.alarm_blink_on = not self.alarm_blink_on
        if self.alarm_blink_on:
            self.blink_alarm_display()
            if not self.sound_file:
                self.alarm_play()

        else:
            self.ui.lcdNumber.display("      ")

    def alarm_play(self):

        if self.valid_sound and self.sound_file:
            self.is_set_sound = False
            self.player.setLoops(QMediaPlayer.Infinite)
            self.player.setSource(QUrl.fromLocalFile(self.sound_file))
            self.player.play()
        else:
            self.beep()

    def alarm_stop(self):
        self.alarm_timer.stop()
        self.player.stop()
        self.alarm_triggered = False
        self.alarm_armed = False
        self.alarm_on = False
        self.ui.setlabel.hide()
        if self.mode == "TIMER":
            self.timer_seconds = self.timer_initial_seconds
            self.display_timer()
            self.timer.start(1000)
        elif self.mode == "ALARM":
            self.timer.start(1000)
            self.display_clock()

    def alarm_set(self):
        self.blink_timer.stop()
        self.editing_alarm = False

        ssf = self.sound_set_file
        if ssf:
            self.is_set_sound = True
            self.player.setLoops(1)
            self.player.setSource(QUrl.fromLocalFile(ssf))
            self.player.play()

        self.alarm_on = True
        self.alarm_armed = False  # initial state and set later in display_clock

        self.hide_alarm_controls()
        self.ui.setlabel.show()
        self.ui.apmlabel.setText("PM" if self.am_pm else "AM")
        self.display_clock()

    # Crono

    def switch_to_crono(self, reset=False):
        if reset:
            self.crono_elapsed_ms = 0
        self.display_crono()

    # general functions

    def player_on_error(self, *args):
        set_sound = self.is_set_sound

        # an error occured use beeps instead
        if not self.new_source:
            if not set_sound:
                self.valid_sound = False

            else:
                self.sound_set_file = None

        self.soundValidated.emit(False, self.is_set_sound, self.new_source)

        self.new_source = None
        self.is_set_sound = None

    def media_status_changed(self, status):
        set_sound = self.is_set_sound

        if status == QMediaPlayer.MediaStatus.LoadedMedia:

            if self.new_source:

                if not set_sound:
                    self.sound_file = self.new_source
                else:
                    self.sound_set_file = self.new_source

                self.soundValidated.emit(True, set_sound, self.new_source)
            if not set_sound:
                self.valid_sound = True  # valid alarm sound loaded

            if not self.new_source and not set_sound and self.sound_set_file and not self.alarm_triggered:
                self.is_set_sound = True
                self.player.setSource(QUrl.fromLocalFile(self.sound_set_file))

            self.new_source = None
            self.is_set_sound = None

        elif status == QMediaPlayer.MediaStatus.InvalidMedia:

            # if its from init invalidate and use beeps
            if not self.new_source:
                self.valid_sound = False

            self.soundValidated.emit(False, self.is_set_sound, self.new_source)

            self.new_source = None
            self.is_set_sound = None

    def change_alarm_sound(self, sound_file, is_set_sound):

        if os.path.isfile(sound_file):
            self.is_set_sound = is_set_sound
            self.new_source = sound_file
            self.player.setSource(QUrl.fromLocalFile(sound_file))
            return True

        print(
            f"Couldnt find alarm {'set ' if is_set_sound else ''}sound file",
            sound_file,
            "alarm set to beep",
        )
        return False

    def set_format(self, theme):
        if not theme:
            return
        palette = self.ui.lcdNumber.palette()

        if theme == "redblack":
            self.ui.lcdNumber.setStyleSheet("background-color: black; border: 1px solid #330000;")
        if theme == "red" or theme == "redblack":
            palette.setColor(palette.ColorRole.WindowText, QColor("#800000"))  # palette.setColor(palette.ColorRole.WindowText, Qt.GlobalColor.red)
            self.ui.apmlabel.setStyleSheet("color: #800000;")  # self.ui.apmlabel.setStyleSheet("color: red;")
        elif theme == "blue":
            palette.setColor(palette.ColorRole.WindowText, QColor("#000080"))  # Qt.GlobalColor.blue
            self.ui.apmlabel.setStyleSheet("color: #000080;")
        elif theme == "black":
            palette.setColor(palette.ColorRole.WindowText, Qt.GlobalColor.black)
            self.ui.apmlabel.setStyleSheet("color: black;")
        else:
            print("Unrecognized theme out of options redblack, red, blue, black. recieved", theme)
            return

        self.ui.lcdNumber.setPalette(palette)

    def set_clock_format(self, _24hformat):

        if self.is_24h != _24hformat:

            # 24 to 12
            if self.is_24h:

                self.ui.apmlabel.show()
                hour, self.alarm_am_pm = self.convert_alarm_time(self.alarm_hour, _24hformat)  # get the hour 1-12 and is it am or pm
                self.ui.apmlabel.setText("PM" if self.am_pm else "AM")  # now = QTime.currentTime()
            else:
                self.ui.apmlabel.hide()

                hour, _ = self.get_alarm_time()  # returns 24hr fmt

            self.is_24h = _24hformat
            d = 8 if _24hformat else 5
            self.ui.lcdNumber.setDigitCount(d)
            self.alarm_hour = hour
            if self.mode == "ALARM":
                self.display_clock()

    def convert_alarm_time(self, hour: int, _24hformat: bool) -> tuple[int, bool]:
        """ used when switching clock from 24 to 12 or
            reading back saved alarm set time in 24hr
            format and convert it based on using a
            12 or 24hr clock """
        is_am_pm = False
        if not _24hformat:
            is_am_pm = hour >= 12
            hour = (hour % 12) or 12
        return hour, is_am_pm

    def get_alarm_time(self) -> tuple[int, int]:
        """ used when switching clock from 12 to 24 or writing the alarm set time in 24hr fmt to file """
        hour = self.alarm_hour
        if not self.is_24h:
            if self.alarm_am_pm:
                if self.alarm_hour < 12:
                    hour = hour + 12
            else:
                if hour == 12:
                    hour = 0
        return hour, self.alarm_minute

    def set_alarm_time(self, alarm_time: str) -> int:
        """ called in constructor on app start if there is a saved alarm time in 24hr format and convert it to 12hr if necessary """
        parts = alarm_time.split(":")
        if len(parts) != 2:
            return 2
        hour, minute = int(parts[0]), int(parts[1])

        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return 3

        # if using 12h clock set am_pm for alarm set time
        hour, self.alarm_am_pm = self.convert_alarm_time(hour, self.is_24h)
        self.alarm_hour, self.alarm_minute = hour, minute
        return 0
