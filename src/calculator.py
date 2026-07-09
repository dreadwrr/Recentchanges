import math
import mpmath
import random
import re
import sys
from functools import partial
from PySide6 import QtWidgets, QtCore, QtGui
from .ui_calculator import Ui_Form
# 07/08/2026


def window_message(parent, message, title="Status", default=True):  # ok
    msg = QtWidgets.QMessageBox(parent)
    msg.setStyleSheet("""
    QFrame { background: palette(window); }
    QLabel { background: transparent; color: palette(windowText); font-size: 18px; }
    """)
    if default:
        msg.setIcon(QtWidgets.QMessageBox.Icon.Warning)
    else:
        msg.setIcon(QtWidgets.QMessageBox.Icon.Information)
    msg.setWindowTitle(title)
    msg.setText(message)
    msg.setWindowIcon(QtGui.QIcon("./Resources/calculator/48.png"))
    msg.setStandardButtons(QtWidgets.QMessageBox.StandardButton.Ok)
    msg.exec()


def str_to_mpmath(expression):
    """ enclose all numberical values in mpf() to retain precision if using mpmath """
    return re.sub(
        r'\d+(?:\.\d+)?(?:[eE][+-]?\d+)?',
        lambda m: f"mpf('{m.group()}')",
        expression
    )


class SCalculator(QtWidgets.QWidget):
    """ calculator with minimal form factor to implement some math with python. Intention was to make it in a way that
        it can later be customized for other specific use cases or layed adjusted to preference. Was a result of
        experimenting with pyside widgets is based off of github https://www.github.com/CodeQuestions """

    ERROR_MSG = "ERROR"
    ERROR_MSG2 = "OVERFLOW"

    FLOAT_SIG_DIGITS = 14
    DECIMAL_DIGITS = 55
    DECIMAL_MAX = 150
    OUTPUT_LIMIT = 57  # 57 chars at font size 35 scientific. 24 chars at font size 25 regular

    SCI_THRESHOLD = 10

    MAX_FONT_SIZE = 100
    MIN_FONT_SIZE = 25  # 25 for scientific # 30 for regular

    # it was found that linear sizing didnt work as smaller text fits more digits than before
    # so need to use two maps one for regular and one for scientific

    FONT_SIZE_RANGES = [
        (9, 9, 90),
        (10, 10, 80),
        (11, 11, 75),
        (12, 12, 70),
        (13, 13, 65),
        (14, 14, 55),
        (15, 16, 50),
        (17, 20, 40),
        (21, 23, 35),
        (24, 24, 30),

    ]

    FONT_SIZE_RANGES_MP = [
        (9, 9, 90),
        (10, 10, 80),
        (11, 11, 75),
        (12, 12, 70),
        (13, 13, 65),
        (14, 28, 60),
        (29, 30, 55),
        (31, 33, 50),
        (34, 37, 45),
        (38, 41, 40),
        (42, 47, 35),
        (48, 56, 30),
        (57, 57, 25),
    ]

    ANGLE_MODES = ["DEG", "RAD", "GRAD"]
    DIGIT_VALUES = set(".0123456789")
    PAREN_TRIGGER_CHARS = (DIGIT_VALUES - {"."}) | {")", "%"}  # | for join sets

    TRIG_MAP = {
        ("sin", False, False): "sin",
        ("sin", True,  False): "asin",
        ("sin", False, True):  "sinh",
        ("sin", True,  True):  "asinh",
        ("cos", False, False): "cos",
        ("cos", True,  False): "acos",
        ("cos", False, True):  "cosh",
        ("cos", True,  True):  "acosh",
        ("tan", False, False): "tan",
        ("tan", True,  False): "atan",
        ("tan", False, True):  "tanh",
        ("tan", True,  True):  "atanh",
    }

    LABEL_MAP = {
        "sin": "sin", "asin": "sin⁻¹", "sinh": "sinh", "asinh": "sinh⁻¹",
        "cos": "cos", "acos": "cos⁻¹", "cosh": "cosh", "acosh": "cosh⁻¹",
        "tan": "tan", "atan": "tan⁻¹", "tanh": "tanh", "atanh": "tanh⁻¹",
    }

    complete = QtCore.Signal()

    def __init__(
            self, parent=None, mode="scientific", sci_threshold=6, decimals=50, theme="block", history_view=False,
            saved_history="", rand_max=9999999, rand_min=0, logger: QtWidgets.QTextEdit | None = None,
            log_level: str | None = "ERROR"
    ):
        super().__init__(parent)
        self.ui = Ui_Form()
        self.ui.setupUi(self)

        self.mode = mode  # regular use python standard library. scientific use mpmath for binary arbitrary precision
        self.sci_threshold = sci_threshold
        self.decimals = decimals
        self.theme = theme  # there is one theme block. any other circular
        self.history_view = history_view  # if history view show expression with the result by outputting to console or text edit to display expressions.
        self.saved_history = saved_history
        self.rand_max = rand_max  # to adjust desired random ceiling to scenario
        self.rand_min = rand_min
        self.logger = logger.appendPlainText if logger else print
        self.log_level = log_level  # log level ERROR or DEBUG will print pre computed expression(s)

        self.preview_log = []  # holds each expression for logging or using alternative to eval

        self._dragPos = None
        self._pos = None
        # auto equals
        self.last_operator = None
        self.last_operand = None

        self.is_mpmath = False

        self.del_locked = True  # pending operator
        self.pending_paren = False  # pending paren # times ten power ect

        self.paren_count = 0

        self.memory = ""  # mr
        self.last_expression = ""  # handle function input if repeated hold so already built
        self.last_history_view = ""  # hold the results of any equations for user saving

        self.angle_mode = "DEG"

        # shown above display
        self.expression_text = ""
        # what gets displayed on main display line edit
        self.text = ""

        self.expression = self.ui.lineEdit2
        self.output = self.ui.lineEdit

        if mode == "regular":
            self.OUTPUT_LIMIT = 24
            self.MIN_FONT_SIZE = 30
            ranges = self.FONT_SIZE_RANGES

        else:
            ranges = self.FONT_SIZE_RANGES_MP
            try:
                import mpmath
                self.is_mpmath = True
                self.decimal_set(decimals)

            except ImportError:
                self.is_mpmath = False

        if 0 <= sci_threshold <= 20:
            self.SCI_THRESHOLD = sci_threshold

        self.FONT_SIZE_BY_LENGTH = {
            length: font_size
            for start, end, font_size in ranges
            for length in range(start, end + 1)
        }

        if self.is_mpmath:
            self.eval_namespace = self.build_namespace_mpmath()
            self.PI_DISPLAY = self.format_number(mpmath.pi)
            self.E_DISPLAY = self.format_number(mpmath.e)
        else:
            self.eval_namespace = self.build_namespace()
            self.PI_DISPLAY = self.format_number(math.pi)
            self.E_DISPLAY = self.format_number(math.e)

        self.trig_btn_base = {
            self.ui.sinButton: "sin",
            self.ui.cosButton: "cos",
            self.ui.tanButton: "tan",
        }

        # Map operator buttons to symbols
        self.operator_btn_list = [
            (self.ui.additionButton, "+"),
            (self.ui.subtractionButton, "-"),
            (self.ui.multiplicationButton, "*"),  # ×
            (self.ui.divisionButton, "/"),  # ÷
        ]

        # Set icons
        self.icon_config = {
            self.ui.closeButton: ["./Resources/calculator/close.svg", 30],
            self.ui.negateButton: ["./Resources/calculator/plus-minus-variant.svg", 40],
            self.ui.percentButton: ["./Resources/calculator/percent-solid.svg", 50],
        }

        self.initialize_ui()

        self.print_history(self.saved_history)  # print any saved history view

    def initialize_ui(self):
        if self.DECIMAL_DIGITS <= 55:
            self.setWindowFlags(QtCore.Qt.WindowType.FramelessWindowHint)
            self.setAttribute(QtCore.Qt.WidgetAttribute.WA_TranslucentBackground)
        self.expression.clear()
        self.expression.setEnabled(True)
        self.output.setText("0")
        self.output.setEnabled(True)
        self.setup_buttons()
        self.init_signal_slot()
        self.set_formatting()

    def setup_buttons(self):
        """ Set up buttons with icons and configurations """
        for btn, conf in self.icon_config.items():
            icon = QtGui.QIcon(conf[0])
            btn.setIcon(icon)
            btn.setIconSize(QtCore.QSize(conf[1], conf[1]))
        self.setFocus()
        self.ui.negateButton.setCheckable(True)
        self.ui.negateButton.setChecked(False)
        self.ui.angleButton.setText("DEG")

    def init_signal_slot(self):
        # self.output.setFocusPolicy(QtCore.Qt.FocusPolicy.NoFocus)
        self.output.textChanged.connect(self.clear_button_text)
        self.output.textChanged.connect(self.update_font_size)

        self.ui.angleButton.clicked.connect(self.cycle_angle_mode)
        self.ui.mrButton.clicked.connect(self.memory_store)
        self.ui.msButton.clicked.connect(self.memory_recall)

        self.ui.closeButton.clicked.connect(self.close)
        self.ui.delButton.clicked.connect(self.backspace)

        self.ui.button_frame.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
        self.ui.button_frame.customContextMenuRequested.connect(self.button_frame_alt_menu)

        # Connect all button clicks to main calculate method
        btn_list = self.ui.button_frame.findChildren(QtWidgets.QPushButton)
        for btn in btn_list:
            btn.clicked.connect(self.switch_board)

        # check toggle button
        self.ui.negateButton.toggled.connect(self.plus_minus)
        self.ui.functionButton.toggled.connect(self.function)
        self.ui.hypButton.toggled.connect(self.hyp)

    def set_formatting(self):

        self.output.setStyleSheet(f"font-size:{self.MAX_FONT_SIZE}px")

        # for pi symbol
        self.ui.piButton.setStyleSheet("""
            font-family: "Segoe UI";
        """)

        if self.theme == "block":
            self.set_format("block")

        # if folding the calculator
        if self.mode == "regular":

            grid = self.ui.button_frame.layout()

            # find current position before removing it
            idx = grid.indexOf(self.ui.lineEdit)
            _, _, _, colspan = grid.getItemPosition(idx)  # row, col, rowspan, colspan

            grid.removeWidget(self.ui.lineEdit)
            grid.addWidget(self.ui.lineEdit, 0, 4, 1, colspan - 4)  # start after hidden column

            for btn in self.ui.button_frame.findChildren(QtWidgets.QPushButton):
                if btn.property("class") == "btn_group_3":
                    btn.hide()

            self.ui.delButton.hide()

            # DRG
            # self.ui.angleButton.hide()
            # self.ui.mrButton.hide()
            # self.ui.msButton.hide()

            self.ui.lineEdit.setFixedWidth(445)
            self.resize(445, 600)  # self.adjustSize() #  stretches buttons

        # else:
        #     self.ui.lineEdit.setFixedWidth(905)

    # Helper functions or utility

    def set_format(self, theme):
        """ after created change the fmt """
        if theme == "block":
            self.ui.button_frame.layout().setSpacing(5)
            self.ui.button_frame.setStyleSheet(
                "#button_frame QPushButton { border-radius: 0px; }"
            )
        else:
            self.ui.button_frame.setStyleSheet(
                "#button_frame QPushButton { border-radius: 50px; }"
            )

    def decimal_set(self, decimals):
        """ scientific set the decimal precision """
        if self.is_mpmath:
            # max_decimals = self.OUTPUT_LIMIT - 2

            self.DECIMAL_DIGITS = decimals
            if decimals > self.DECIMAL_MAX:
                self.DECIMAL_DIGITS = self.DECIMAL_MAX
                self.OUTPUT_LIMIT = self.DECIMAL_DIGITS
                print(f"Max decimals set to {self.DECIMAL_MAX} exceeded OUTPUT_LIMIT {self.OUTPUT_LIMIT} given {decimals}")
            elif decimals < 20:
                self.DECIMAL_DIGITS = 20

            mpmath.mp.dps = self.DECIMAL_DIGITS + 30

    def print_history(self, saved_history):
        """ print last_history_view to logger
            on init. as well as saving hist
            to db.
            trims any trailing newline char """
        if saved_history:
            out_str = saved_history
            if saved_history.endswith("\n"):
                out_str = saved_history[:-1]
            self.logger(out_str)

    # end Helper functions or utility

    def build_namespace(self):
        return {
            "sin": self.fwd_trig(math.sin),
            "cos": self.fwd_trig(math.cos),
            "tan": self.fwd_trig(math.tan),
            "asin": self.inv_trig(math.asin),
            "acos": self.inv_trig(math.acos),
            "atan": self.inv_trig(math.atan),
            "sinh": math.sinh,
            "cosh": math.cosh,
            "tanh": math.tanh,
            "asinh": math.asinh,
            "acosh": math.acosh,
            "atanh": math.atanh,
            "cbrt": self.cbrt,
            "sqrt": math.sqrt,
            "sqr": self.sqr,
            "logbase": lambda x, y: math.log(x, y),
            "log": math.log10,
            "logtwo": math.log2,
            "ln": math.log,
            "pi": math.pi,
            "e": math.e,
            "fact": math.factorial,
            "rndg": random.gauss
        }

    def build_namespace_mpmath(self):
        return {
            "mpf": mpmath.mpf,
            "sin": self.fwd_trig_mpmath(mpmath.sin),
            "cos": self.fwd_trig_mpmath(mpmath.cos),
            "tan": self.fwd_trig_mpmath(mpmath.tan),
            "asin": self.inv_trig_mpmath(mpmath.asin),
            "acos": self.inv_trig_mpmath(mpmath.acos),
            "atan": self.inv_trig_mpmath(mpmath.atan),
            "sinh": mpmath.sinh,
            "cosh": mpmath.cosh,
            "tanh": mpmath.tanh,
            "asinh": mpmath.asinh,
            "acosh": mpmath.acosh,
            "atanh": mpmath.atanh,
            "cbrt": self.cbrt_mpmath,
            "sqrt": mpmath.sqrt,
            "sqr": self.sqr_mpmath,
            "logbase": lambda x, y: mpmath.log(mpmath.mpf(x)) / mpmath.log(mpmath.mpf(y)),
            "log": lambda x: mpmath.log10(mpmath.mpf(x)),
            "logtwo": lambda x: mpmath.log2(mpmath.mpf(x)),
            "ln": lambda x: mpmath.log(mpmath.mpf(x)),
            "pi": mpmath.pi,
            "e": mpmath.e,
            "fact": mpmath.factorial,
            "rndg": self.rndg_mpmath
        }

    def fwd_trig(self, func):
        def wrapped(x):
            if self.angle_mode == "DEG":
                x = math.radians(x)
            elif self.angle_mode == "GRAD":
                x = x * (math.pi / 200)
            return func(x)
        return wrapped

    def fwd_trig_mpmath(self, func):
        def wrapped(x):
            x = mpmath.mpf(x)
            if self.angle_mode == "DEG":
                x = x * (mpmath.pi / 180)
            elif self.angle_mode == "GRAD":
                x = x * (mpmath.pi / 200)
            return func(x)
        return wrapped

    def inv_trig(self, func):
        def wrapped(x):
            result = func(x)
            if self.angle_mode == "DEG":
                result = math.degrees(result)
            elif self.angle_mode == "GRAD":
                result = result * (200 / math.pi)
            return result
        return wrapped

    def inv_trig_mpmath(self, func):
        def wrapped(x):
            result = func(mpmath.mpf(x))
            if self.angle_mode == "DEG":
                result = result * (180 / mpmath.pi)
            elif self.angle_mode == "GRAD":
                result = result * (200 / mpmath.pi)
            return result
        return wrapped

    def cbrt(self, x):
        if x < 0:
            return -((-x) ** (1/3))
        return x ** (1/3)

    def cbrt_mpmath(self, x):
        x = mpmath.mpf(x)
        third = mpmath.mpf(1) / 3
        if x < 0:
            return -((-x) ** third)
        return x ** third

    def sqr(self, x):
        return x ** 2

    def sqr_mpmath(self, x):
        x = mpmath.mpf(x)
        return x ** 2

    def rndg_mpmath(self, mu, sigma):
        mu = mpmath.mpf(mu)
        sigma = mpmath.mpf(sigma)
        u1 = mpmath.rand()
        u2 = mpmath.rand()
        z = mpmath.sqrt(-2 * mpmath.log(u1)) * mpmath.cos(2 * mpmath.pi * u2)
        return mu + sigma * z

    # return an integer let mpmath handle scientific
    # then for float check if it fits on screen otherwise use scientific
    def get_mpmath_scientific(self, val):

        # original design
        # if val == int(val):
        #   int_str = str(int(val))
        #   if len(int_str) > self.OUTPUT_LIMIT:

        #       exp = len(int_str.lstrip('-')) - 1  # exp = mpmath.floor(mpmath.log10(abs(val)))  # by magnitude
        #       exp_len = len(f"e+{int(exp)}")  # account for notation chars

        #       sig_digits = max(self.OUTPUT_LIMIT - exp_len - sign_len - dec_len, 1)
        #       return mpmath.nstr(val, sig_digits, strip_zeros=True, min_fixed=0, max_fixed=0)
        #   return str(int(val))

        # int
        if val == mpmath.floor(val):
            sign_len = 1 if val < 0 else 0
            int_str = str(int(val))

            if len(int_str) <= self.OUTPUT_LIMIT:
                return int_str

            exp = len(int_str.lstrip('-')) - 1
            exp_len = len(f"e+{int(exp)}")

            sig_digits = max(self.OUTPUT_LIMIT - exp_len - sign_len - 1, 1)

            return mpmath.nstr(
                val,
                sig_digits,
                strip_zeros=True,
                min_fixed=0,
                max_fixed=0
            )

        abs_val = abs(val)

        if abs_val >= 1:
            int_digits = len(str(int(abs_val)))
            sig_digits = int_digits + self.DECIMAL_DIGITS
        else:
            exp = mpmath.floor(mpmath.log10(abs_val))
            leading_zeros = max(-int(exp) - 1, 0)

            if self.SCI_THRESHOLD != 0 and leading_zeros >= self.SCI_THRESHOLD:
                sig_digits = 0
            else:
                sig_digits = max(self.DECIMAL_DIGITS - leading_zeros, 1)

        # try to fit it in the screen unless scientific threshold is reached
        if sig_digits:
            fixed = mpmath.nstr(
                val,
                sig_digits,
                strip_zeros=True,
                min_fixed=-1e9,
                max_fixed=1e9,
            )

            if len(fixed) <= self.OUTPUT_LIMIT:
                return fixed

        # use scientific notation
        return mpmath.nstr(
            val,
            self.DECIMAL_DIGITS,  # mpmath.mp.dps
            strip_zeros=True,
            min_fixed=0,
            max_fixed=0
        )

    # if used later with log on closed paren for type casting
    def wrap_mpf(self, curr_text):
        if curr_text in ("π", "e", ""):
            return curr_text
        return f"mpf('{curr_text}')"

    @staticmethod
    def _round_sig(value, sig_digits):
        if value == 0:
            return 0.0
        magnitude = math.floor(math.log10(abs(value))) + 1
        decimal_places = max(sig_digits - magnitude, 0)
        return round(value, decimal_places)

    def format_number(self, value):

        if self.is_mpmath:
            val = mpmath.mpf(value)  # account for negative char
            return self.get_mpmath_scientific(val)

        # is it an int?
        number_float = float(value)
        number = int(number_float)
        if float(value) == number:
            int_str = str(number)
            if len(int_str) > self.OUTPUT_LIMIT:
                try:
                    return repr(float(value))
                except OverflowError:
                    self.logger(f"error overflow {value}")
                    return self.ERROR_MSG2
            return int_str
        # its a float and
        if number_float == 0:
            return "0"

        rounded = self._round_sig(number_float, self.FLOAT_SIG_DIGITS)

        abs_val = abs(rounded)
        exp = math.floor(math.log10(abs_val))  # this replaces "leading zeros"

        # convert to fixed string first
        fixed = str(rounded)

        # estimate leading zeros only for small numbers
        leading_zeros = max(-exp - 1, 0)

        if self.SCI_THRESHOLD != 0 and leading_zeros >= self.SCI_THRESHOLD:
            return f"{rounded:.{self.FLOAT_SIG_DIGITS}e}"

        return fixed

    def cycle_angle_mode(self):
        idx = self.ANGLE_MODES.index(self.angle_mode)
        self.angle_mode = self.ANGLE_MODES[(idx + 1) % len(self.ANGLE_MODES)]
        self.ui.angleButton.setText(self.angle_mode)

    def num_paste(self):
        text = QtGui.QGuiApplication.clipboard().text()

        try:
            float(text)
        except ValueError:
            window_message(self, "Invalid number")
            return

        self.text = text
        self.del_locked = False
        self.display_text()

    def memory_store(self):
        curr_text = self.output.text().replace(",", "")
        if curr_text == self.PI_DISPLAY:
            curr_text = "π"
        elif curr_text == self.E_DISPLAY:
            curr_text = "e"
        self.memory = curr_text

    def memory_recall(self):
        if self.memory is None:
            return
        if self.memory == "π":
            self.text = self.PI_DISPLAY
        elif self.memory == "e":
            self.text = self.E_DISPLAY
        else:
            self.text = self.memory
        self.del_locked = False
        self.display_text()

    def update_font_size(self):
        """ Adjust based on the length of the text """
        text = self.output.text()
        text_length = len(text)

        if text_length in self.FONT_SIZE_BY_LENGTH:
            self.output.setStyleSheet(f"font-size:{self.FONT_SIZE_BY_LENGTH.get(text_length)}px")
            # print("text len", text_length, "return size", self.FONT_SIZE_BY_LENGTH.get(text_length))  # debug
            return

        if text_length >= 20:
            self.output.setStyleSheet(f"font-size:{self.MIN_FONT_SIZE}px")
            # print("min text len", text_length, "return size", self.MIN_FONT_SIZE)  # debug
        else:
            self.output.setStyleSheet(f"font-size:{self.MAX_FONT_SIZE}px")
            # print("max text len", text_length, "return size", self.MAX_FONT_SIZE)  # debug

    def mousePressEvent(self, event: QtGui.QMouseEvent):
        """ Enable window dragging with mouse press """
        if event.button() == QtCore.Qt.MouseButton.LeftButton:
            self._dragPos = event.globalPosition().toPoint()
        # elif event.button() == QtCore.Qt.MouseButton.RightButton:
        #     self._dragPos = None
        #     local_pos = self.output.mapFrom(self, event.position().toPoint())
        #     if self.output.rect().contains(local_pos):
        #         self.output.selectAll()

    def mouseReleaseEvent(self, event: QtGui.QMouseEvent):
        """ Reset dragging position on mouse release """
        self._dragPos = None

    def mouseMoveEvent(self, event: QtGui.QMouseEvent):
        """ Handle window movement on mouse drag """
        if event.buttons() == QtCore.Qt.MouseButton.LeftButton and self._dragPos:
            self.move(self.pos() + event.globalPosition().toPoint() - self._dragPos)
            self._dragPos = event.globalPosition().toPoint()

    def closeEvent(self, event):
        self.complete.emit()
        super().closeEvent(event)

    def button_frame_alt_menu(self, _pos):
        menu = QtWidgets.QMenu(self)
        menu.addAction("rnd", partial(self.random, "RAND"))
        menu.addAction("rndint", partial(self.random, "RNDINT"))
        menu.addAction("rndg", partial(self.random, "RNDG"))
        menu.exec(self.ui.button_frame.mapToGlobal(_pos))

    def logline_out(self, text, logger, log_level):
        if log_level and log_level == "DEBUG":
            logger(text + '\n')

    def display_expression(self):
        return self.expression_text.replace("*", "×").replace("/", "÷")

    def display_text(self):
        if self.is_mpmath:
            self.display_mpmath()
        else:
            self.display_int_float()

    def display_int_float(self):

        self.expression.setText(self.display_expression())

        if self.text:

            # could be float or float in exponent form
            value = None
            try:
                value = float(self.text)
            except ValueError as e:
                if self.text != ".":
                    self.logger(e)
                    self.logger(f"display_int_float reference on {self.text}")
                    raise
            if "." in self.text or value and not value.is_integer():

                self.output.setText(f"{self.text}")

            # its a int
            elif "." not in self.text and "e" not in self.text:

                self.output.setText(f"{int(self.text):,}")

            else:
                self.output.setText(f"{self.text}")

        # show 0 on null
        else:
            self.output.setText("0")

    def display_mpmath(self):

        self.expression.setText(self.display_expression())

        if self.text and "." in self.text:

            try:
                _ = mpmath.mpf(self.text)
            except (ValueError, TypeError) as e:
                self.logger(e)
                self.logger(f"mpmath error not a parsable raw: {self.text}")
                self.output.setText(self.text)
                return

            int_part, dec_part = self.text.split(".", 1)
            sign = ""
            if int_part.startswith("-"):
                sign, int_part = "-", int_part[1:]
            int_part = int_part or "0"
            self.output.setText(f"{sign}{int(int_part):,}.{dec_part}")

        elif self.text and "." not in self.text:
            self.output.setText(f"{int(self.text):,}")

        else:
            self.output.setText("0")

    def switch_board(self):

        clicked_btn = self.sender()

        # Clear input
        if clicked_btn == self.ui.clearButton:
            self.clear_button()
        elif self.output.text() in (self.ERROR_MSG, self.ERROR_MSG2):
            return

        # Button input. dark grey
        elif clicked_btn in self.trig_btn_base:
            self.handle_trig_input(self.trig_btn_base[clicked_btn])
        elif clicked_btn == self.ui.lnButton:
            self.ln_button()  # eˣ or ln
        elif clicked_btn == self.ui.logBaseButton:
            self.logbase()  # x! or log(x)(y)
        elif clicked_btn == self.ui.logButton:
            self.log(self.ui.logButton.text())  # log₂ or log
        elif clicked_btn == self.ui.piButton:
            self.pi()
        elif clicked_btn == self.ui.powerButton:
            self.power()
        elif clicked_btn == self.ui.eButton:
            self.e()
        elif clicked_btn == self.ui.sqrtButton:
            self.sqrt()
        elif clicked_btn == self.ui.cubeRootButton:
            self.cube_root()
        elif clicked_btn == self.ui.squareButton:
            self.square()  # 2ˣ or x²
        elif clicked_btn == self.ui.reciprocalButton:
            self.reciprocal()
        elif clicked_btn == self.ui.rootButton:
            self.root()
        elif clicked_btn == self.ui.tenPowerButton:
            self.ten_power_button()  # 10ˣ or x10ˣ
        elif clicked_btn == self.ui.openParenButton:
            self.open_paren()
        elif clicked_btn == self.ui.closeParenButton:
            self.close_paren()
        # Number or decimal input. the grey
        elif clicked_btn.text() in self.DIGIT_VALUES:
            self.handle_digit_input(clicked_btn.text())
        elif clicked_btn == self.ui.percentButton:
            self.handle_operator_input("%")
        # Operator input. orange sans equals
        elif clicked_btn in [btn[0] for btn in self.operator_btn_list]:

            selected_operator = [btn for btn in self.operator_btn_list if clicked_btn == btn[0]]
            op = selected_operator[0][1]
            self.handle_operator_input(op)

        # Equals input. orange
        elif clicked_btn == self.ui.equalsButton:
            self.equals()

    def keyPressEvent(self, event):
        key_text = event.text()
        # exit on ctrl-c
        # print("keyPressEvent", self.sender(), QtWidgets.QApplication.focusWidget())  # debug
        if event.key() == QtCore.Qt.Key.Key_C and event.modifiers() & QtCore.Qt.KeyboardModifier.ControlModifier:
            QtWidgets.QApplication.quit()
            sys.exit(0)

        elif event.key() in (QtCore.Qt.Key_Escape, QtCore.Qt.Key_Delete):
            self.clear_button()
            return
        elif event.key() == QtCore.Qt.Key_Backspace:
            self.backspace()
            return
        elif key_text in self.DIGIT_VALUES:
            self.handle_digit_input(key_text)
            return
        elif key_text and key_text in "+-*/":
            self.handle_operator_input(key_text)
            return
        elif event.key() in (QtCore.Qt.Key_Return, QtCore.Qt.Key_Enter, QtCore.Qt.Key_Equal):
            self.equals()
            return
        # output retains focus after right-click copy, which swallows Ctrl+V otherwise
        elif QtWidgets.QApplication.focusWidget() is self.output or event.matches(QtGui.QKeySequence.StandardKey.Paste):
            self.num_paste()
            return

        super().keyPressEvent(event)

    def handle_trig_input(self, base):
        func_name = self.TRIG_MAP[(base, self.ui.functionButton.isChecked(), self.ui.hypButton.isChecked())]
        self.handle_function_input(func_name)

    def handle_digit_input(self, char):

        # new
        if self.del_locked:

            if char == "." and "." not in self.text:
                self.text = "0."
            elif char == ".":
                return
            else:
                self.text = char
            self.last_expression = None
            self.del_locked = False
            self.ui.negateButton.setChecked(False)
        # or append
        else:
            if char == "." and "." in self.text:
                return
            curr_text = self.output.text().replace(",", "")
            if len(curr_text) >= self.OUTPUT_LIMIT:
                return
            self.text = curr_text + char

        self.display_text()

    def resolve_percent_chains(self, expression, is_mpmath):
        """
        this calculator allows for the % to either be modulo or percent so some logic is needed
        Whenever a % is NOT followed by a digit or decimal point, treat as percent-conversion
        this is because eval does not recognize % as percent. only modulo.
        A % followed by a digit is left alone as modulo.
        """
        i = 0
        while i < len(expression):
            if expression[i] == "%":

                next_char = expression[i + 1] if i + 1 < len(expression) else ""
                if next_char.isdigit() or next_char == ".":
                    i += 1
                    continue
                else:

                    j = i - 1
                    while j >= 0 and (expression[j].isdigit() or expression[j] == "."):
                        j -= 1
                    num_str = expression[j + 1:i]
                    if num_str == "":
                        em = f"No number found before % at position {i}"
                        self.logger(em + "\n" + f"expression: \n {expression}")
                        raise ValueError(em)

                    if is_mpmath:
                        value = mpmath.mpf(num_str) / 100
                        logged_value = f"mpmath.mpf({num_str}) / 100"
                        value_str = mpmath.nstr(value, mpmath.mp.dps, strip_zeros=False)
                    else:
                        value = float(num_str) / 100
                        logged_value = num_str + " / 100"
                        value_str = str(value)

                    exp_a = expression[:j + 1]
                    exp_b = expression[i + 1:]

                    self.logline_out(exp_a + logged_value + exp_b, self.logger, self.log_level)

                    expression = exp_a + value_str + exp_b

                    i = j + 1 + len(value_str)
            else:
                i += 1

        return expression

    def resolve_operand(self, text, token, compute, namespaces, is_mpmath):
        """
        find and parse and then eval each convention before and evaluating main expression
        windows calc uses yroot this uses root. logbase is used which is same.
        """

        while token in text:
            idx = text.find(token)

            left = idx
            while left > 0 and (text[left-1].isdigit() or text[left-1] == "." or text[left-1] == "-"):
                left -= 1

            right = idx + len(token)
            while right < len(text) and (text[right].isdigit() or text[right] == "."):
                right += 1

            a = text[left:idx]
            b = text[idx+len(token):right]

            logged = compute(a, b)
            result = eval(logged, {"__builtins__": {}}, namespaces)

            result_str = mpmath.nstr(result, mpmath.mp.dps, strip_zeros=False) if is_mpmath else str(result)

            left = text[:left]
            right = text[right:]
            text = left + result_str + right
            logged_value = left + logged + right

            self.logline_out(logged_value, self.logger, self.log_level)

        return text

    def commit_pending_operand(self, suffix):
        curr_text = self.load_current_value()
        expression = curr_text + suffix
        self.expression_text += expression
        if suffix == "%":
            self.last_expression = expression
        self.del_locked = True

    def handle_operator_input(self, op):
        if self.del_locked and self.expression_text and self.expression_text[-1] == "(":
            # invalid to do anything
            return
        elif self.del_locked and self.expression_text and self.expression_text[-1] in "+-*/^":
            self.expression_text = self.expression_text[:-1] + op  # change of operator

        elif self.del_locked and self.expression_text and self.expression_text[-1] in "%)":
            self.expression_text += op  # it is valid to append

        else:
            self.commit_pending_operand(op)  # new expression
        self.expression.setText(self.display_expression())

    def handle_function_input(self, func_name, display_symbol=None, template="{symbol}({arg})"):
        symbol = display_symbol or func_name
        curr_text = self.output.text().replace(",", "")
        if self.del_locked and self.expression_text and self.expression_text[-1] in "%)":
            old_last = self.last_expression if self.last_expression else curr_text

            prefix = self.expression_text[:-len(old_last)] if old_last else self.expression_text

            self.last_expression = template.format(symbol=symbol, arg=old_last)

            self.expression_text = f"{prefix}{self.last_expression}"

        else:
            # auto multiply
            if not self.del_locked and self.expression_text and curr_text and self.expression_text[-1] not in "+-*/(":
                if not self.last_expression:
                    self.expression_text += "*"
            arg = curr_text

            self.last_expression = template.format(symbol=symbol, arg=arg)

            self.expression_text += self.last_expression

        self.del_locked = True

        self.expression.setText(self.display_expression())

    def substitute_expression(self, expression):
        resolved = expression.replace("π", "pi").replace("√", "sqrt").replace("∛", "cbrt").replace("^", "**")

        if self.is_mpmath:
            resolved = self.resolve_operand(
                resolved, "root", lambda x, y: f"(mpf('{x}'))**(1/(mpf('{y}')))", self.eval_namespace, self.is_mpmath
            )
        else:
            resolved = self.resolve_operand(
                resolved, "root", lambda x, y: f"({x})**(1/({y}))", self.eval_namespace, self.is_mpmath
            )

        resolved = self.resolve_operand(resolved, "logbase", lambda x, y: f"logbase({x},{y})", self.eval_namespace, self.is_mpmath)

        resolved = self.resolve_operand(resolved, "rndg", lambda x, y: f"rndg({x},{y})", self.eval_namespace, self.is_mpmath)

        resolved = self.resolve_percent_chains(resolved, self.is_mpmath)  # percent modulo

        # cast types if in decimal arbitrary precision
        if self.is_mpmath:
            resolved = str_to_mpmath(resolved)
        return resolved

    def load_current_value(self):
        ct = self.output.text().replace(",", "")
        if ct == self.PI_DISPLAY:
            ct = "π"
        elif ct == self.E_DISPLAY:
            ct = "e"
        return ct

    def equals(self):

        curr_text = self.load_current_value()
        resolved_expression = ""
        step = ""

        # cleared state
        if self.expression_text == "" and curr_text in ("0", ""):
            return
        # expression was commited and nothing was pressed
        elif self.del_locked and self.expression_text and not self.pending_paren and self.expression_text[-1] in "+-*/^(":
            return

        # build the expression

        # repeat
        elif self.expression_text == "" and self.last_operator is not None and self.del_locked:

            full_expression = curr_text + self.last_operator + self.last_operand

        # normal
        else:

            full_expression = self.expression_text

            if self.pending_paren:
                if self.expression_text and self.expression_text[-1] not in "+-*/":
                    if self.expression_text[-1] not in ")":
                        full_expression = full_expression + curr_text
                    for x in range(self.paren_count):
                        full_expression = full_expression + ")"
                    self.paren_count = 0
                elif self.expression_text:
                    if self.expression_text[-1] not in ")":
                        full_expression = self.expression_text + curr_text
                self.pending_paren = False
            # normal
            else:

                if not self.del_locked:

                    # auto multiply
                    if self.expression_text and self.expression_text[-1] in ")" and curr_text:

                        full_expression = self.expression_text + "*" + curr_text
                    else:
                        full_expression = self.expression_text + curr_text

            # save for potential repeat
            if self.expression_text and self.expression_text[-1] in "+-*/":
                self.last_operator = self.expression_text[-1]
                self.last_operand = curr_text

        self.logline_out(full_expression, self.logger, self.log_level)

        # evaluate expression

        try:

            # swap out necessary symbols and conventions for eval

            step = "substiting"
            resolved_expression = self.substitute_expression(full_expression)

            self.logline_out(resolved_expression, self.logger, self.log_level)

            step = "eval"

            result = eval(resolved_expression, {"__builtins__": {}}, self.eval_namespace)

            self.expression_text = full_expression + "="

            step = "format"
            self.text = self.format_number(result)

            # show result

            step = "display"
            self.display_text()
            if self.history_view:

                if self.log_level != "DEBUG":
                    self.logger(self.expression_text)
                    self.last_history_view += self.expression_text + "\n" + self.text + "\n\n"

                self.logger(self.text + "\n")

            # reset
            self.ui.negateButton.blockSignals(True)
            self.ui.negateButton.setChecked(False)
            self.ui.negateButton.blockSignals(False)

            self.expression_text = ""  # for auto repeat

            self.ui.clearButton.setText("AC")

        except OverflowError:
            self.output.setText(self.ERROR_MSG2)
        except Exception as e:
            self.output.setText(self.ERROR_MSG)
            self.logger(f'equals expression step: {step} err: {e}')
            if self.log_level != "DEBUG":
                self.logger("expression \n" + resolved_expression)

        self.del_locked = True

    # button logic

    def close_paren(self):

        open_count = self.expression_text.count("(")
        close_count = self.expression_text.count(")")
        if open_count <= close_count:
            return
        if self.pending_paren:
            curr_text = self.output.text().replace(",", "")
            self.expression_text += curr_text + ")"
            self.paren_count -= 1

        else:

            if self.del_locked and self.expression_text and self.expression_text[-1] == ")":
                self.expression_text += ")"
            else:
                self.commit_pending_operand(")")

        self.log_closed_paren()

        self.expression.setText(self.display_expression())

    def find_matching_open(self, expression_text):
        """Index of the ( matching the ) just appended or the last char of expression_text"""
        depth = 0
        # -2 skip the ) itself
        for i in range(len(expression_text) - 2, -1, -1):
            ch = expression_text[i]
            if ch == ")":
                depth += 1
            elif ch == "(":
                if depth == 0:
                    return i
                depth -= 1
        return -1

    def log_closed_paren(self):
        expression_text = self.expression_text
        open_idx = self.find_matching_open(expression_text)
        if open_idx == -1:
            self.logger("log_closed_paren failed to find matching open paren")
            return

        sub_expr = expression_text[open_idx:]  # from matching ( to )
        self.last_expression = sub_expr

        try:

            resolved = self.substitute_expression(sub_expr)

            # if having to diagnose unknown results this area can help track each stage
            # self.logger("on paren close sub_expr to resolved \n")
            # self.logline_out(sub_expr, self.logger, self.log_level)
            # self.logline_out(resolved, self.logger, self.log_level)

            result = eval(resolved, {"__builtins__": {}}, self.eval_namespace)

            self.preview_log.append((sub_expr, result))

        except Exception as e:
            self.logger(f"error in sub expression: \n {sub_expr!r} -> {e}")

    def open_paren(self):

        if not self.del_locked and self.output.text().replace(",", ""):
            self.commit_pending_operand("*")
        elif self.expression_text and self.expression_text[-1] in self.PAREN_TRIGGER_CHARS:
            self.expression_text += "*"
        self.expression_text += "("
        self.expression.setText(self.display_expression())

    # called on text change
    def clear_button_text(self):

        if self.output.text() != "0":
            self.ui.clearButton.setText("C")
        else:
            self.ui.clearButton.setText("AC")

    def clear_button(self):
        """ When C is pressed reset to initial state """
        self.preview_log = []

        self.last_expression = ""
        self.last_operator = None
        self.last_operand = None

        self.expression_text = ""
        self.expression.clear()

        self.text = ""
        self.display_text()

        self.clear_button_text()

    def backspace(self):
        if self.del_locked:
            return
        result = self.output.text().replace(",", "")
        if len(result) > 1:
            new_result = result[:-1]
            if new_result in ("-", ""):  # guard against invalid intermediate states
                self.text = "0"
            elif new_result.endswith("."):
                self.text = new_result[:-1]
                if len(self.text) < 1:
                    self.text = "0"
            else:
                self.text = self.format_number(new_result)
        else:
            self.text = "0"
        self.display_text()

    def random(self, kind):

        if kind == "RNDG":
            self.commit_pending_operand("rndg")
            self.expression.setText(self.display_expression())

        else:
            if kind == "RAND":
                self.text = str(mpmath.rand()) if self.is_mpmath else str(random.random())
                # digits = random.randint(1, 16)
                # self.text = str(random.randint(0, 10**digits - 1))
            elif kind == "RNDINT":
                self.text = str(random.randint(self.rand_min, self.rand_max))

            self.del_locked = False
            self.display_text()

    def percent(self):
        curr_text = self.output.text().replace(",", "")

        value = mpmath.mpf(curr_text) / 100 if self.is_mpmath else float(curr_text) / 100
        self.text = self.format_number(value)
        self.display_text()

    # checkable
    def plus_minus(self, state):
        if self.output.text().replace(",", "") not in (self.ERROR_MSG, self.ERROR_MSG2):
            if state:
                self.text = f"-{self.output.text()}".replace(",", "")
            else:
                self.text = self.output.text()[1:].replace(",", "")
            self.display_text()

    # checkable toggles
    def hyp(self, state):
        self.function_hyp(self.ui.functionButton.isChecked(), state)

    def function(self, state):

        if state:
            self.ui.squareButton.setText("2ˣ")
            self.ui.reciprocalButton.setText("n!")
            # self.ui.logBaseButton.setText("n!")
            self.ui.logButton.setText("log₂")
            self.ui.lnButton.setText("eˣ")
            self.ui.tenPowerButton.setText("10ˣ")
        else:
            self.ui.squareButton.setText("x²")
            # self.ui.logBaseButton.setText("log(y)(x)")
            self.ui.reciprocalButton.setText("1/x")
            self.ui.logButton.setText("log")
            self.ui.lnButton.setText("ln")
            self.ui.tenPowerButton.setText("x10ˣ")

        self.function_hyp(state, self.ui.hypButton.isChecked())

    def function_hyp(self, function_state, hyp_state):
        for base, btn in (("sin", self.ui.sinButton), ("cos", self.ui.cosButton), ("tan", self.ui.tanButton)):
            func_name = self.TRIG_MAP[(base, function_state, hyp_state)]
            btn.setText(self.LABEL_MAP[func_name])

    # convention
    def logbase(self):
        self.commit_pending_operand("logbase")
        self.expression.setText(self.display_expression())

    def root(self):
        self.commit_pending_operand("root")
        self.expression.setText(self.display_expression())

    # constant
    def e(self):
        self.text = self.E_DISPLAY
        self.del_locked = False
        self.display_text()

    def pi(self):
        self.text = self.PI_DISPLAY
        self.del_locked = False
        self.display_text()

    # operator
    def power(self):
        self.handle_operator_input("^")

    # function
    def ln_button(self):
        if self.ui.lnButton.text() == "eˣ":
            self.handle_function_input(None, template="e^({arg})")
        else:
            self.handle_function_input("ln")

    def log(self, text):
        self.handle_function_input(text.replace("₂", "two"))

    def reciprocal(self):
        if self.ui.reciprocalButton.text() == "n!":
            self.handle_function_input("fact")
        else:
            self.handle_function_input(None, template="1/({arg})")

    def cube_root(self):
        self.handle_function_input("cbrt", display_symbol="∛")

    def sqrt(self):
        self.handle_function_input("sqrt", display_symbol="√")

    def square(self):
        if self.ui.squareButton.text() == "2ˣ":
            self.handle_function_input(None, template="2^({arg})")
        else:
            self.handle_function_input("sqr")

    def ten_power_button(self):
        if self.ui.tenPowerButton.text() == "10ˣ":
            self.ten_power()
        else:
            self.times_ten_power()

    def ten_power(self):
        self.handle_function_input(None, template="10^({arg})")

    def times_ten_power(self):
        self.handle_function_input(None, template="{arg}*10^(")
        self.paren_count += 1
        self.pending_paren = True


if __name__ == '__main__':

    app = QtWidgets.QApplication(sys.argv)
    window = SCalculator()
    window.show()
    sys.exit(app.exec())
