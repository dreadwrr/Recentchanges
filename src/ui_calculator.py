# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'new_main_new.ui'
##
## Created by: Qt User Interface Compiler version 6.11.1
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PySide6.QtCore import (QCoreApplication, QDate, QDateTime, QLocale,
    QMetaObject, QObject, QPoint, QRect,
    QSize, QTime, QUrl, Qt)
from PySide6.QtGui import (QBrush, QColor, QConicalGradient, QCursor,
    QFont, QFontDatabase, QGradient, QIcon,
    QImage, QKeySequence, QLinearGradient, QPainter,
    QPalette, QPixmap, QRadialGradient, QTransform)
from PySide6.QtWidgets import (QApplication, QFrame, QGridLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QSizePolicy,
    QSpacerItem, QWidget)

class Ui_Form(object):
    def setupUi(self, Form):
        if not Form.objectName():
            Form.setObjectName(u"Form")
        Form.resize(945, 864)
        Form.setStyleSheet(u"#closeButton {\n"
"	background-color: #000;\n"
"	\n"
"	\n"
"}\n"
"#mrButton {\n"
"	background-color: #000;\n"
"	border: none;\n"
"	\n"
"}\n"
"#mrButton:hover {\n"
"	background-color: #555;\n"
"	border: none;\n"
"	\n"
"}\n"
"#mrButton:pressed {\n"
"	background-color: #fff;\n"
"	\n"
"}\n"
"\n"
"#msButton {\n"
"	background-color: #000;\n"
"	border: none;\n"
"	\n"
"}\n"
"\n"
"#msButton:hover {\n"
"	background-color: #555;\n"
"	\n"
"}\n"
"#msButton:pressed {\n"
"	background-color: #fff;\n"
"	\n"
"}\n"
"#rndButton {\n"
"	background-color: #000;\n"
"	border: none;\n"
"	\n"
"}\n"
"\n"
"#rndButton:hover {\n"
"	background-color: #555;\n"
"	\n"
"}\n"
"#rndButton:pressed {\n"
"	background-color: #fff;\n"
"	\n"
"}\n"
"#angleButton{\n"
"	background-color: #000;\n"
"	border: none;\n"
"}\n"
"\n"
"/* Styles for any QFrame and QMainWindow widgets to have a black background */\n"
"QFrame {\n"
"    background-color: #000000; /* Sets the background color to black */\n"
"}\n"
"\n"
"QLabel {\n"
"    font-size: 25px; /* Sets font siz"
                        "e to 50 pixels */\n"
"    font-family: \"Helvetica\"; /* Sets font family to Helvetica */\n"
" 	color: #fff;\n"
"}\n"
"\n"
"/* Styles for the widget with objectName 'function_frame' */\n"
"#function_frame {\n"
"    border-bottom: 1px solid #333333; /* Sets a dark grey border, 2 pixels thick */\n"
"	border-top-left-radius: 15px;\n"
"	border-top-right-radius: 15px;\n"
"}\n"
"\n"
"#function_frame QLineEdit {\n"
"    background-color: #000000;\n"
"    color: #888888;          /* dimmer grey, secondary emphasis */\n"
"    border: none;\n"
"    height: 40px;\n"
"    padding: 5px 5px;\n"
"    font-size: 32px;         /* smaller than main display */\n"
"    font-family: \"Segoe UI\";\n"
"}\n"
"\n"
"#button_frame {\n"
"	border-bottom-left-radius: 15px;\n"
"	border-bottom-right-radius: 15px;\n"
"}\n"
"\n"
"#button_frame QLineEdit {\n"
"    background-color: #000000; /* Sets background color to black */\n"
"    color: #FFF; /* Sets text color to white */\n"
"    border: none;\n"
"    height: 100px;\n"
"    padding: 10px "
                        "5px;\n"
"    font-size: 100px;\n"
"    font-family: \"Segoe UI\";\n"
"}\n"
"\n"
"#button_frame QPushButton {\n"
"    background-color: #333333; /* Sets button background to dark grey */\n"
"    color: #FFF; /* Sets button text color to white */\n"
"    font-size: 50px; /* Sets font size to 50 pixels */\n"
"    font-family: \"Helvetica\"; /* Sets font family to Helvetica */\n"
"    border-radius: 50%; /* Makes the button round */\n"
"    width: 100px; /* Sets the button width */\n"
"    height: 100px; /* Sets the button height */\n"
"\n"
"}\n"
"\n"
"#button_frame QPushButton:pressed {\n"
"    background-color: #555; /* Darkens the button background when pressed */\n"
"}\n"
"\n"
"#button_frame #zeroButton {\n"
"    text-align: left; /* Aligns the text to the left */\n"
"    padding-left: 35px; /* Adds left padding for the text inside the button */\n"
"}\n"
"\n"
"#button_frame QPushButton[class=\"btn_group_1\"] {\n"
"    background: #a5a5a5; /* Sets the background color to a light grey */\n"
"    color: #020202; "
                        "/* Sets the text color to a very dark grey (almost black) */\n"
"    font-size: 45px; /* Sets the font size to 45 pixels */\n"
"\n"
"}\n"
"\n"
"#button_frame QPushButton[class=\"btn_group_1\"]:pressed {\n"
"    background: #d6d6d6;\n"
"}\n"
"\n"
"#button_frame QPushButton[class=\"btn_group_2\"] {\n"
"    background-color: #ca3401; /* Sets the background color to a red-orange shade */\n"
"    font-size: 60px; /* Sets the font size to 60 pixels */\n"
"}\n"
"\n"
"/* #button_frame QPushButton[class=\"btn_group_2\"]:checked, */\n"
"#button_frame QPushButton[class=\"btn_group_2\"]:pressed {\n"
"    background-color: #fff;\n"
"  	color: #ca3401;\n"
"}\n"
"\n"
"#button_frame QPushButton[class=\"btn_group_3\"] {\n"
"	background-color: #1C1C1C; /* Sets button background to dark grey */\n"
"    font-size: 25px; /* Sets the font size to 60 pixels */\n"
"	\n"
"\n"
"}\n"
"\n"
"#button_frame QPushButton[class=\"btn_group_3\"]:checked,\n"
"#button_frame QPushButton[class=\"btn_group_3\"]:pressed {\n"
"	background-color: #555;"
                        "\n"
"}")
        self.gridLayout_3 = QGridLayout(Form)
        self.gridLayout_3.setSpacing(0)
        self.gridLayout_3.setObjectName(u"gridLayout_3")
        self.gridLayout_3.setContentsMargins(0, 0, 0, 0)
        self.function_frame = QFrame(Form)
        self.function_frame.setObjectName(u"function_frame")
        sizePolicy = QSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.function_frame.sizePolicy().hasHeightForWidth())
        self.function_frame.setSizePolicy(sizePolicy)
        self.function_frame.setFrameShape(QFrame.Shape.StyledPanel)
        self.function_frame.setFrameShadow(QFrame.Shadow.Raised)
        self.gridLayout_2 = QGridLayout(self.function_frame)
        self.gridLayout_2.setObjectName(u"gridLayout_2")
        self.gridLayout_2.setContentsMargins(20, -1, 10, -1)
        self.horizontalSpacer_2 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.gridLayout_2.addItem(self.horizontalSpacer_2, 0, 4, 1, 1)

        self.closeButton = QPushButton(self.function_frame)
        self.closeButton.setObjectName(u"closeButton")
        self.closeButton.setStyleSheet(u"")

        self.gridLayout_2.addWidget(self.closeButton, 0, 5, 1, 1)

        self.lineEdit2 = QLineEdit(self.function_frame)
        self.lineEdit2.setObjectName(u"lineEdit2")
        self.lineEdit2.setEnabled(False)
        sizePolicy1 = QSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        sizePolicy1.setHorizontalStretch(0)
        sizePolicy1.setVerticalStretch(0)
        sizePolicy1.setHeightForWidth(self.lineEdit2.sizePolicy().hasHeightForWidth())
        self.lineEdit2.setSizePolicy(sizePolicy1)
        self.lineEdit2.setAlignment(Qt.AlignmentFlag.AlignRight|Qt.AlignmentFlag.AlignTrailing|Qt.AlignmentFlag.AlignVCenter)
        self.lineEdit2.setReadOnly(True)

        self.gridLayout_2.addWidget(self.lineEdit2, 2, 0, 1, 6)

        self.horizontalLayout = QHBoxLayout()
        self.horizontalLayout.setObjectName(u"horizontalLayout")
        self.angleButton = QPushButton(self.function_frame)
        self.angleButton.setObjectName(u"angleButton")
        self.angleButton.setMinimumSize(QSize(31, 0))
        self.angleButton.setStyleSheet(u"")

        self.horizontalLayout.addWidget(self.angleButton)

        self.msButton = QPushButton(self.function_frame)
        self.msButton.setObjectName(u"msButton")
        self.msButton.setStyleSheet(u"")

        self.horizontalLayout.addWidget(self.msButton)

        self.mrButton = QPushButton(self.function_frame)
        self.mrButton.setObjectName(u"mrButton")
        self.mrButton.setStyleSheet(u"")

        self.horizontalLayout.addWidget(self.mrButton)


        self.gridLayout_2.addLayout(self.horizontalLayout, 1, 0, 1, 2)

        self.label_2 = QLabel(self.function_frame)
        self.label_2.setObjectName(u"label_2")
        self.label_2.setMinimumSize(QSize(20, 20))
        self.label_2.setMaximumSize(QSize(20, 20))
        self.label_2.setPixmap(QPixmap(u"Resources/calculator/calculator.svg"))
        self.label_2.setScaledContents(True)

        self.gridLayout_2.addWidget(self.label_2, 0, 0, 1, 1)

        self.label = QLabel(self.function_frame)
        self.label.setObjectName(u"label")

        self.gridLayout_2.addWidget(self.label, 0, 1, 1, 1)

        self.verticalSpacer = QSpacerItem(20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding)

        self.gridLayout_2.addItem(self.verticalSpacer, 1, 2, 1, 1)


        self.gridLayout_3.addWidget(self.function_frame, 0, 0, 1, 1)

        self.button_frame = QFrame(Form)
        self.button_frame.setObjectName(u"button_frame")
        self.button_frame.setFrameShape(QFrame.Shape.StyledPanel)
        self.button_frame.setFrameShadow(QFrame.Shadow.Raised)
        self.gridLayout = QGridLayout(self.button_frame)
        self.gridLayout.setSpacing(15)
        self.gridLayout.setObjectName(u"gridLayout")
        self.gridLayout.setContentsMargins(20, 20, 20, 20)
        self.logButton = QPushButton(self.button_frame)
        self.logButton.setObjectName(u"logButton")

        self.gridLayout.addWidget(self.logButton, 3, 3, 1, 1)

        self.multiplicationButton = QPushButton(self.button_frame)
        self.multiplicationButton.setObjectName(u"multiplicationButton")
        self.multiplicationButton.setStyleSheet(u"")
        self.multiplicationButton.setCheckable(False)
        self.multiplicationButton.setAutoExclusive(False)

        self.gridLayout.addWidget(self.multiplicationButton, 2, 7, 1, 1)

        self.piButton = QPushButton(self.button_frame)
        self.piButton.setObjectName(u"piButton")

        self.gridLayout.addWidget(self.piButton, 5, 2, 1, 1)

        self.subtractionButton = QPushButton(self.button_frame)
        self.subtractionButton.setObjectName(u"subtractionButton")
        self.subtractionButton.setStyleSheet(u"")
        self.subtractionButton.setCheckable(False)
        self.subtractionButton.setAutoExclusive(False)

        self.gridLayout.addWidget(self.subtractionButton, 3, 7, 1, 1)

        self.pushButton_9 = QPushButton(self.button_frame)
        self.pushButton_9.setObjectName(u"pushButton_9")
        self.pushButton_9.setLayoutDirection(Qt.LayoutDirection.LeftToRight)

        self.gridLayout.addWidget(self.pushButton_9, 2, 6, 1, 1)

        self.powerButton = QPushButton(self.button_frame)
        self.powerButton.setObjectName(u"powerButton")

        self.gridLayout.addWidget(self.powerButton, 2, 0, 1, 1)

        self.pushButton_1 = QPushButton(self.button_frame)
        self.pushButton_1.setObjectName(u"pushButton_1")

        self.gridLayout.addWidget(self.pushButton_1, 4, 4, 1, 1)

        self.squareButton = QPushButton(self.button_frame)
        self.squareButton.setObjectName(u"squareButton")

        self.gridLayout.addWidget(self.squareButton, 2, 1, 1, 1)

        self.cosButton = QPushButton(self.button_frame)
        self.cosButton.setObjectName(u"cosButton")

        self.gridLayout.addWidget(self.cosButton, 2, 2, 1, 1)

        self.pushButton_6 = QPushButton(self.button_frame)
        self.pushButton_6.setObjectName(u"pushButton_6")

        self.gridLayout.addWidget(self.pushButton_6, 3, 6, 1, 1)

        self.delButton = QPushButton(self.button_frame)
        self.delButton.setObjectName(u"delButton")

        self.gridLayout.addWidget(self.delButton, 1, 3, 1, 1)

        self.pushButton_7 = QPushButton(self.button_frame)
        self.pushButton_7.setObjectName(u"pushButton_7")

        self.gridLayout.addWidget(self.pushButton_7, 2, 4, 1, 1)

        self.rootButton = QPushButton(self.button_frame)
        self.rootButton.setObjectName(u"rootButton")

        self.gridLayout.addWidget(self.rootButton, 3, 0, 1, 1)

        self.equalsButton = QPushButton(self.button_frame)
        self.equalsButton.setObjectName(u"equalsButton")
        self.equalsButton.setStyleSheet(u"")

        self.gridLayout.addWidget(self.equalsButton, 5, 7, 1, 1)

        self.pushButton_4 = QPushButton(self.button_frame)
        self.pushButton_4.setObjectName(u"pushButton_4")

        self.gridLayout.addWidget(self.pushButton_4, 3, 4, 1, 1)

        self.pushButton_3 = QPushButton(self.button_frame)
        self.pushButton_3.setObjectName(u"pushButton_3")

        self.gridLayout.addWidget(self.pushButton_3, 4, 6, 1, 1)

        self.percentButton = QPushButton(self.button_frame)
        self.percentButton.setObjectName(u"percentButton")
        font = QFont()
        font.setFamilies([u"Helvetica"])
        self.percentButton.setFont(font)
        self.percentButton.setStyleSheet(u"")
        icon = QIcon()
        icon.addFile(u"Resources/calculator/percent-solid.svg", QSize(), QIcon.Mode.Normal, QIcon.State.Off)
        self.percentButton.setIcon(icon)
        self.percentButton.setIconSize(QSize(50, 50))

        self.gridLayout.addWidget(self.percentButton, 1, 6, 1, 1)

        self.pushButton_dec = QPushButton(self.button_frame)
        self.pushButton_dec.setObjectName(u"pushButton_dec")

        self.gridLayout.addWidget(self.pushButton_dec, 5, 6, 1, 1)

        self.sqrtButton = QPushButton(self.button_frame)
        self.sqrtButton.setObjectName(u"sqrtButton")

        self.gridLayout.addWidget(self.sqrtButton, 3, 1, 1, 1)

        self.pushButton_8 = QPushButton(self.button_frame)
        self.pushButton_8.setObjectName(u"pushButton_8")

        self.gridLayout.addWidget(self.pushButton_8, 2, 5, 1, 1)

        self.cubeRootButton = QPushButton(self.button_frame)
        self.cubeRootButton.setObjectName(u"cubeRootButton")

        self.gridLayout.addWidget(self.cubeRootButton, 4, 1, 1, 1)

        self.negateButton = QPushButton(self.button_frame)
        self.negateButton.setObjectName(u"negateButton")
        self.negateButton.setFont(font)
        self.negateButton.setStyleSheet(u"qproperty-iconSize: 40px;")
        icon1 = QIcon()
        icon1.addFile(u"Resources/calculator/plus-minus-variant.svg", QSize(), QIcon.Mode.Normal, QIcon.State.Off)
        self.negateButton.setIcon(icon1)
        self.negateButton.setIconSize(QSize(40, 40))

        self.gridLayout.addWidget(self.negateButton, 1, 5, 1, 1)

        self.sinButton = QPushButton(self.button_frame)
        self.sinButton.setObjectName(u"sinButton")

        self.gridLayout.addWidget(self.sinButton, 1, 2, 1, 1)

        self.tenPowerButton = QPushButton(self.button_frame)
        self.tenPowerButton.setObjectName(u"tenPowerButton")

        self.gridLayout.addWidget(self.tenPowerButton, 5, 3, 1, 1)

        self.functionButton = QPushButton(self.button_frame)
        self.functionButton.setObjectName(u"functionButton")
        self.functionButton.setCheckable(True)
        self.functionButton.setAutoExclusive(False)

        self.gridLayout.addWidget(self.functionButton, 1, 0, 1, 1)

        self.lnButton = QPushButton(self.button_frame)
        self.lnButton.setObjectName(u"lnButton")

        self.gridLayout.addWidget(self.lnButton, 4, 3, 1, 1)

        self.divisionButton = QPushButton(self.button_frame)
        self.divisionButton.setObjectName(u"divisionButton")
        self.divisionButton.setLayoutDirection(Qt.LayoutDirection.LeftToRight)
        self.divisionButton.setStyleSheet(u"")
        self.divisionButton.setLocale(QLocale(QLocale.English, QLocale.UnitedStatesOutlyingIslands))
        self.divisionButton.setCheckable(False)
        self.divisionButton.setAutoExclusive(False)

        self.gridLayout.addWidget(self.divisionButton, 1, 7, 1, 1)

        self.zeroButton = QPushButton(self.button_frame)
        self.zeroButton.setObjectName(u"zeroButton")
        sizePolicy2 = QSizePolicy(QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Preferred)
        sizePolicy2.setHorizontalStretch(0)
        sizePolicy2.setVerticalStretch(0)
        sizePolicy2.setHeightForWidth(self.zeroButton.sizePolicy().hasHeightForWidth())
        self.zeroButton.setSizePolicy(sizePolicy2)
        self.zeroButton.setLayoutDirection(Qt.LayoutDirection.LeftToRight)
        self.zeroButton.setStyleSheet(u"")

        self.gridLayout.addWidget(self.zeroButton, 5, 4, 1, 2)

        self.reciprocalButton = QPushButton(self.button_frame)
        self.reciprocalButton.setObjectName(u"reciprocalButton")

        self.gridLayout.addWidget(self.reciprocalButton, 4, 0, 1, 1)

        self.eButton = QPushButton(self.button_frame)
        self.eButton.setObjectName(u"eButton")

        self.gridLayout.addWidget(self.eButton, 4, 2, 1, 1)

        self.openParenButton = QPushButton(self.button_frame)
        self.openParenButton.setObjectName(u"openParenButton")

        self.gridLayout.addWidget(self.openParenButton, 5, 0, 1, 1)

        self.pushButton_5 = QPushButton(self.button_frame)
        self.pushButton_5.setObjectName(u"pushButton_5")

        self.gridLayout.addWidget(self.pushButton_5, 3, 5, 1, 1)

        self.hypButton = QPushButton(self.button_frame)
        self.hypButton.setObjectName(u"hypButton")
        self.hypButton.setCheckable(True)
        self.hypButton.setAutoExclusive(False)

        self.gridLayout.addWidget(self.hypButton, 1, 1, 1, 1)

        self.closeParenButton = QPushButton(self.button_frame)
        self.closeParenButton.setObjectName(u"closeParenButton")

        self.gridLayout.addWidget(self.closeParenButton, 5, 1, 1, 1)

        self.logBaseButton = QPushButton(self.button_frame)
        self.logBaseButton.setObjectName(u"logBaseButton")

        self.gridLayout.addWidget(self.logBaseButton, 2, 3, 1, 1)

        self.additionButton = QPushButton(self.button_frame)
        self.additionButton.setObjectName(u"additionButton")
        self.additionButton.setStyleSheet(u"")
        self.additionButton.setCheckable(False)
        self.additionButton.setAutoExclusive(False)

        self.gridLayout.addWidget(self.additionButton, 4, 7, 1, 1)

        self.pushButton_2 = QPushButton(self.button_frame)
        self.pushButton_2.setObjectName(u"pushButton_2")

        self.gridLayout.addWidget(self.pushButton_2, 4, 5, 1, 1)

        self.clearButton = QPushButton(self.button_frame)
        self.clearButton.setObjectName(u"clearButton")
        self.clearButton.setStyleSheet(u"")

        self.gridLayout.addWidget(self.clearButton, 1, 4, 1, 1)

        self.tanButton = QPushButton(self.button_frame)
        self.tanButton.setObjectName(u"tanButton")

        self.gridLayout.addWidget(self.tanButton, 3, 2, 1, 1)

        self.lineEdit = QLineEdit(self.button_frame)
        self.lineEdit.setObjectName(u"lineEdit")
        self.lineEdit.setEnabled(False)
        sizePolicy1.setHeightForWidth(self.lineEdit.sizePolicy().hasHeightForWidth())
        self.lineEdit.setSizePolicy(sizePolicy1)
        self.lineEdit.setLayoutDirection(Qt.LayoutDirection.LeftToRight)
        self.lineEdit.setAlignment(Qt.AlignmentFlag.AlignRight|Qt.AlignmentFlag.AlignTrailing|Qt.AlignmentFlag.AlignVCenter)
        self.lineEdit.setReadOnly(True)

        self.gridLayout.addWidget(self.lineEdit, 0, 0, 1, 8)


        self.gridLayout_3.addWidget(self.button_frame, 1, 0, 1, 1)


        self.retranslateUi(Form)

        QMetaObject.connectSlotsByName(Form)
    # setupUi

    def retranslateUi(self, Form):
        Form.setWindowTitle(QCoreApplication.translate("Form", u"Form", None))
        self.closeButton.setText("")
        self.lineEdit2.setText(QCoreApplication.translate("Form", u"1000 x", None))
        self.angleButton.setText(QCoreApplication.translate("Form", u"DEG", None))
        self.msButton.setText(QCoreApplication.translate("Form", u"MR", None))
        self.mrButton.setText(QCoreApplication.translate("Form", u"MS", None))
        self.label_2.setText("")
        self.label.setText(QCoreApplication.translate("Form", u"Calculator", None))
        self.logButton.setText(QCoreApplication.translate("Form", u"log", None))
        self.logButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.multiplicationButton.setText(QCoreApplication.translate("Form", u"\u00d7", None))
        self.multiplicationButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_2", None))
        self.piButton.setText(QCoreApplication.translate("Form", u"\u03c0", None))
        self.piButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.subtractionButton.setText(QCoreApplication.translate("Form", u"\ufe63", None))
        self.subtractionButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_2", None))
        self.pushButton_9.setText(QCoreApplication.translate("Form", u"9", None))
        self.powerButton.setText(QCoreApplication.translate("Form", u"x\u02b8", None))
        self.powerButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.pushButton_1.setText(QCoreApplication.translate("Form", u"1", None))
        self.squareButton.setText(QCoreApplication.translate("Form", u"x\u00b2", None))
        self.squareButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.cosButton.setText(QCoreApplication.translate("Form", u"cos", None))
        self.cosButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.pushButton_6.setText(QCoreApplication.translate("Form", u"6", None))
        self.delButton.setText(QCoreApplication.translate("Form", u"\u232b", None))
        self.delButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_1", None))
        self.pushButton_7.setText(QCoreApplication.translate("Form", u"7", None))
        self.rootButton.setText(QCoreApplication.translate("Form", u"\u02b8\u221ax", None))
        self.rootButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.equalsButton.setText(QCoreApplication.translate("Form", u"=", None))
        self.equalsButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_2", None))
        self.pushButton_4.setText(QCoreApplication.translate("Form", u"4", None))
        self.pushButton_3.setText(QCoreApplication.translate("Form", u"3", None))
        self.percentButton.setText("")
        self.percentButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_1", None))
        self.pushButton_dec.setText(QCoreApplication.translate("Form", u".", None))
        self.sqrtButton.setText(QCoreApplication.translate("Form", u"\u221ax", None))
        self.sqrtButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.pushButton_8.setText(QCoreApplication.translate("Form", u"8", None))
        self.cubeRootButton.setText(QCoreApplication.translate("Form", u"\u221bx", None))
        self.cubeRootButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.negateButton.setText("")
        self.negateButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_1", None))
        self.sinButton.setText(QCoreApplication.translate("Form", u"sin", None))
        self.sinButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.tenPowerButton.setText(QCoreApplication.translate("Form", u"\u00d710\u02e3", None))
        self.tenPowerButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.functionButton.setText(QCoreApplication.translate("Form", u"2nd", None))
        self.functionButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.lnButton.setText(QCoreApplication.translate("Form", u"ln", None))
        self.lnButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.divisionButton.setText(QCoreApplication.translate("Form", u"\u00f7", None))
        self.divisionButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_2", None))
        self.zeroButton.setText(QCoreApplication.translate("Form", u"0", None))
        self.reciprocalButton.setText(QCoreApplication.translate("Form", u"1/x", None))
        self.reciprocalButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.eButton.setText(QCoreApplication.translate("Form", u"e", None))
        self.eButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.openParenButton.setText(QCoreApplication.translate("Form", u"(", None))
        self.openParenButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.pushButton_5.setText(QCoreApplication.translate("Form", u"5", None))
        self.hypButton.setText(QCoreApplication.translate("Form", u"hyp", None))
        self.hypButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.closeParenButton.setText(QCoreApplication.translate("Form", u")", None))
        self.closeParenButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.logBaseButton.setText(QCoreApplication.translate("Form", u"log\u208dy\u208e(x)", None))
        self.logBaseButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.additionButton.setText(QCoreApplication.translate("Form", u"+", None))
        self.additionButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_2", None))
        self.pushButton_2.setText(QCoreApplication.translate("Form", u"2", None))
        self.clearButton.setText(QCoreApplication.translate("Form", u"AC", None))
        self.clearButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_1", None))
        self.tanButton.setText(QCoreApplication.translate("Form", u"tan", None))
        self.tanButton.setProperty(u"class", QCoreApplication.translate("Form", u"btn_group_3", None))
        self.lineEdit.setText(QCoreApplication.translate("Form", u"1,000", None))
    # retranslateUi

