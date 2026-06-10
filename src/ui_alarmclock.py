# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'ui_alarmclock.ui'
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
from PySide6.QtWidgets import (QApplication, QGridLayout, QHBoxLayout, QLCDNumber,
    QLabel, QLayout, QPushButton, QSizePolicy,
    QSpacerItem, QVBoxLayout, QWidget)

class Ui_AlarmClock(object):
    def setupUi(self, AlarmClock):
        if not AlarmClock.objectName():
            AlarmClock.setObjectName(u"AlarmClock")
        AlarmClock.resize(449, 186)
        self.gridLayoutWidget = QWidget(AlarmClock)
        self.gridLayoutWidget.setObjectName(u"gridLayoutWidget")
        self.gridLayoutWidget.setGeometry(QRect(9, 9, 383, 124))
        self.gridLayout = QGridLayout(self.gridLayoutWidget)
        self.gridLayout.setSpacing(0)
        self.gridLayout.setObjectName(u"gridLayout")
        self.gridLayout.setSizeConstraint(QLayout.SizeConstraint.SetMinAndMaxSize)
        self.gridLayout.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_2 = QVBoxLayout()
        self.verticalLayout_2.setObjectName(u"verticalLayout_2")
        self.apmlabel = QLabel(self.gridLayoutWidget)
        self.apmlabel.setObjectName(u"apmlabel")
        sizePolicy = QSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.apmlabel.sizePolicy().hasHeightForWidth())
        self.apmlabel.setSizePolicy(sizePolicy)

        self.verticalLayout_2.addWidget(self.apmlabel)

        self.verticalSpacer = QSpacerItem(20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Fixed)

        self.verticalLayout_2.addItem(self.verticalSpacer)


        self.gridLayout.addLayout(self.verticalLayout_2, 0, 3, 1, 1)

        self.lcdNumber = QLCDNumber(self.gridLayoutWidget)
        self.lcdNumber.setObjectName(u"lcdNumber")
        sizePolicy1 = QSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Expanding)
        sizePolicy1.setHorizontalStretch(0)
        sizePolicy1.setVerticalStretch(0)
        sizePolicy1.setHeightForWidth(self.lcdNumber.sizePolicy().hasHeightForWidth())
        self.lcdNumber.setSizePolicy(sizePolicy1)
        self.lcdNumber.setMinimumSize(QSize(200, 63))
        self.lcdNumber.setMaximumSize(QSize(200, 63))
        self.lcdNumber.setLayoutDirection(Qt.LayoutDirection.LeftToRight)
        self.lcdNumber.setDigitCount(8)
        self.lcdNumber.setSegmentStyle(QLCDNumber.SegmentStyle.Flat)
        self.lcdNumber.setProperty(u"value", 88888888.000000000000000)
        self.lcdNumber.setProperty(u"intValue", 88888888)

        self.gridLayout.addWidget(self.lcdNumber, 0, 2, 1, 1)

        self.horizontalLayout = QHBoxLayout()
        self.horizontalLayout.setObjectName(u"horizontalLayout")
        self.horizontalLayout.setSizeConstraint(QLayout.SizeConstraint.SetMinAndMaxSize)

        self.gridLayout.addLayout(self.horizontalLayout, 0, 0, 1, 1)

        self.snoozeb = QPushButton(self.gridLayoutWidget)
        self.snoozeb.setObjectName(u"snoozeb")
        self.snoozeb.setMinimumSize(QSize(79, 22))
        self.snoozeb.setMaximumSize(QSize(79, 22))

        self.gridLayout.addWidget(self.snoozeb, 1, 3, 1, 1)

        self.verticalLayout = QVBoxLayout()
        self.verticalLayout.setSpacing(0)
        self.verticalLayout.setObjectName(u"verticalLayout")
        self.verticalLayout.setSizeConstraint(QLayout.SizeConstraint.SetMinAndMaxSize)
        self.setlabel = QLabel(self.gridLayoutWidget)
        self.setlabel.setObjectName(u"setlabel")
        sizePolicy.setHeightForWidth(self.setlabel.sizePolicy().hasHeightForWidth())
        self.setlabel.setSizePolicy(sizePolicy)
        self.setlabel.setMinimumSize(QSize(10, 10))
        self.setlabel.setMaximumSize(QSize(10, 10))
        self.setlabel.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self.setlabel.setStyleSheet(u"background-color: red;\n"
"border-radius: 5px; \n"
"border: 1px solid darkred;")

        self.verticalLayout.addWidget(self.setlabel)

        self.verticalSpacer_3 = QSpacerItem(20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Fixed)

        self.verticalLayout.addItem(self.verticalSpacer_3)


        self.gridLayout.addLayout(self.verticalLayout, 0, 1, 1, 1)

        self.horizontalLayout_22 = QHBoxLayout()
        self.horizontalLayout_22.setSpacing(0)
        self.horizontalLayout_22.setObjectName(u"horizontalLayout_22")
        self.horizontalLayout_22.setSizeConstraint(QLayout.SizeConstraint.SetMinAndMaxSize)
        self.upb = QPushButton(self.gridLayoutWidget)
        self.upb.setObjectName(u"upb")
        self.upb.setMinimumSize(QSize(51, 22))
        self.upb.setMaximumSize(QSize(51, 22))

        self.horizontalLayout_22.addWidget(self.upb)

        self.downb = QPushButton(self.gridLayoutWidget)
        self.downb.setObjectName(u"downb")
        self.downb.setMinimumSize(QSize(51, 24))
        self.downb.setMaximumSize(QSize(51, 24))

        self.horizontalLayout_22.addWidget(self.downb)

        self.horizontalSpacer = QSpacerItem(15, 20, QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_22.addItem(self.horizontalSpacer)

        self.setb = QPushButton(self.gridLayoutWidget)
        self.setb.setObjectName(u"setb")
        self.setb.setMinimumSize(QSize(79, 22))
        self.setb.setMaximumSize(QSize(79, 22))

        self.horizontalLayout_22.addWidget(self.setb)


        self.gridLayout.addLayout(self.horizontalLayout_22, 1, 2, 1, 1)

        self.alarmb = QPushButton(self.gridLayoutWidget)
        self.alarmb.setObjectName(u"alarmb")
        sizePolicy.setHeightForWidth(self.alarmb.sizePolicy().hasHeightForWidth())
        self.alarmb.setSizePolicy(sizePolicy)
        self.alarmb.setMinimumSize(QSize(79, 22))
        self.alarmb.setMaximumSize(QSize(79, 22))
        self.alarmb.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        self.gridLayout.addWidget(self.alarmb, 1, 1, 1, 1)

        self.horizontalSpacer_2 = QSpacerItem(79, 20, QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Minimum)

        self.gridLayout.addItem(self.horizontalSpacer_2, 2, 3, 1, 1)


        self.retranslateUi(AlarmClock)

        QMetaObject.connectSlotsByName(AlarmClock)
    # setupUi

    def retranslateUi(self, AlarmClock):
        AlarmClock.setWindowTitle(QCoreApplication.translate("AlarmClock", u"Form", None))
        self.apmlabel.setText(QCoreApplication.translate("AlarmClock", u"PM", None))
        self.snoozeb.setText("")
        self.setlabel.setText("")
        self.upb.setText(QCoreApplication.translate("AlarmClock", u"Up", None))
        self.downb.setText(QCoreApplication.translate("AlarmClock", u"Down", None))
        self.setb.setText(QCoreApplication.translate("AlarmClock", u"Set", None))
        self.alarmb.setText(QCoreApplication.translate("AlarmClock", u"ALARM", None))
    # retranslateUi

