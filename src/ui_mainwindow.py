# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'untitledlatest5.ui'
##
## Created by: Qt User Interface Compiler version 6.9.3
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PySide6.QtCore import (QCoreApplication,
    QMetaObject, QRect, QSize,
    Qt)
from PySide6.QtGui import (QAction,
    QPixmap)
from PySide6.QtWidgets import (QAbstractSpinBox, QCheckBox, QComboBox,
    QGridLayout, QHBoxLayout, QLabel, QLayout, QLineEdit,
    QMenu, QMenuBar, QProgressBar, QPushButton, QSizePolicy,
    QSpacerItem, QSpinBox, QStackedWidget, QStatusBar,
    QTableView, QTextEdit, QToolButton, QVBoxLayout,
    QWidget)


class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(1704, 1025)
        sizePolicy = QSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(MainWindow.sizePolicy().hasHeightForWidth())
        MainWindow.setSizePolicy(sizePolicy)
        self.actionExit = QAction(MainWindow)
        self.actionExit.setObjectName(u"actionExit")
        self.actionResource = QAction(MainWindow)
        self.actionResource.setObjectName(u"actionResource")
        self.actionAbout = QAction(MainWindow)
        self.actionAbout.setObjectName(u"actionAbout")
        self.actionStop = QAction(MainWindow)
        self.actionStop.setObjectName(u"actionStop")
        self.actionManual = QAction(MainWindow)
        self.actionManual.setObjectName(u"actionManual")
        self.actionSave = QAction(MainWindow)
        self.actionSave.setObjectName(u"actionSave")
        self.actionUpdates = QAction(MainWindow)
        self.actionUpdates.setObjectName(u"actionUpdates")
        self.actionClearh = QAction(MainWindow)
        self.actionClearh.setObjectName(u"actionClearh")
        self.actionLaunch = QAction(MainWindow)
        self.actionLaunch.setObjectName(u"actionLaunch")
        self.actionClear_expensions = QAction(MainWindow)
        self.actionClear_expensions.setObjectName(u"actionClear_expensions")
        self.actionHelp = QAction(MainWindow)
        self.actionHelp.setObjectName(u"actionHelp")
        self.actionCreate_GPG_Key = QAction(MainWindow)
        self.actionCreate_GPG_Key.setObjectName(u"actionCreate_GPG_Key")
        self.actionChange_Base_Drive = QAction(MainWindow)
        self.actionChange_Base_Drive.setObjectName(u"actionChange_Base_Drive")
        self.actionTotal_Directories = QAction(MainWindow)
        self.actionTotal_Directories.setObjectName(u"actionTotal_Directories")
        self.actionCommands_2 = QAction(MainWindow)
        self.actionCommands_2.setObjectName(u"actionCommands_2")
        self.actionQuick1 = QAction(MainWindow)
        self.actionQuick1.setObjectName(u"actionQuick1")
        self.actionDiag1 = QAction(MainWindow)
        self.actionDiag1.setObjectName(u"actionDiag1")
        self.actionChange_Default_Location = QAction(MainWindow)
        self.actionChange_Default_Location.setObjectName(u"actionChange_Default_Location")
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        self.gridLayout_6 = QGridLayout(self.centralwidget)
        self.gridLayout_6.setObjectName(u"gridLayout_6")
        self.stackedWidget = QStackedWidget(self.centralwidget)
        self.stackedWidget.setObjectName(u"stackedWidget")
        self.page = QWidget()
        self.page.setObjectName(u"page")
        self.gridLayout_3 = QGridLayout(self.page)
        self.gridLayout_3.setObjectName(u"gridLayout_3")
        self.gridLayout = QGridLayout()
        self.gridLayout.setObjectName(u"gridLayout")
        self.horizontalLayout_12 = QHBoxLayout()
        self.horizontalLayout_12.setSpacing(0)
        self.horizontalLayout_12.setObjectName(u"horizontalLayout_12")
        self.horizontalLayout_12.setSizeConstraint(QLayout.SizeConstraint.SetFixedSize)
        self.jpgcr = QLabel(self.page)
        self.jpgcr.setObjectName(u"jpgcr")
        sizePolicy1 = QSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        sizePolicy1.setHorizontalStretch(0)
        sizePolicy1.setVerticalStretch(0)
        sizePolicy1.setHeightForWidth(self.jpgcr.sizePolicy().hasHeightForWidth())
        self.jpgcr.setSizePolicy(sizePolicy1)
        self.jpgcr.setMinimumSize(QSize(250, 250))
        self.jpgcr.setMaximumSize(QSize(250, 250))
        self.jpgcr.setLayoutDirection(Qt.LayoutDirection.LeftToRight)

        self.horizontalLayout_12.addWidget(self.jpgcr)


        self.gridLayout.addLayout(self.horizontalLayout_12, 5, 8, 9, 1)

        self.horizontalLayout_11 = QHBoxLayout()
        self.horizontalLayout_11.setSpacing(0)
        self.horizontalLayout_11.setObjectName(u"horizontalLayout_11")
        self.horizontalLayout_11.setSizeConstraint(QLayout.SizeConstraint.SetMinAndMaxSize)
        self.stime = QSpinBox(self.page)
        self.stime.setObjectName(u"stime")
        sizePolicy1.setHeightForWidth(self.stime.sizePolicy().hasHeightForWidth())
        self.stime.setSizePolicy(sizePolicy1)
        self.stime.setMinimumSize(QSize(89, 22))
        self.stime.setMaximumSize(QSize(89, 22))
        self.stime.setMinimum(0)
        self.stime.setMaximum(2147483647)
        self.stime.setValue(0)

        self.horizontalLayout_11.addWidget(self.stime)

        self.ftimesecondsl = QLabel(self.page)
        self.ftimesecondsl.setObjectName(u"ftimesecondsl")
        sizePolicy1.setHeightForWidth(self.ftimesecondsl.sizePolicy().hasHeightForWidth())
        self.ftimesecondsl.setSizePolicy(sizePolicy1)
        self.ftimesecondsl.setMinimumSize(QSize(89, 22))
        self.ftimesecondsl.setMaximumSize(QSize(89, 22))

        self.horizontalLayout_11.addWidget(self.ftimesecondsl)


        self.gridLayout.addLayout(self.horizontalLayout_11, 8, 0, 1, 1)

        self.combftimeout = QComboBox(self.page)
        self.combftimeout.addItem("")
        self.combftimeout.addItem(u"Desktop")
        self.combftimeout.setObjectName(u"combftimeout")
        sizePolicy1.setHeightForWidth(self.combftimeout.sizePolicy().hasHeightForWidth())
        self.combftimeout.setSizePolicy(sizePolicy1)
        self.combftimeout.setMinimumSize(QSize(82, 22))
        self.combftimeout.setMaximumSize(QSize(82, 22))

        self.gridLayout.addWidget(self.combftimeout, 9, 1, 1, 1)

        self.verticalSpacer_2 = QSpacerItem(20, 75, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Fixed)

        self.gridLayout.addItem(self.verticalSpacer_2, 29, 2, 1, 1)

        self.ilabel = QLabel(self.page)
        self.ilabel.setObjectName(u"ilabel")

        self.gridLayout.addWidget(self.ilabel, 5, 6, 1, 1)

        self.horizontalLayout = QHBoxLayout()
        self.horizontalLayout.setSpacing(0)
        self.horizontalLayout.setObjectName(u"horizontalLayout")
        self.horizontalLayout.setSizeConstraint(QLayout.SizeConstraint.SetMinAndMaxSize)
        self.horizontalLayout.setContentsMargins(3, -1, -1, -1)
        self.toollftb = QToolButton(self.page)
        self.toollftb.setObjectName(u"toollftb")
        self.toollftb.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self.toollftb.setArrowType(Qt.ArrowType.LeftArrow)

        self.horizontalLayout.addWidget(self.toollftb)

        self.toolhomeb = QToolButton(self.page)
        self.toolhomeb.setObjectName(u"toolhomeb")
        sizePolicy2 = QSizePolicy(QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Fixed)
        sizePolicy2.setHorizontalStretch(0)
        sizePolicy2.setVerticalStretch(0)
        sizePolicy2.setHeightForWidth(self.toolhomeb.sizePolicy().hasHeightForWidth())
        self.toolhomeb.setSizePolicy(sizePolicy2)
        self.toolhomeb.setMinimumSize(QSize(483, 23))
        self.toolhomeb.setMaximumSize(QSize(483, 23))
        self.toolhomeb.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self.toolhomeb.setArrowType(Qt.ArrowType.UpArrow)

        self.horizontalLayout.addWidget(self.toolhomeb)

        self.toolrtb = QToolButton(self.page)
        self.toolrtb.setObjectName(u"toolrtb")
        self.toolrtb.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self.toolrtb.setArrowType(Qt.ArrowType.RightArrow)

        self.horizontalLayout.addWidget(self.toolrtb)


        self.gridLayout.addLayout(self.horizontalLayout, 1, 8, 1, 1)

        self.verticalLayout = QVBoxLayout()
        self.verticalLayout.setSpacing(0)
        self.verticalLayout.setObjectName(u"verticalLayout")
        self.verticalLayout.setSizeConstraint(QLayout.SizeConstraint.SetFixedSize)
        self.verticalLayout.setContentsMargins(0, -1, -1, -1)
        self.textEdit = QTextEdit(self.page)
        self.textEdit.setObjectName(u"textEdit")
        sizePolicy1.setHeightForWidth(self.textEdit.sizePolicy().hasHeightForWidth())
        self.textEdit.setSizePolicy(sizePolicy1)
        self.textEdit.setMinimumSize(QSize(533, 229))
        self.textEdit.setMaximumSize(QSize(533, 259))

        self.verticalLayout.addWidget(self.textEdit)

        self.progressBAR = QProgressBar(self.page)
        self.progressBAR.setObjectName(u"progressBAR")
        sizePolicy1.setHeightForWidth(self.progressBAR.sizePolicy().hasHeightForWidth())
        self.progressBAR.setSizePolicy(sizePolicy1)
        self.progressBAR.setMinimumSize(QSize(533, 22))
        self.progressBAR.setMaximumSize(QSize(533, 22))
        self.progressBAR.setValue(0)

        self.verticalLayout.addWidget(self.progressBAR)


        self.gridLayout.addLayout(self.verticalLayout, 26, 8, 11, 1)

        self.horizontalLayout_10 = QHBoxLayout()
        self.horizontalLayout_10.setSpacing(0)
        self.horizontalLayout_10.setObjectName(u"horizontalLayout_10")
        self.horizontalLayout_10.setSizeConstraint(QLayout.SizeConstraint.SetMinAndMaxSize)
        self.horizontalLayout_10.setContentsMargins(-1, -1, 0, -1)
        self.stimeb = QPushButton(self.page)
        self.stimeb.setObjectName(u"stimeb")
        sizePolicy1.setHeightForWidth(self.stimeb.sizePolicy().hasHeightForWidth())
        self.stimeb.setSizePolicy(sizePolicy1)
        self.stimeb.setMinimumSize(QSize(89, 0))
        self.stimeb.setMaximumSize(QSize(89, 22))

        self.horizontalLayout_10.addWidget(self.stimeb)

        self.ftimeb = QPushButton(self.page)
        self.ftimeb.setObjectName(u"ftimeb")
        sizePolicy1.setHeightForWidth(self.ftimeb.sizePolicy().hasHeightForWidth())
        self.ftimeb.setSizePolicy(sizePolicy1)
        self.ftimeb.setMinimumSize(QSize(89, 0))
        self.ftimeb.setMaximumSize(QSize(89, 22))

        self.horizontalLayout_10.addWidget(self.ftimeb)


        self.gridLayout.addLayout(self.horizontalLayout_10, 9, 0, 1, 1)

        self.rmvButton = QPushButton(self.page)
        self.rmvButton.setObjectName(u"rmvButton")
        self.rmvButton.setMinimumSize(QSize(89, 22))
        self.rmvButton.setMaximumSize(QSize(89, 22))

        self.gridLayout.addWidget(self.rmvButton, 7, 7, 1, 1)

        self.horizontalLayout_8 = QHBoxLayout()
        self.horizontalLayout_8.setSpacing(0)
        self.horizontalLayout_8.setObjectName(u"horizontalLayout_8")
        self.horizontalLayout_8.setContentsMargins(-1, -1, 0, -1)
        self.ntbrowseb = QPushButton(self.page)
        self.ntbrowseb.setObjectName(u"ntbrowseb")
        sizePolicy1.setHeightForWidth(self.ntbrowseb.sizePolicy().hasHeightForWidth())
        self.ntbrowseb.setSizePolicy(sizePolicy1)
        self.ntbrowseb.setMinimumSize(QSize(89, 0))

        self.horizontalLayout_8.addWidget(self.ntbrowseb)

        self.ntlineEDIT = QLineEdit(self.page)
        self.ntlineEDIT.setObjectName(u"ntlineEDIT")
        sizePolicy3 = QSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        sizePolicy3.setHorizontalStretch(0)
        sizePolicy3.setVerticalStretch(0)
        sizePolicy3.setHeightForWidth(self.ntlineEDIT.sizePolicy().hasHeightForWidth())
        self.ntlineEDIT.setSizePolicy(sizePolicy3)

        self.horizontalLayout_8.addWidget(self.ntlineEDIT)


        self.gridLayout.addLayout(self.horizontalLayout_8, 12, 0, 1, 3)

        self.ftimebf = QPushButton(self.page)
        self.ftimebf.setObjectName(u"ftimebf")
        sizePolicy1.setHeightForWidth(self.ftimebf.sizePolicy().hasHeightForWidth())
        self.ftimebf.setSizePolicy(sizePolicy1)
        self.ftimebf.setMinimumSize(QSize(89, 0))

        self.gridLayout.addWidget(self.ftimebf, 6, 0, 1, 1)

        self.stimebf = QPushButton(self.page)
        self.stimebf.setObjectName(u"stimebf")
        sizePolicy1.setHeightForWidth(self.stimebf.sizePolicy().hasHeightForWidth())
        self.stimebf.setSizePolicy(sizePolicy1)
        self.stimebf.setMinimumSize(QSize(89, 0))
        self.stimebf.setMaximumSize(QSize(89, 16777215))

        self.gridLayout.addWidget(self.stimebf, 7, 0, 1, 1)

        self.mftmainlabel = QLabel(self.page)
        self.mftmainlabel.setObjectName(u"mftmainlabel")
        sizePolicy.setHeightForWidth(self.mftmainlabel.sizePolicy().hasHeightForWidth())
        self.mftmainlabel.setSizePolicy(sizePolicy)
        self.mftmainlabel.setMinimumSize(QSize(89, 0))
        self.mftmainlabel.setMaximumSize(QSize(89, 16777215))

        self.gridLayout.addWidget(self.mftmainlabel, 36, 7, 1, 1)

        self.downloadButton = QPushButton(self.page)
        self.downloadButton.setObjectName(u"downloadButton")
        self.downloadButton.setEnabled(True)

        self.gridLayout.addWidget(self.downloadButton, 6, 5, 1, 1)

        self.verticalLayout_4 = QVBoxLayout()
        self.verticalLayout_4.setSpacing(0)
        self.verticalLayout_4.setObjectName(u"verticalLayout_4")
        self.verticalLayout_4.setSizeConstraint(QLayout.SizeConstraint.SetFixedSize)
        self.jpgv = QLabel(self.page)
        self.jpgv.setObjectName(u"jpgv")
        sizePolicy1.setHeightForWidth(self.jpgv.sizePolicy().hasHeightForWidth())
        self.jpgv.setSizePolicy(sizePolicy1)
        self.jpgv.setMinimumSize(QSize(533, 300))
        self.jpgv.setMaximumSize(QSize(533, 300))
        self.jpgv.setPixmap(QPixmap(u"../.designer/.designer/.designer/.designer/.designer/.designer/.designer/.designer/mnt/sdb1/ventoy/Themes/background.png"))
        self.jpgv.setScaledContents(True)

        self.verticalLayout_4.addWidget(self.jpgv)

        self.horizontalLayout_6 = QHBoxLayout()
        self.horizontalLayout_6.setSpacing(0)
        self.horizontalLayout_6.setObjectName(u"horizontalLayout_6")
        self.horizontalLayout_6.setSizeConstraint(QLayout.SizeConstraint.SetMinAndMaxSize)
        self.horizontalLayout_6.setContentsMargins(-1, -1, 0, -1)
        self.jpgb = QPushButton(self.page)
        self.jpgb.setObjectName(u"jpgb")
        self.jpgb.setEnabled(True)
        sizePolicy1.setHeightForWidth(self.jpgb.sizePolicy().hasHeightForWidth())
        self.jpgb.setSizePolicy(sizePolicy1)
        self.jpgb.setMinimumSize(QSize(89, 0))
        self.jpgb.setMaximumSize(QSize(89, 16777215))

        self.horizontalLayout_6.addWidget(self.jpgb)

        self.tomlb = QToolButton(self.page)
        self.tomlb.setObjectName(u"tomlb")
        self.tomlb.setMinimumSize(QSize(89, 21))
        self.tomlb.setMaximumSize(QSize(89, 21))

        self.horizontalLayout_6.addWidget(self.tomlb)

        self.horizontalSpacer_8 = QSpacerItem(353, 20, QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_6.addItem(self.horizontalSpacer_8)


        self.verticalLayout_4.addLayout(self.horizontalLayout_6)

        self.verticalSpacer = QSpacerItem(20, 0, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding)

        self.verticalLayout_4.addItem(self.verticalSpacer)


        self.gridLayout.addLayout(self.verticalLayout_4, 3, 8, 1, 1)

        self.diffchkc = QCheckBox(self.page)
        self.diffchkc.setObjectName(u"diffchkc")
        self.diffchkc.setEnabled(True)

        self.gridLayout.addWidget(self.diffchkc, 36, 1, 1, 1)

        self.ntsb = QPushButton(self.page)
        self.ntsb.setObjectName(u"ntsb")
        sizePolicy1.setHeightForWidth(self.ntsb.sizePolicy().hasHeightForWidth())
        self.ntsb.setSizePolicy(sizePolicy1)
        self.ntsb.setMinimumSize(QSize(89, 0))
        self.ntsb.setMaximumSize(QSize(89, 16777215))

        self.gridLayout.addWidget(self.ntsb, 12, 5, 1, 1)

        self.horizontalLayout_3 = QHBoxLayout()
        self.horizontalLayout_3.setSpacing(0)
        self.horizontalLayout_3.setObjectName(u"horizontalLayout_3")
        self.horizontalLayout_3.setContentsMargins(0, 0, 0, -1)
        self.horizontalLayout_7 = QHBoxLayout()
        self.horizontalLayout_7.setSpacing(0)
        self.horizontalLayout_7.setObjectName(u"horizontalLayout_7")
        self.horizontalLayout_7.setSizeConstraint(QLayout.SizeConstraint.SetMinAndMaxSize)
        self.ffilet = QLineEdit(self.page)
        self.ffilet.setObjectName(u"ffilet")
        sizePolicy3.setHeightForWidth(self.ffilet.sizePolicy().hasHeightForWidth())
        self.ffilet.setSizePolicy(sizePolicy3)
        self.ffilet.setMinimumSize(QSize(300, 24))

        self.horizontalLayout_7.addWidget(self.ffilet)

        self.combffile = QComboBox(self.page)
        self.combffile.addItem("")
        self.combffile.setObjectName(u"combffile")
        sizePolicy4 = QSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        sizePolicy4.setHorizontalStretch(0)
        sizePolicy4.setVerticalStretch(0)
        sizePolicy4.setHeightForWidth(self.combffile.sizePolicy().hasHeightForWidth())
        self.combffile.setSizePolicy(sizePolicy4)
        self.combffile.setMinimumSize(QSize(82, 22))
        self.combffile.setMaximumSize(QSize(275, 22))
        self.combffile.setStyleSheet(u"")
        self.combffile.setEditable(True)
        self.combffile.setInsertPolicy(QComboBox.InsertPolicy.InsertAtTop)
        self.combffile.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)

        self.horizontalLayout_7.addWidget(self.combffile)

        self.horizontalLayout_4 = QHBoxLayout()
        self.horizontalLayout_4.setSpacing(0)
        self.horizontalLayout_4.setObjectName(u"horizontalLayout_4")
        self.horizontalLayout_4.setSizeConstraint(QLayout.SizeConstraint.SetDefaultConstraint)
        self.ffileb2 = QToolButton(self.page)
        self.ffileb2.setObjectName(u"ffileb2")
        self.ffileb2.setMinimumSize(QSize(21, 22))
        self.ffileb2.setMaximumSize(QSize(21, 22))

        self.horizontalLayout_4.addWidget(self.ffileb2)

        self.ffilelabel2 = QLabel(self.page)
        self.ffilelabel2.setObjectName(u"ffilelabel2")
        sizePolicy5 = QSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Preferred)
        sizePolicy5.setHorizontalStretch(0)
        sizePolicy5.setVerticalStretch(0)
        sizePolicy5.setHeightForWidth(self.ffilelabel2.sizePolicy().hasHeightForWidth())
        self.ffilelabel2.setSizePolicy(sizePolicy5)
        self.ffilelabel2.setMinimumSize(QSize(61, 22))
        self.ffilelabel2.setMaximumSize(QSize(61, 22))

        self.horizontalLayout_4.addWidget(self.ffilelabel2)


        self.horizontalLayout_7.addLayout(self.horizontalLayout_4)


        self.horizontalLayout_3.addLayout(self.horizontalLayout_7)

        self.horizontalSpacer_7 = QSpacerItem(73, 20, QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_3.addItem(self.horizontalSpacer_7)


        self.gridLayout.addLayout(self.horizontalLayout_3, 31, 0, 1, 3)

        self.ntbrowseb2 = QPushButton(self.page)
        self.ntbrowseb2.setObjectName(u"ntbrowseb2")
        sizePolicy1.setHeightForWidth(self.ntbrowseb2.sizePolicy().hasHeightForWidth())
        self.ntbrowseb2.setSizePolicy(sizePolicy1)
        self.ntbrowseb2.setMinimumSize(QSize(89, 0))

        self.gridLayout.addWidget(self.ntbrowseb2, 13, 0, 1, 1)

        self.addButton = QPushButton(self.page)
        self.addButton.setObjectName(u"addButton")
        self.addButton.setMinimumSize(QSize(89, 22))
        self.addButton.setMaximumSize(QSize(89, 22))

        self.gridLayout.addWidget(self.addButton, 6, 7, 1, 1)

        self.combd = QComboBox(self.page)
        self.combd.addItem("")
        self.combd.setObjectName(u"combd")
        sizePolicy1.setHeightForWidth(self.combd.sizePolicy().hasHeightForWidth())
        self.combd.setSizePolicy(sizePolicy1)
        self.combd.setMinimumSize(QSize(89, 22))
        self.combd.setMaximumSize(QSize(89, 22))

        self.gridLayout.addWidget(self.combd, 6, 6, 1, 1)

        self.ffilelabel1 = QLabel(self.page)
        self.ffilelabel1.setObjectName(u"ffilelabel1")
        self.ffilelabel1.setMinimumSize(QSize(89, 22))
        self.ffilelabel1.setMaximumSize(QSize(89, 22))

        self.gridLayout.addWidget(self.ffilelabel1, 30, 0, 1, 1)

        self.ntlabel = QLabel(self.page)
        self.ntlabel.setObjectName(u"ntlabel")
        sizePolicy1.setHeightForWidth(self.ntlabel.sizePolicy().hasHeightForWidth())
        self.ntlabel.setSizePolicy(sizePolicy1)

        self.gridLayout.addWidget(self.ntlabel, 11, 0, 1, 1)

        self.ftimelabel1 = QLabel(self.page)
        self.ftimelabel1.setObjectName(u"ftimelabel1")
        sizePolicy1.setHeightForWidth(self.ftimelabel1.sizePolicy().hasHeightForWidth())
        self.ftimelabel1.setSizePolicy(sizePolicy1)

        self.gridLayout.addWidget(self.ftimelabel1, 8, 1, 1, 1)

        self.dlabel = QLabel(self.page)
        self.dlabel.setObjectName(u"dlabel")

        self.gridLayout.addWidget(self.dlabel, 5, 5, 1, 1)

        self.queryButton = QPushButton(self.page)
        self.queryButton.setObjectName(u"queryButton")
        self.queryButton.setMinimumSize(QSize(89, 0))
        self.queryButton.setMaximumSize(QSize(89, 22))

        self.gridLayout.addWidget(self.queryButton, 4, 0, 1, 1)

        self.horizontalSpacer_4 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.gridLayout.addItem(self.horizontalSpacer_4, 11, 2, 1, 1)

        self.diffchkb = QCheckBox(self.page)
        self.diffchkb.setObjectName(u"diffchkb")
        sizePolicy1.setHeightForWidth(self.diffchkb.sizePolicy().hasHeightForWidth())
        self.diffchkb.setSizePolicy(sizePolicy1)
        self.diffchkb.setMinimumSize(QSize(157, 20))
        self.diffchkb.setMaximumSize(QSize(157, 20))

        self.gridLayout.addWidget(self.diffchkb, 36, 0, 1, 1)

        self.mftsearchb = QPushButton(self.page)
        self.mftsearchb.setObjectName(u"mftsearchb")
        self.mftsearchb.setMinimumSize(QSize(89, 22))
        self.mftsearchb.setMaximumSize(QSize(89, 22))

        self.gridLayout.addWidget(self.mftsearchb, 36, 6, 1, 1)

        self.hudt = QTextEdit(self.page)
        self.hudt.setObjectName(u"hudt")
        self.hudt.setStyleSheet(u"QTextEdit {\n"
"    background-color: black;\n"
"    color: #00FF00;\n"
"    font-family: Consolas, Courier, monospace;\n"
"    font-size: 12pt;\n"
"}")

        self.gridLayout.addWidget(self.hudt, 3, 0, 1, 8)

        self.combt = QComboBox(self.page)
        self.combt.addItem("")
        self.combt.addItem("")
        self.combt.setObjectName(u"combt")
        sizePolicy1.setHeightForWidth(self.combt.sizePolicy().hasHeightForWidth())
        self.combt.setSizePolicy(sizePolicy1)
        self.combt.setMinimumSize(QSize(89, 22))
        self.combt.setMaximumSize(QSize(89, 22))

        self.gridLayout.addWidget(self.combt, 11, 5, 1, 1)

        self.gridLayout_5 = QGridLayout()
        self.gridLayout_5.setObjectName(u"gridLayout_5")
        self.horizontalLayout_9 = QHBoxLayout()
        self.horizontalLayout_9.setSpacing(0)
        self.horizontalLayout_9.setObjectName(u"horizontalLayout_9")
        self.horizontalLayout_9.setSizeConstraint(QLayout.SizeConstraint.SetDefaultConstraint)
        self.ffilecb = QPushButton(self.page)
        self.ffilecb.setObjectName(u"ffilecb")
        sizePolicy1.setHeightForWidth(self.ffilecb.sizePolicy().hasHeightForWidth())
        self.ffilecb.setSizePolicy(sizePolicy1)
        self.ffilecb.setMinimumSize(QSize(89, 22))
        self.ffilecb.setMaximumSize(QSize(89, 22))

        self.horizontalLayout_9.addWidget(self.ffilecb)

        self.combffileout = QComboBox(self.page)
        self.combffileout.addItem("")
        self.combffileout.addItem(u"Desktop")
        self.combffileout.setObjectName(u"combffileout")
        sizePolicy.setHeightForWidth(self.combffileout.sizePolicy().hasHeightForWidth())
        self.combffileout.setSizePolicy(sizePolicy)
        self.combffileout.setMinimumSize(QSize(82, 22))
        self.combffileout.setMaximumSize(QSize(89, 22))
        self.combffileout.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)

        self.horizontalLayout_9.addWidget(self.combffileout)


        self.gridLayout_5.addLayout(self.horizontalLayout_9, 1, 0, 1, 1)

        self.ffileb = QPushButton(self.page)
        self.ffileb.setObjectName(u"ffileb")
        sizePolicy1.setHeightForWidth(self.ffileb.sizePolicy().hasHeightForWidth())
        self.ffileb.setSizePolicy(sizePolicy1)
        self.ffileb.setMinimumSize(QSize(89, 22))
        self.ffileb.setMaximumSize(QSize(89, 22))

        self.gridLayout_5.addWidget(self.ffileb, 0, 0, 1, 1)


        self.gridLayout.addLayout(self.gridLayout_5, 32, 0, 1, 1)

        self.verticalSpacer_3 = QSpacerItem(20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Fixed)

        self.gridLayout.addItem(self.verticalSpacer_3, 10, 5, 1, 1)

        self.horizontalLayout_16 = QHBoxLayout()
        self.horizontalLayout_16.setSpacing(0)
        self.horizontalLayout_16.setObjectName(u"horizontalLayout_16")
        self.horizontalLayout_16.setContentsMargins(10, -1, -1, -1)
        self.mftlabel4 = QLabel(self.page)
        self.mftlabel4.setObjectName(u"mftlabel4")
        sizePolicy1.setHeightForWidth(self.mftlabel4.sizePolicy().hasHeightForWidth())
        self.mftlabel4.setSizePolicy(sizePolicy1)
        self.mftlabel4.setMinimumSize(QSize(55, 20))
        self.mftlabel4.setMaximumSize(QSize(55, 20))

        self.horizontalLayout_16.addWidget(self.mftlabel4)

        self.mftchkb = QCheckBox(self.page)
        self.mftchkb.setObjectName(u"mftchkb")
        sizePolicy1.setHeightForWidth(self.mftchkb.sizePolicy().hasHeightForWidth())
        self.mftchkb.setSizePolicy(sizePolicy1)
        self.mftchkb.setMinimumSize(QSize(21, 16))
        self.mftchkb.setMaximumSize(QSize(21, 16))
        self.mftchkb.setLayoutDirection(Qt.LayoutDirection.LeftToRight)

        self.horizontalLayout_16.addWidget(self.mftchkb)


        self.gridLayout.addLayout(self.horizontalLayout_16, 36, 5, 1, 1)

        self.resetButton = QPushButton(self.page)
        self.resetButton.setObjectName(u"resetButton")
        self.resetButton.setMaximumSize(QSize(89, 22))

        self.gridLayout.addWidget(self.resetButton, 4, 7, 1, 1)

        self.horizontalLayout_14 = QHBoxLayout()
        self.horizontalLayout_14.setSpacing(0)
        self.horizontalLayout_14.setObjectName(u"horizontalLayout_14")
        self.horizontalLayout_14.setContentsMargins(10, -1, -1, -1)
        self.mftlabel3 = QLabel(self.page)
        self.mftlabel3.setObjectName(u"mftlabel3")
        sizePolicy1.setHeightForWidth(self.mftlabel3.sizePolicy().hasHeightForWidth())
        self.mftlabel3.setSizePolicy(sizePolicy1)
        self.mftlabel3.setMinimumSize(QSize(55, 20))
        self.mftlabel3.setMaximumSize(QSize(55, 20))

        self.horizontalLayout_14.addWidget(self.mftlabel3)

        self.mftchka = QCheckBox(self.page)
        self.mftchka.setObjectName(u"mftchka")
        sizePolicy1.setHeightForWidth(self.mftchka.sizePolicy().hasHeightForWidth())
        self.mftchka.setSizePolicy(sizePolicy1)
        self.mftchka.setMinimumSize(QSize(21, 16))
        self.mftchka.setMaximumSize(QSize(21, 16))
        self.mftchka.setLayoutDirection(Qt.LayoutDirection.LeftToRight)

        self.horizontalLayout_14.addWidget(self.mftchka)


        self.gridLayout.addLayout(self.horizontalLayout_14, 35, 5, 1, 1)

        self.mftrange = QSpinBox(self.page)
        self.mftrange.setObjectName(u"mftrange")
        sizePolicy.setHeightForWidth(self.mftrange.sizePolicy().hasHeightForWidth())
        self.mftrange.setSizePolicy(sizePolicy)
        self.mftrange.setMinimumSize(QSize(89, 0))
        self.mftrange.setMaximumSize(QSize(89, 22))
        self.mftrange.setMinimum(1)
        self.mftrange.setSingleStep(1)
        self.mftrange.setStepType(QAbstractSpinBox.StepType.DefaultStepType)
        self.mftrange.setValue(24)
        self.mftrange.setDisplayIntegerBase(10)

        self.gridLayout.addWidget(self.mftrange, 35, 6, 1, 1)

        self.mftlabel2 = QLabel(self.page)
        self.mftlabel2.setObjectName(u"mftlabel2")
        sizePolicy1.setHeightForWidth(self.mftlabel2.sizePolicy().hasHeightForWidth())
        self.mftlabel2.setSizePolicy(sizePolicy1)

        self.gridLayout.addWidget(self.mftlabel2, 35, 7, 1, 1)

        self.mftbrowseb = QPushButton(self.page)
        self.mftbrowseb.setObjectName(u"mftbrowseb")
        sizePolicy1.setHeightForWidth(self.mftbrowseb.sizePolicy().hasHeightForWidth())
        self.mftbrowseb.setSizePolicy(sizePolicy1)
        self.mftbrowseb.setMinimumSize(QSize(89, 0))
        self.mftbrowseb.setMaximumSize(QSize(89, 16777215))

        self.gridLayout.addWidget(self.mftbrowseb, 34, 7, 1, 1)

        self.mftlabel1 = QLabel(self.page)
        self.mftlabel1.setObjectName(u"mftlabel1")
        sizePolicy1.setHeightForWidth(self.mftlabel1.sizePolicy().hasHeightForWidth())
        self.mftlabel1.setSizePolicy(sizePolicy1)
        self.mftlabel1.setMinimumSize(QSize(89, 0))
        self.mftlabel1.setMaximumSize(QSize(89, 16777215))

        self.gridLayout.addWidget(self.mftlabel1, 34, 6, 1, 1)

        self.diffchka = QCheckBox(self.page)
        self.diffchka.setObjectName(u"diffchka")
        self.diffchka.setMinimumSize(QSize(141, 20))
        self.diffchka.setMaximumSize(QSize(141, 20))

        self.gridLayout.addWidget(self.diffchka, 35, 0, 1, 1)

        self.difflabel = QLabel(self.page)
        self.difflabel.setObjectName(u"difflabel")
        self.difflabel.setMinimumSize(QSize(51, 16))
        self.difflabel.setMaximumSize(QSize(51, 16))

        self.gridLayout.addWidget(self.difflabel, 34, 0, 1, 1)


        self.gridLayout_3.addLayout(self.gridLayout, 0, 0, 1, 1)

        self.stackedWidget.addWidget(self.page)
        self.page_2 = QWidget()
        self.page_2.setObjectName(u"page_2")
        self.gridLayout_4 = QGridLayout(self.page_2)
        self.gridLayout_4.setObjectName(u"gridLayout_4")
        self.gridLayout_2 = QGridLayout()
        self.gridLayout_2.setObjectName(u"gridLayout_2")
        self.horizontalLayout_5 = QHBoxLayout()
        self.horizontalLayout_5.setSpacing(0)
        self.horizontalLayout_5.setObjectName(u"horizontalLayout_5")
        self.horizontalLayout_5.setSizeConstraint(QLayout.SizeConstraint.SetMinAndMaxSize)
        self.horizontalSpacer_2 = QSpacerItem(40, 20, QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_5.addItem(self.horizontalSpacer_2)

        self.dbidxb1 = QPushButton(self.page_2)
        self.dbidxb1.setObjectName(u"dbidxb1")
        sizePolicy1.setHeightForWidth(self.dbidxb1.sizePolicy().hasHeightForWidth())
        self.dbidxb1.setSizePolicy(sizePolicy1)
        self.dbidxb1.setMinimumSize(QSize(80, 22))
        self.dbidxb1.setMaximumSize(QSize(80, 22))

        self.horizontalLayout_5.addWidget(self.dbidxb1)

        self.dbidxb2 = QPushButton(self.page_2)
        self.dbidxb2.setObjectName(u"dbidxb2")
        sizePolicy1.setHeightForWidth(self.dbidxb2.sizePolicy().hasHeightForWidth())
        self.dbidxb2.setSizePolicy(sizePolicy1)
        self.dbidxb2.setMinimumSize(QSize(80, 22))
        self.dbidxb2.setMaximumSize(QSize(80, 22))

        self.horizontalLayout_5.addWidget(self.dbidxb2)

        self.dbprogressBAR = QProgressBar(self.page_2)
        self.dbprogressBAR.setObjectName(u"dbprogressBAR")
        sizePolicy6 = QSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        sizePolicy6.setHorizontalStretch(0)
        sizePolicy6.setVerticalStretch(0)
        sizePolicy6.setHeightForWidth(self.dbprogressBAR.sizePolicy().hasHeightForWidth())
        self.dbprogressBAR.setSizePolicy(sizePolicy6)
        self.dbprogressBAR.setMinimumSize(QSize(451, 22))
        self.dbprogressBAR.setValue(24)

        self.horizontalLayout_5.addWidget(self.dbprogressBAR)


        self.gridLayout_2.addLayout(self.horizontalLayout_5, 2, 9, 1, 1)

        self.horizontalSpacer_5 = QSpacerItem(150, 20, QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Minimum)

        self.gridLayout_2.addItem(self.horizontalSpacer_5, 2, 1, 1, 1)

        self.horizontalLayout_13 = QHBoxLayout()
        self.horizontalLayout_13.setSpacing(0)
        self.horizontalLayout_13.setObjectName(u"horizontalLayout_13")
        self.horizontalLayout_13.setSizeConstraint(QLayout.SizeConstraint.SetMinAndMaxSize)
        self.horizontalLayout_13.setContentsMargins(-1, -1, 0, -1)
        self.combdb = QComboBox(self.page_2)
        self.combdb.setObjectName(u"combdb")
        sizePolicy5.setHeightForWidth(self.combdb.sizePolicy().hasHeightForWidth())
        self.combdb.setSizePolicy(sizePolicy5)
        self.combdb.setMinimumSize(QSize(0, 22))
        self.combdb.setMaximumSize(QSize(150, 22))
        self.combdb.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)

        self.horizontalLayout_13.addWidget(self.combdb)

        self.dbmainb3 = QToolButton(self.page_2)
        self.dbmainb3.setObjectName(u"dbmainb3")
        self.dbmainb3.setMinimumSize(QSize(24, 22))
        self.dbmainb3.setMaximumSize(QSize(24, 22))
        self.dbmainb3.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self.dbmainb3.setArrowType(Qt.ArrowType.LeftArrow)

        self.horizontalLayout_13.addWidget(self.dbmainb3)

        self.horizontalSpacer = QSpacerItem(73, 20, QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_13.addItem(self.horizontalSpacer)


        self.gridLayout_2.addLayout(self.horizontalLayout_13, 2, 0, 1, 1)

        self.horizontalSpacer_12 = QSpacerItem(358, 20, QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Minimum)

        self.gridLayout_2.addItem(self.horizontalSpacer_12, 1, 3, 1, 1)

        self.dbidxb3 = QPushButton(self.page_2)
        self.dbidxb3.setObjectName(u"dbidxb3")
        self.dbidxb3.setMinimumSize(QSize(80, 0))
        self.dbidxb3.setMaximumSize(QSize(80, 22))

        self.gridLayout_2.addWidget(self.dbidxb3, 2, 5, 1, 1)

        self.dbmainb2 = QPushButton(self.page_2)
        self.dbmainb2.setObjectName(u"dbmainb2")
        self.dbmainb2.setEnabled(False)
        self.dbmainb2.setMinimumSize(QSize(91, 22))
        self.dbmainb2.setMaximumSize(QSize(91, 22))

        self.gridLayout_2.addWidget(self.dbmainb2, 2, 2, 1, 1)

        self.horizontalSpacer_9 = QSpacerItem(80, 20, QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Minimum)

        self.gridLayout_2.addItem(self.horizontalSpacer_9, 1, 6, 1, 1)

        self.tableView = QTableView(self.page_2)
        self.tableView.setObjectName(u"tableView")
        self.tableView.setMinimumSize(QSize(1564, 835))

        self.gridLayout_2.addWidget(self.tableView, 3, 0, 1, 10)

        self.horizontalLayout_2 = QHBoxLayout()
        self.horizontalLayout_2.setSpacing(0)
        self.horizontalLayout_2.setObjectName(u"horizontalLayout_2")
        self.horizontalLayout_2.setSizeConstraint(QLayout.SizeConstraint.SetMinAndMaxSize)
        self.horizontalLayout_2.setContentsMargins(0, -1, -1, -1)
        self.horizontalSpacer_3 = QSpacerItem(0, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_2.addItem(self.horizontalSpacer_3)

        self.dbmainb1 = QPushButton(self.page_2)
        self.dbmainb1.setObjectName(u"dbmainb1")
        sizePolicy1.setHeightForWidth(self.dbmainb1.sizePolicy().hasHeightForWidth())
        self.dbmainb1.setSizePolicy(sizePolicy1)
        self.dbmainb1.setMinimumSize(QSize(80, 22))
        self.dbmainb1.setMaximumSize(QSize(80, 22))

        self.horizontalLayout_2.addWidget(self.dbmainb1)

        self.toollftb_2 = QToolButton(self.page_2)
        self.toollftb_2.setObjectName(u"toollftb_2")
        self.toollftb_2.setMinimumSize(QSize(24, 0))
        self.toollftb_2.setMaximumSize(QSize(24, 16777215))
        self.toollftb_2.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self.toollftb_2.setArrowType(Qt.ArrowType.LeftArrow)

        self.horizontalLayout_2.addWidget(self.toollftb_2)

        self.toolhomeb_2 = QToolButton(self.page_2)
        self.toolhomeb_2.setObjectName(u"toolhomeb_2")
        sizePolicy1.setHeightForWidth(self.toolhomeb_2.sizePolicy().hasHeightForWidth())
        self.toolhomeb_2.setSizePolicy(sizePolicy1)
        self.toolhomeb_2.setMinimumSize(QSize(483, 0))
        self.toolhomeb_2.setMaximumSize(QSize(483, 16777215))
        self.toolhomeb_2.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self.toolhomeb_2.setArrowType(Qt.ArrowType.UpArrow)

        self.horizontalLayout_2.addWidget(self.toolhomeb_2)

        self.toolrtb_2 = QToolButton(self.page_2)
        self.toolrtb_2.setObjectName(u"toolrtb_2")
        self.toolrtb_2.setMinimumSize(QSize(24, 0))
        self.toolrtb_2.setMaximumSize(QSize(24, 16777215))
        self.toolrtb_2.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self.toolrtb_2.setArrowType(Qt.ArrowType.RightArrow)

        self.horizontalLayout_2.addWidget(self.toolrtb_2)


        self.gridLayout_2.addLayout(self.horizontalLayout_2, 1, 9, 1, 1)

        self.horizontalLayout_15 = QHBoxLayout()
        self.horizontalLayout_15.setObjectName(u"horizontalLayout_15")
        self.dbmainlabel = QLabel(self.page_2)
        self.dbmainlabel.setObjectName(u"dbmainlabel")
        sizePolicy6.setHeightForWidth(self.dbmainlabel.sizePolicy().hasHeightForWidth())
        self.dbmainlabel.setSizePolicy(sizePolicy6)
        self.dbmainlabel.setMinimumSize(QSize(91, 0))
        self.dbmainlabel.setMaximumSize(QSize(405, 16777215))

        self.horizontalLayout_15.addWidget(self.dbmainlabel)

        self.horizontalSpacer_6 = QSpacerItem(0, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_15.addItem(self.horizontalSpacer_6)


        self.gridLayout_2.addLayout(self.horizontalLayout_15, 1, 0, 1, 3)

        self.horizontalLayout_17 = QHBoxLayout()
        self.horizontalLayout_17.setSpacing(2)
        self.horizontalLayout_17.setObjectName(u"horizontalLayout_17")
        self.horizontalLayout_17.setContentsMargins(0, -1, -1, -1)
        self.dbsymlabel = QLabel(self.page_2)
        self.dbsymlabel.setObjectName(u"dbsymlabel")
        sizePolicy1.setHeightForWidth(self.dbsymlabel.sizePolicy().hasHeightForWidth())
        self.dbsymlabel.setSizePolicy(sizePolicy1)

        self.horizontalLayout_17.addWidget(self.dbsymlabel)

        self.dbchka = QCheckBox(self.page_2)
        self.dbchka.setObjectName(u"dbchka")
        sizePolicy1.setHeightForWidth(self.dbchka.sizePolicy().hasHeightForWidth())
        self.dbchka.setSizePolicy(sizePolicy1)
        self.dbchka.setMinimumSize(QSize(21, 16))
        self.dbchka.setMaximumSize(QSize(21, 16))
        self.dbchka.setLayoutDirection(Qt.LayoutDirection.LeftToRight)

        self.horizontalLayout_17.addWidget(self.dbchka)


        self.gridLayout_2.addLayout(self.horizontalLayout_17, 2, 4, 1, 1)


        self.gridLayout_4.addLayout(self.gridLayout_2, 1, 0, 1, 1)

        self.stackedWidget.addWidget(self.page_2)

        self.gridLayout_6.addWidget(self.stackedWidget, 0, 0, 1, 1)

        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QMenuBar(MainWindow)
        self.menubar.setObjectName(u"menubar")
        self.menubar.setGeometry(QRect(0, 0, 1704, 19))
        self.menuRecent_changes = QMenu(self.menubar)
        self.menuRecent_changes.setObjectName(u"menuRecent_changes")
        self.menuHelp = QMenu(self.menubar)
        self.menuHelp.setObjectName(u"menuHelp")
        self.menuComm = QMenu(self.menubar)
        self.menuComm.setObjectName(u"menuComm")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QStatusBar(MainWindow)
        self.statusbar.setObjectName(u"statusbar")
        self.statusbar.setEnabled(True)
        self.statusbar.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.statusbar.setContextMenuPolicy(Qt.ContextMenuPolicy.DefaultContextMenu)
        self.statusbar.setAcceptDrops(True)
        self.statusbar.setAutoFillBackground(False)
        MainWindow.setStatusBar(self.statusbar)

        self.menubar.addAction(self.menuRecent_changes.menuAction())
        self.menubar.addAction(self.menuComm.menuAction())
        self.menubar.addAction(self.menuHelp.menuAction())
        self.menuRecent_changes.addSeparator()
        self.menuRecent_changes.addAction(self.actionStop)
        self.menuRecent_changes.addAction(self.actionSave)
        self.menuRecent_changes.addAction(self.actionClearh)
        self.menuRecent_changes.addAction(self.actionClear_expensions)
        self.menuRecent_changes.addAction(self.actionExit)
        self.menuHelp.addAction(self.actionResource)
        self.menuHelp.addAction(self.actionHelp)
        self.menuHelp.addAction(self.actionUpdates)
        self.menuHelp.addAction(self.actionAbout)
        self.menuComm.addAction(self.actionCommands_2)
        self.menuComm.addSeparator()
        self.menuComm.addSeparator()
        self.menuComm.addAction(self.actionQuick1)
        self.menuComm.addAction(self.actionDiag1)

        self.retranslateUi(MainWindow)

        self.stackedWidget.setCurrentIndex(0)
        self.combffile.setCurrentIndex(0)


        QMetaObject.connectSlotsByName(MainWindow)
    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"Recent Changes", None))
        self.actionExit.setText(QCoreApplication.translate("MainWindow", u"Exit", None))
        self.actionResource.setText(QCoreApplication.translate("MainWindow", u"Resource", None))
        self.actionAbout.setText(QCoreApplication.translate("MainWindow", u"About", None))
        self.actionStop.setText(QCoreApplication.translate("MainWindow", u"Stop", None))
        self.actionManual.setText(QCoreApplication.translate("MainWindow", u"Manual", None))
        self.actionSave.setText(QCoreApplication.translate("MainWindow", u"Save", None))
        self.actionUpdates.setText(QCoreApplication.translate("MainWindow", u"Check For Updates", None))
        self.actionClearh.setText(QCoreApplication.translate("MainWindow", u"Clear Hudt", None))
        self.actionLaunch.setText(QCoreApplication.translate("MainWindow", u"Launch Cmd Prompt", None))
        self.actionClear_expensions.setText(QCoreApplication.translate("MainWindow", u"Clear Extensions", None))
        self.actionHelp.setText(QCoreApplication.translate("MainWindow", u"Get Help", None))
        self.actionCreate_GPG_Key.setText(QCoreApplication.translate("MainWindow", u"Create GPG Key", None))
        self.actionChange_Base_Drive.setText(QCoreApplication.translate("MainWindow", u"Change Base Drive", None))
        self.actionTotal_Directories.setText(QCoreApplication.translate("MainWindow", u"Status Display", None))
        self.actionCommands_2.setText(QCoreApplication.translate("MainWindow", u"Quick Commands", None))
        self.actionQuick1.setText(QCoreApplication.translate("MainWindow", u"Edit Quick Cmnds", None))
        self.actionDiag1.setText(QCoreApplication.translate("MainWindow", u"Display Diagnostics", None))
        self.actionChange_Default_Location.setText(QCoreApplication.translate("MainWindow", u"Explr/Cmd Popup Dir", None))
        self.jpgcr.setText(QCoreApplication.translate("MainWindow", u"TextLabel", None))
        self.ftimesecondsl.setText(QCoreApplication.translate("MainWindow", u"Seconds", None))
        self.combftimeout.setItemText(0, QCoreApplication.translate("MainWindow", u"AppData", None))

#if QT_CONFIG(tooltip)
        self.combftimeout.setToolTip(QCoreApplication.translate("MainWindow", u"unfiltered search output", None))
#endif // QT_CONFIG(tooltip)
        self.ilabel.setText(QCoreApplication.translate("MainWindow", u"Index", None))
        self.toollftb.setText(QCoreApplication.translate("MainWindow", u"1", None))
        self.toolhomeb.setText(QCoreApplication.translate("MainWindow", u"2", None))
        self.toolrtb.setText(QCoreApplication.translate("MainWindow", u"3", None))
        self.stimeb.setText(QCoreApplication.translate("MainWindow", u"Search", None))
        self.ftimeb.setText(QCoreApplication.translate("MainWindow", u"5 Min Search", None))
        self.rmvButton.setText(QCoreApplication.translate("MainWindow", u"Remove", None))
#if QT_CONFIG(tooltip)
        self.ntbrowseb.setToolTip(QCoreApplication.translate("MainWindow", u"newer than a file", None))
#endif // QT_CONFIG(tooltip)
        self.ntbrowseb.setText(QCoreApplication.translate("MainWindow", u"Browse", None))
#if QT_CONFIG(tooltip)
        self.ftimebf.setToolTip(QCoreApplication.translate("MainWindow", u"output desktop", None))
#endif // QT_CONFIG(tooltip)
        self.ftimebf.setText(QCoreApplication.translate("MainWindow", u"5 Min Filtered", None))
#if QT_CONFIG(tooltip)
        self.stimebf.setToolTip(QCoreApplication.translate("MainWindow", u"by time output desktop", None))
#endif // QT_CONFIG(tooltip)
        self.stimebf.setText(QCoreApplication.translate("MainWindow", u"Filtered", None))
        self.mftmainlabel.setText(QCoreApplication.translate("MainWindow", u"System Mft", None))
#if QT_CONFIG(tooltip)
        self.downloadButton.setToolTip(QCoreApplication.translate("MainWindow", u"scan for new files by folder mtime", None))
#endif // QT_CONFIG(tooltip)
        self.downloadButton.setText(QCoreApplication.translate("MainWindow", u"Downloads", None))
        self.jpgv.setText("")
        self.jpgb.setText(QCoreApplication.translate("MainWindow", u"Custom", None))
        self.tomlb.setText(QCoreApplication.translate("MainWindow", u"Settings", None))
#if QT_CONFIG(tooltip)
        self.diffchkc.setToolTip(QCoreApplication.translate("MainWindow", u"Show miss rate and other metric differences in diff file", None))
#endif // QT_CONFIG(tooltip)
        self.diffchkc.setText(QCoreApplication.translate("MainWindow", u"Symmetrics", None))
        self.ntsb.setText(QCoreApplication.translate("MainWindow", u"Search", None))
        self.combffile.setItemText(0, "")

        self.combffile.setCurrentText("")
        self.ffileb2.setText(QCoreApplication.translate("MainWindow", u"+", None))
        self.ffilelabel2.setText(QCoreApplication.translate("MainWindow", u"Extension", None))
#if QT_CONFIG(tooltip)
        self.ntbrowseb2.setToolTip(QCoreApplication.translate("MainWindow", u"search newer than a folder", None))
#endif // QT_CONFIG(tooltip)
        self.ntbrowseb2.setText(QCoreApplication.translate("MainWindow", u"Folder", None))
        self.addButton.setText(QCoreApplication.translate("MainWindow", u"Add", None))
        self.combd.setItemText(0, QCoreApplication.translate("MainWindow", u"C:\\", None))

#if QT_CONFIG(tooltip)
        self.combd.setToolTip(QCoreApplication.translate("MainWindow", u"drive which to search for new files", None))
#endif // QT_CONFIG(tooltip)
        self.ffilelabel1.setText(QCoreApplication.translate("MainWindow", u"Find file", None))
        self.ntlabel.setText(QCoreApplication.translate("MainWindow", u"Newer than", None))
        self.ftimelabel1.setText(QCoreApplication.translate("MainWindow", u"Output", None))
        self.dlabel.setText(QCoreApplication.translate("MainWindow", u"Find", None))
#if QT_CONFIG(tooltip)
        self.queryButton.setToolTip(QCoreApplication.translate("MainWindow", u"info and statistics from searches and filter hits", None))
#endif // QT_CONFIG(tooltip)
        self.queryButton.setText(QCoreApplication.translate("MainWindow", u"Query", None))
#if QT_CONFIG(tooltip)
        self.diffchkb.setToolTip(QCoreApplication.translate("MainWindow", u"scan idx and append diff file", None))
#endif // QT_CONFIG(tooltip)
        self.diffchkb.setText(QCoreApplication.translate("MainWindow", u"Postop scan idx", None))
        self.mftsearchb.setText(QCoreApplication.translate("MainWindow", u"Search", None))
#if QT_CONFIG(tooltip)
        self.hudt.setToolTip(QCoreApplication.translate("MainWindow", u"hudt", None))
#endif // QT_CONFIG(tooltip)
        self.combt.setItemText(0, QCoreApplication.translate("MainWindow", u"Filtered", None))
        self.combt.setItemText(1, QCoreApplication.translate("MainWindow", u"Unfiltered", None))

#if QT_CONFIG(tooltip)
        self.combt.setToolTip(QCoreApplication.translate("MainWindow", u"default apply filter", None))
#endif // QT_CONFIG(tooltip)
        self.ffilecb.setText(QCoreApplication.translate("MainWindow", u"Compress", None))
        self.combffileout.setItemText(0, QCoreApplication.translate("MainWindow", u"AppData", None))

        self.ffileb.setText(QCoreApplication.translate("MainWindow", u"Search", None))
        self.mftlabel4.setText(QCoreApplication.translate("MainWindow", u"Save Mft", None))
#if QT_CONFIG(tooltip)
        self.mftchkb.setToolTip(QCoreApplication.translate("MainWindow", u"dump raw mft", None))
#endif // QT_CONFIG(tooltip)
        self.mftchkb.setText("")
        self.resetButton.setText(QCoreApplication.translate("MainWindow", u"Stop Defaults", None))
        self.mftlabel3.setText(QCoreApplication.translate("MainWindow", u"RamDisk", None))
#if QT_CONFIG(tooltip)
        self.mftchka.setToolTip(QCoreApplication.translate("MainWindow", u"where to do any work save or temp file location", None))
#endif // QT_CONFIG(tooltip)
        self.mftchka.setText("")
        self.mftrange.setSuffix("")
        self.mftrange.setPrefix("")
        self.mftlabel2.setText(QCoreApplication.translate("MainWindow", u"(Range) Hrs", None))
#if QT_CONFIG(tooltip)
        self.mftbrowseb.setToolTip(QCoreApplication.translate("MainWindow", u"convert raw mft to csv", None))
#endif // QT_CONFIG(tooltip)
        self.mftbrowseb.setText(QCoreApplication.translate("MainWindow", u"Browse", None))
#if QT_CONFIG(tooltip)
        self.mftlabel1.setToolTip(QCoreApplication.translate("MainWindow", u"convert to csv", None))
#endif // QT_CONFIG(tooltip)
        self.mftlabel1.setText(QCoreApplication.translate("MainWindow", u"Import Mft", None))
#if QT_CONFIG(tooltip)
        self.diffchka.setToolTip(QCoreApplication.translate("MainWindow", u"create tsv sheet from specified search", None))
#endif // QT_CONFIG(tooltip)
        self.diffchka.setText(QCoreApplication.translate("MainWindow", u"Postop file doctrine", None))
        self.difflabel.setText(QCoreApplication.translate("MainWindow", u"Options", None))
        self.dbidxb1.setText(QCoreApplication.translate("MainWindow", u"Clear IDX", None))
        self.dbidxb2.setText(QCoreApplication.translate("MainWindow", u"Build IDX", None))
        self.dbmainb3.setText(QCoreApplication.translate("MainWindow", u"1", None))
        self.dbidxb3.setText(QCoreApplication.translate("MainWindow", u"Scan IDX", None))
#if QT_CONFIG(tooltip)
        self.dbmainb2.setToolTip(QCoreApplication.translate("MainWindow", u"combine sys2 with sys table", None))
#endif // QT_CONFIG(tooltip)
        self.dbmainb2.setText(QCoreApplication.translate("MainWindow", u"Super Impose", None))
#if QT_CONFIG(tooltip)
        self.dbmainb1.setToolTip(QCoreApplication.translate("MainWindow", u"remove cache items from logs and stats table", None))
#endif // QT_CONFIG(tooltip)
        self.dbmainb1.setText(QCoreApplication.translate("MainWindow", u"Clear cache", None))
        self.toollftb_2.setText(QCoreApplication.translate("MainWindow", u"1", None))
        self.toolhomeb_2.setText(QCoreApplication.translate("MainWindow", u"2", None))
        self.toolrtb_2.setText(QCoreApplication.translate("MainWindow", u"3", None))
        self.dbmainlabel.setText(QCoreApplication.translate("MainWindow", u"Status: Offline", None))
        self.dbsymlabel.setText(QCoreApplication.translate("MainWindow", u"symmetrics", None))
#if QT_CONFIG(tooltip)
        self.dbchka.setToolTip(QCoreApplication.translate("MainWindow", u"Show miss rate in difference file ect", None))
#endif // QT_CONFIG(tooltip)
        self.dbchka.setText("")
        self.menuRecent_changes.setTitle(QCoreApplication.translate("MainWindow", u"File", None))
        self.menuHelp.setTitle(QCoreApplication.translate("MainWindow", u"Help", None))
        self.menuComm.setTitle(QCoreApplication.translate("MainWindow", u"Comm", None))
    # retranslateUi

