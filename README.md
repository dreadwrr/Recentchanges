Last updated: 06/09/2026 <br>
Windows 10 11 <br>



![Alt text](https://i.imgur.com/yNnttmU.png) ![Alt text](https://i.imgur.com/gqbO4HB.png) <br><br>
File search application with Hybrid analysis and MFT search <br><br>
Can be used with system python or one step pyinstall build. A windows setup version is available if python isnt installed <br>

GPG included or can use gpg4win. Can use system 7-zip winrar. default python zipfile. <br> <br><br>

<p>
Save encrypted notes <br>
Select from a number of fonts or install your own. also 5 themes unix, wb (white on black), solar, monochrome or modern  <br>
Quick commands displays saved commands for easy reference <br>
Create a custom crest with a .png image file max size 255 x 333 <br>
Will use gpg4win system install or app gpg if not installed <br>
Full commandline support with recentchanges.bat and rnt.bat <br>

find file or files by extension and also compress to a .zip/.rar archive by time<br>
A new feature Find new files fast with a  drive index that is stored in a .gpg cache file loaded into memory.<br><br>
Scan the system index independently with scan IDX from the main hybrid analysis to catch files that have a different checksum but same modified 
or faked modified time.  <br><br>

Add to path from settings buttom <br>
Commands: <br>
./recentchanges <br>
./recentchanges search <br>
./recentchanges reset <br>
./recentchanges query <br>
</p><br>
if encountering gpg key issues or some other problem try recentchanges reset. Which can be added to path from setup button <br><br>

## Troubleshooting

before installation and setup up want to cover steps to do in case of other cases that could be encountered. <br>
I will do my best to automate the most with recentchanges reset <br><br>

This applies to GPG4Win <br>
If it says cannot open instance of qt and then no pinentry it is a condition in windows where the agent wasnt fully activated and can be resovled with: <br>

from in Kleopatra - Tools - Restart Background Processes

if python is installed. <br><br>
If you dont have python installed there is a Windows setup version available <br>
# Installation
make a venv if preferred <br>
pip install -r requirements.txt <br>
python main.py <br><br>
may require packages requests, packaging<br>

# Pyinstaller instructions <br>
remove old dist folder <br>
create a venv <br>
python -m venv .venv <br>
.\\.venv\Scripts\Activate.ps1 <br>
python -m pip install --upgrade pip <br>
pip install -r requirements.txt <br>
pip install pyinstaller <br>
pyinstaller main.spec <br><br>
copy _internal and main from in dist folder to app folder <br>
<br>
also to use the commandline with main edit bat files recentchanges.bat or rnt.bat. <br>
remove python and change src\set_recent_helper.py to main in two places <br><br>

Things to do after installation. Recommend installing notepad++ and changing dspEDITOR to notepad++ <br>


further pyinstall documentation https://github.com/dreadwrr/Recent-Pyinstaller<br>
which is just updating the .bat files to point to the executable <br><br>

# Nuitka instruction <br>

This can be built with nuitka to produce a single binary that extracts at runtime

pip install nuitka <br>

```
python -m nuitka --onefile --output-filename=main.exe `
--onefile-tempdir-spec="{TEMP}\onefile_{PID}_YD1fmvHJ_Qc" `
--remove-output --enable-plugin=pyside6 `
--noinclude-qt-plugins=printsupport `
--include-qt-plugins=sensible,platforms,sqldrivers `
--windows-icon-from-ico=Resources\recentchanges.ico `
--windows-uac-admin `
--jobs=4 main.py
```

remove main.dist and main.build

The following would put the output into main.dist but is not as organized as pyinstaller. There are too many files beside the executable
so not using for main build

nuitka main.py --mode=standalone --enable-plugin='pyside6' --remove-output --include-qt-plugins=sqldrivers --windows-icon-from-ico=Resources\recentchanges.ico <br><br>

##
To enable ansi text in powershell try the following command <br>
Set-ItemProperty HKCU:\Console VirtualTerminalLevel -Type DWORD 1<br>

Ntfstools included for Mft saving, also can use the sleuth kit icat if placed in \bin <br>
See required tsk files https://drive.google.com/file/d/16suEa4ohxFHdAlGsuTKjW_EgdweF9O1x/view?usp=sharing <br>
order is: icat\fstat and ntfstools. and other can be removed <br><br>

[![Alt text](https://i.imgur.com/6q4THX4.png)](https://i.imgur.com/6q4THX4.png) <br>

windows ui file https://drive.google.com/file/d/1DAHvPq4yqNQNxdVmFLM0SyX5lRF2Qs72/view?usp=sharing <br><br>
mft sources <br>
omerbenamram mft dump and python hooks <br>
Eric Zimmerman MFTECmd <br>
thewhiteninja ntfstools <br>
brian carrier the sleuthkit (tsk) <br>
ignacioj/mftf github <br>

