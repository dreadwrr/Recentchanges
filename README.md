Windows 10 11 <br>
Released 3.0.6 <br>
![Alt text](https://i.imgur.com/yNnttmU.png) ![Alt text](https://i.imgur.com/gqbO4HB.png) <br><br>

Can be used with system python or one step pyinstall build. A windows setup version is available if python isnt installed <br><br>
3.0.6 added add to and remove from path in settings and if powershell 7 is installed it uses pwsh and there is a speed boost. also there is python scandir which can be used with pwrshell = False in config.<br>

To enable ansi text in powershell try the following command <br>
Set-ItemProperty HKCU:\Console VirtualTerminalLevel -Type DWORD 1<br><br>

With Hybrid analysis and MFT search <br><br>
GPG, Ntfstools, 7-zip included <br><br>
<p>
Save encrypted notes <br>
Select from a number of fonts or install your own. also 5 themes unix, wb (white on black), solar, monochrome or modern  <br>
Quick commands displays saved commands for easy reference <br>
Create a custom crest with a .png image file max size 255 x 333 <br>
Will use gpg4win system install or app gpg if not installed <br>
Full commandline support with recentchanges.bat and rnt.bat <br>

find file or files by extension and also compress to a .zip/.rar archive by time<br>
A new feature Find new files fast with a  drive index that is stored in a .gpg cache file loaded into memory.<br><br>
Scan the system index independtly with scan IDX from the main hybrid analysis to catch files that have a different checksum but same modified 
or faked modified time.  <br><br>

Commands: <br>
./recentchanges <br>
./recentchanges search <br>
./recentchanges reset <br>
./recentchanges query <br>
</p><br>

if python is installed. <br><br>
If you dont have python installed there is a Windows setup version available <br>
## Installation
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
pyinstaller main.spec icon=rntchanges.ico <br><br>
copy _internal and main from in dist folder to app folder <br>
<br>
also to use the commandline with main the bat file has to be edited recentchanges.bat or rnt.bat remove python and change src\set_recent_helper.py to main in two places <br><br>

Things to do after installation. Recommend installing notepad++ and changing dspEDITOR to notepad++ <br><br>

Ntfstools included for Mft options, also supports the sleuth kit icat if placed in \bin <br>
See required tsk files above <br>
order is  mftecmd, parser, icat\fstat and ntfstools so remove others when using specific one <br>

further pyinstall documentation https://github.com/dreadwrr/Recent-Pyinstaller<br>
which is just updating the .bat files to point to the executable <br><br>

Links for MFTCmd with cutoff allowing to read the Mft into memory while doing an Mft search. <br>
https://docs.google.com/document/d/1EJAKd1v41LTLN74eXHf5N_BdvGYlfU5Ai8oWBDSGeho/edit?tab=t.0#bookmark=id.ct8qv65gr0wc <br><br>

# Nuitka instruction <br>
nuitka main.py --mode=standalone --enable-plugin='pyside6' --remove-output --include-qt-plugins=sqldrivers --windows-icon-from-ico=recentchanges.ico <br><br>
![Alt text](https://i.imgur.com/p1kuXYp.png) <br><br>
![Alt text](https://i.imgur.com/6q4THX4.png) <br>
The app can be used without included gpg or 7-zip and would use gpg4win. if 7-zip or winrar arent installed will use python zipfile. <br>

windows ui file https://drive.google.com/file/d/1DAHvPq4yqNQNxdVmFLM0SyX5lRF2Qs72/view?usp=sharing <br><br>
mft sources <br>
omerbenamram mft dump and python hooks <br>
Eric Zimmerman MFTECmd <br>
thewhiteninja ntfstools <br>
brian carrier the sleuthkit (tsk) <br>
ignacioj/mftf github <br>

