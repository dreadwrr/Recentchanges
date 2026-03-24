Windows 10 11 <br>
![Alt text](https://i.imgur.com/yNnttmU.png) <br><br>

# Recentchanges
New release! all updates from recentchanges Qt linux  <br>
To enable ansi text in powershell try the following command <br>
Set-ItemProperty HKCU:\Console VirtualTerminalLevel -Type DWORD 1<br><br>
search with Powershell or windows subsystem of linux. <br>
With Hybrid analysis and MFT search <br><br>
GPG, Ntfstools, 7-zip included <br><br>
<p>
Save encrypted notes <br>
Select from a number of fonts or install your own. also 5 themes unix, wb (white on black), solar, monochrome or modern  <br>
Quick commands displays saved commands for easy reference <br>
Create a custom crest with a .png image file max size 255 x 333 <br>
Will use gpg4win system install or app gpg if not installed <br>
Full commandline support with recentchanges.bat and rnt.bat <br>

Full commandline support with: <br>
./recentchanges <br>
./recentchanges search <br>
./recentchanges reset <br>
./recentchanges query <br>
</p><br>
Ntfstools included for Mft options, also supports the sleuth kit icat if placed in \bin <br>
See required tsk files above <br>
order is  mftecmd, icat\fstat and ntfstools so remove others when using specific one <br><br>

Links for MFTCmd with cutoff allowing to read the Mft into memory while doing an Mft search. <br>
https://docs.google.com/document/d/1EJAKd1v41LTLN74eXHf5N_BdvGYlfU5Ai8oWBDSGeho/edit?tab=t.0#bookmark=id.ct8qv65gr0wc <br><br>

if python is installed. <br><br>
If you dont have python installed use Windows Setup in releases <br>
## Installation
make a venv if preferred <br>
pip install -r requirements.txt <br>
python main.py <br><br>

may require packages requests, packaging

<br><br>
find file or files by extension and also compress to a .zip/.rar archive by time<br>
A new feature Find new files fast with a  drive index that is stored in a .gpg cache file loaded into memory.

Scan the system index independtly with scan IDX from the main hybrid analysis to catch files that have a different checksum but same modified 
or faked modified time.  <br><br>
The application works with MFTCmd official\standard or the MFTECmd cutoff version if placed in \bin <br>
`icat` and `fsstat` can be used alternatively to ntfstools see tskrequiredfiles.txt and placed in \bin <br><br>

# Pyinstaller instructions <br>
remove old dist folder <br>
create a venv <br>
python -m venv .venv <br>
.\\.venv\Scripts\Activate.ps1 <br>
python -m pip install --upgrade pip <br>
pip install -r requirements.txt <br>
pip install pyinstaller <br>
pyinstaller main.spec icon=rntchanges.ico <br><br>
copy _internal and main to app folder <br>
<br>
also recentchanges.bat and rnt.bat change remove python in two places and change src\set_recent_helper.py to main <br>
further pyinstall documentation https://github.com/dreadwrr/Recent-Pyinstaller<br>
which is just updating the .bat files to point to the executable <br><br>


![Alt text](https://i.imgur.com/6q4THX4.png) <br>
The app can be used without included gpg or 7-zip and would use gpg4win. if 7-zip or winrar arent installed will use python zipfile. <br>
powershell is default and uses PSSQLite for data typing and accuracy <br><br>
mft sources <br>
omerbenamram mft dump and python hooks <br>
Eric Zimmerman MFTECmd <br>
thewhiteninja ntfstools <br>
brian carrier the sleuthkit (tsk) <br>
ignacioj/mftf github <br>

