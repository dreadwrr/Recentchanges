from dataclasses import dataclass
from pathlib import Path
from typing import Dict
from .config import load_toml
from .configfunctions import get_config
from .configfunctions import find_user_folder
from .fsearchfunctions import get_file_id
from .pyfunctions import epoch_to_date
from .pyfunctions import epoch_to_str
from .pyfunctions import user_path
from .fsearchfunctions import file_owner


@dataclass
class ConfigData:
    USRDIR: Path
    toml_file: Path
    json_file: Path
    log_file: Path
    config: Dict
    EXCLDIRS: list
    nogo: list
    filterout_list: list
    driveTYPE: str
    ll_level: str


# read and return the configs for dirwalker to avoid passing too many arguments
def get_config_data(appdata_local, USR):

    toml_file, json_file, _ = get_config(appdata_local, USR, platform="Windows")

    USRDIR = find_user_folder("Desktop")
    if USRDIR is None:
        print("Could not find user Desktop folder")
        return None
    config = load_toml(toml_file)
    if not config:
        None
    EXCLDIRS = user_path(config['search']['EXCLDIRS'], USR)
    nogo = user_path(config['shield']['nogo'], USR)
    filterout_list = user_path(config['shield']['filterout'], USR)
    driveTYPE = config['search']['driveTYPE']
    ll_level = config['logs']['logLEVEL']
    log_file = config['logs']['userLOG']
    log_file = appdata_local / "logs" / log_file

    return ConfigData(USRDIR, toml_file, json_file, log_file, config, EXCLDIRS, nogo, filterout_list, driveTYPE, ll_level)


def return_info(file_path, st, symlink, link_target, log_q):
    fmt = "%Y-%m-%d %H:%M:%S"
    sym = target = hardlink = None

    if symlink:
        sym = "y"
        target = link_target
    # attrs = getattr(st, "st_file_attributes", 0)
    # mode = get_mode(attrs, sym)
    # inode = st.st_ino
    # hardlink = st.st_nlink

    inode, _, hardlink, _, mode, status = get_file_id(file_path, log_q)  # reparse c_time
    if status in ("Nosuchfile", "Error"):
        return sym, target, mode, inode, hardlink, None, None, None, st.st_mtime_ns, None, None, None, st.st_size, status
    resolve_owner = file_owner(file_path, log_q)
    if resolve_owner in (None, "Nosuchfile"):
        return sym, target, mode, inode, hardlink, None, None, None, st.st_mtime_ns, None, None, None, st.st_size, status
    owner, domain = resolve_owner if resolve_owner else (None, None)

    m_epoch = st.st_mtime
    m_epoch_ns = st.st_mtime_ns
    c_epoch = st.st_birthtime
    a_epoch = st.st_atime
    m_dt = epoch_to_date(m_epoch)
    m_time = m_dt.strftime(fmt)
    c_time = epoch_to_str(c_epoch)
    a_time = epoch_to_str(a_epoch)
    size = st.st_size
    return sym, target, mode, inode, hardlink, owner, domain, m_dt, m_epoch_ns, m_time, c_time, a_time, size, status


def get_extension_tup(extension):
    extn_set = set()
    is_noextension = False
    for e in extension:
        if e:
            extn_set.add(e.lower())
        else:
            is_noextension = True
    return tuple(extn_set), is_noextension
