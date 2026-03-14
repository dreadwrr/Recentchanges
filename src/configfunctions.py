import getpass
import os
import shutil
import subprocess
import sys
try:
    import pwd  # linux
except ImportError:
    pwd = None
from pathlib import Path


# app location if files are moved to a src or separate directory its the one below it
def find_install():
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent.parent


def not_absolute(user_path: str, quiet=False) -> bool:
    p = Path(user_path)
    if p.is_absolute():
        if not quiet:
            print("proteus_EXTN path cant be absolute: ", p)
        # raise ValueError("Absolute paths not allowed")
        return False
    return True


def check_config(paths, nogo, filterout):
    for entry in paths + nogo + filterout:
        if not not_absolute(entry):
            return False
    return True


def get_user():
    """ read from environ inaccurate """
    user = None
    try:
        user = getpass.getuser()
    except (KeyError, OSError):
        print("unable to get username attempting fallback")
    if user:
        return user
    else:
        try:
            user = Path.home().parts[-1]
        except RuntimeError as e:
            raise RuntimeError("unable to find current user.") from e
        if user:
            return user
    return None


def user_info(user=None):
    if pwd is None:
        raise ValueError("pwd module is unavailable on this platform")
    try:
        if user:
            usr_info = pwd.getpwnam(user)
        else:
            usr_info = pwd.getpwuid(os.geteuid())
        USR = usr_info.pw_name
        uid = usr_info.pw_uid
        gid = usr_info.pw_gid  # gid = grp.getgrnam(user).gr_gid
        home_dir = Path(usr_info.pw_dir)

        return USR, uid, gid, home_dir
    except (KeyError, OSError) as e:
        raise ValueError(f"unable to get info for {user if user else 'current user'}") from e


def find_user_folder(folder_name="Desktop"):
    home = Path.home()
    onedrive_envs = ["OneDrive", "OneDriveConsumer"]
    for env in onedrive_envs:
        onedrive_path = os.environ.get(env)
        if onedrive_path:
            folder = Path(onedrive_path) / folder_name
            if folder.exists():
                return folder
    onedrive_paths = list(home.glob("OneDrive*/" + folder_name))
    if onedrive_paths:
        return onedrive_paths[0]
    fallback = home / folder_name
    if fallback.exists():
        return fallback
    return None


def windows_user_info():
    try:
        user = get_user()
        # home_dir = Path.home()
        return user  # , home_dir
    except (KeyError, OSError) as e:
        raise ValueError("unable to get info for user") from e


def get_default_user(path="/usr/local/bin/recentchanges"):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line.startswith("default_user="):
                value = line.split("=", 1)[1]
                return value.strip().strip('"')
    return None


def ensure_default_utils():
    required = ["md5sum", "find"]
    missing = [cmd for cmd in required if shutil.which(cmd) is None]
    if missing:
        missing_str = ", ".join(missing)
        raise RuntimeError(f"Missing required utility(s): {missing_str}")

    try:
        out = subprocess.run(
            ["find", "--version"],
            capture_output=True,
            text=True,
            check=True
        ).stdout
    except Exception as e:
        raise RuntimeError(f"Unable to validate GNU find: {type(e).__name__} {e}")

    if "GNU findutils" not in out:
        raise RuntimeError("Unsupported `find` detected. GNU findutils is required.")


def get_xdg_runtime(uid):
    # uid_str = os.environ.get("PKEXEC_UID")
    # if uid_str:
    #     runtime_dir = f"/run/user/{uid_str}"
    # else:
    # raise RuntimeError("Not launched via pkexec (PKEXEC_UID missing)")
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR")
    if not runtime_dir:
        if uid is None:
            raise RuntimeError("environment xdg_runtime_dir not set and there is uid is None")
        runtime_dir = f"/run/user/{uid}"
    xdg_runtime = Path(runtime_dir)
    if not xdg_runtime.is_dir() or (int(uid) == 0 and xdg_runtime.stat().st_uid != 0):
        xdg_runtime = Path(f"/tmp/recentchanges-{uid}")
        os.makedirs(xdg_runtime, mode=0o700, exist_ok=True)
    # print("runtime_dir", runtime_dir)
    return xdg_runtime


# toml


def get_config(appdata_local=None, user=None, platform="Windows"):
    """ user configuration location """
    config_copy = "config (copy).toml"
    json_copy = "usrprofile (copy).json"

    if not appdata_local:
        appdata_local = find_install()
    config_home = appdata_local / "config"
    default_conf = config_home / config_copy
    default_json = config_home / json_copy

    if platform.lower() == "windows":
        if not user:
            user = windows_user_info()

        toml_file = config_home / "config.toml"
        json_file = config_home / "usrprofile.json"

        toml_missing = not toml_file.is_file()
        json_missing = not json_file.is_file()
        # first_time_setup = toml_missing and json_missing

        if toml_missing and default_conf.is_file():
            shutil.copy(default_conf, toml_file)

        if json_missing and default_json.is_file():
            shutil.copy(default_json, json_file)

        # if first_time_setup:
        #     ensure_default_utils()

        if toml_file.is_file():
            return toml_file, json_file, user
        raise FileNotFoundError(f"Unable to find config.toml config file in {config_home}")

    if platform.lower() == "linux":

        user, uid, gid, home_dir = user_info(user)

        xdg_config = os.environ.get("XDG_CONFIG_HOME")

        if xdg_config:
            config_home = Path(xdg_config)

        elif home_dir:
            config_home = home_dir / ".config"
        # fallback
        else:
            if user == "root":
                default_conf_home = "/root/.config"
            else:
                default_conf_home = f"/home/{user}/.config"
            config_home = Path(default_conf_home)

        config_local = config_home / "recentchanges"

        toml_file = config_local / "config.toml"
        json_file = config_local / "usrprofile.json"

        os.makedirs(config_local, mode=0o755, exist_ok=True)
        toml_missing = not toml_file.is_file()
        json_missing = not json_file.is_file()
        first_time_setup = toml_missing and json_missing

        if toml_missing and default_conf.is_file():
            shutil.copy(default_conf, toml_file)
            if user != "root":
                if os.path.isfile(toml_file):
                    os.chown(toml_file, uid, gid)

        if json_missing and default_json.is_file():
            shutil.copy(default_json, json_file)
            if user != "root":
                if os.path.isfile(json_file):
                    os.chown(json_file, uid, gid)

        if first_time_setup:
            ensure_default_utils()

        xdg_runtime = get_xdg_runtime(uid)
        if toml_file.is_file():
            return toml_file, json_file, home_dir, xdg_config, xdg_runtime, user, uid, gid
        raise FileNotFoundError(f"Unable to find config.toml config file in {config_local}")
