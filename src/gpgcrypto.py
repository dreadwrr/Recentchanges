import csv
import os
import re
import sys
import shlex
import subprocess
import traceback
from io import StringIO
from typing import Any
from src.rntchangesfunctions import cnc


def encr_cache(cfr, CACHE_F, email, compLVL):
    data_to_write = dict_to_list(cfr)
    ctarget = dict_string(data_to_write)

    nc = cnc(CACHE_F, compLVL)

    rlt = encrm(ctarget, CACHE_F, email, no_compression=nc, armor=False)
    if not rlt:
        print("Reencryption failed cache not saved.")


def encr_sys_cache(dir_data, CACHE_S, email):
    data_to_write = dict_to_list_sys(dir_data)
    ctarget = dict_string(data_to_write)
    if encrm(ctarget, CACHE_S, email, no_compression=False, armor=False):
        return True
    return False


# enc mem
def encrm(c_data: str, opt: str, r_email: str, user=None, no_compression=False, armor=False) -> bool:
    try:

        cmd = ["gpg", "--batch", "--yes", "--encrypt", "-r", r_email, "-o", opt]

        if no_compression:
            cmd.extend(["--compress-level", "0"])

        if armor:
            cmd.append("--armor")

        subprocess.run(
            cmd,
            input=c_data.encode("utf-8"),
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return True

    except subprocess.CalledProcessError as e:
        err_msg = e.stderr.decode().strip() if e.stderr else str(e)
        print(f"[ERROR] Cache Encryption failed: {err_msg}")
    return False


# dec mem
def decrm(src: str, quiet=False) -> str | None:
    if os.path.isfile(src):
        try:
            cmd = [
                "gpg",
                "--quiet",
                "--batch",
                "--yes",
                "--decrypt",
                src
            ]

            result = subprocess.run(cmd, check=True, capture_output=True, text=True, encoding="utf-8")

            return result.stdout  # decrypted CSV content
        except subprocess.CalledProcessError as e:
            if not quiet:
                print(f"[ERROR] Decryption failed: {e}")
            return None
    else:
        print('No such .gpg file')
        return None


def encr(database, opt, email, no_compression=False, dcr=False):
    try:
        cmd = [
            "gpg",
            "--yes",
            "--encrypt",
            "-r", email,
            "-o", opt,
        ]
        if no_compression:
            cmd.extend(["--compress-level", "0"])
        cmd.append(database)
        subprocess.run(cmd, capture_output=True, text=True, check=True)
        if not dcr:
            os.remove(database)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to encrypt:  {e} return_code: {e.returncode}")
        combined = "\n".join(filter(None, [e.stdout, e.stderr]))
        if combined:
            print("[OUTPUT]\n" + combined)
    except FileNotFoundError:
        print("[ERROR] GPG not found. Please ensure GPG is installed.")
    except Exception as e:
        print(f"[ERROR] : {e} {type(e).__name__} \n {traceback.format_exc()}")
    return False


def decr(src, opt):
    if os.path.isfile(src):
        try:
            cmd = [
                "gpg",
                "--yes",
                "--decrypt",
                "-o", opt,
                src
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            if result.returncode == 0:
                return True
            else:
                print(f"[ERROR] Decryption failed with exit code {result.returncode}")
                print(f"stderr: {result.stderr}")
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Decryption failed: {e}")
            print(f"stderr: {e.stderr}")
        except FileNotFoundError:
            print("GPG not found. Please ensure GPG is installed.")
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}")
    else:
        print(f"[ERROR] File {src} not found. Ensure the .gpg file exists.")

    return False


def decr_ctime(CACHE_F: str) -> dict:
    if not CACHE_F or not os.path.isfile(CACHE_F):
        return {}

    csv_path = decrm(CACHE_F)
    if not csv_path:
        if csv_path is None:
            print("if having problems run recentchanges reset to clear .gpg files and keys")
        print(f"Unable to retrieve cache file {CACHE_F}. the cache file might be corrupt removing it may resolve problem quitting.")
        sys.exit(1)

    cfr_src = {}
    reader = csv.DictReader(StringIO(csv_path), delimiter='|')

    for row in reader:
        root = row.get('root')
        if not root:
            continue

        # normalize types
        try:
            size = int(row['size']) if row.get('size') else None
        except ValueError:
            size = None
        try:
            modified_ep = int(row['modified_ep']) if row.get('modified_ep') else None
        except ValueError:
            modified_ep = None
        if modified_ep is None:
            continue
        cfr_src.setdefault(root, {})[modified_ep] = {
            "checksum": row.get('checksum', None),
            "size": size,
            "modified_time": row.get('modified_time', None),
            "owner": row.get('owner', None),
            "domain": row.get('domain', None)
        }

    return cfr_src


# commandline start the users gpg agent before decrypting the cache file ***
def start_user_agent(gpg_file):
    cmd = ["gpg", "--decrypt", "--dry-run", gpg_file]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    stderr = result.stderr
    if stderr:
        for line in stderr.splitlines():
            if "no secret key" in line.lower():
                print(line)
                print(f"No key for {gpg_file} delete the file to reset")
                return False
    return result.returncode == 0


# Qt precache or refresh gpg passphrase.
def start_gpg_agent(email):
    """ purpose to know when passphrase has expired then prompt with Qt dialog """
    result = subprocess.run(["gpg", "--default-key", email, "--pinentry-mode", "loopback", "--passphrase", "phraseunkown", "-s"], input=b"", capture_output=True)
    for line in result.stderr.split(b'\n'):
        # print(line.decode('utf-8', errors='ignore'))
        # if b"ioctl" in line.lower():  # wont show up with loopback
        #     return None
        if email.encode() not in line:
            if b"bad passphrase" in line.lower():
                # print(line.decode('utf-8', errors='ignore'))
                return False
            # print(line.decode('utf-8', errors='ignore'))
    for line in result.stdout.split(b'\n'):
        if b"bad passphrase" in line.lower():
            # print(line)
            return False
    if result.returncode != 0:
        return None
    return True


def test_gpg_agent(email):
    """ If result is None there is no tty and user is using tty or curses
    purpose is to refresh the cached passphrase or see if passphrase has expired """
    result = subprocess.run(["gpg", "--default-key", email, "-s"], input=b"", capture_output=True)
    # for line in result.stdout.decode('utf-8', errors='ignore').split('\n'): # slower
    #     if 'bad passphrase' in line.lower():
    #         return False
    # for line in result.stderr.decode('utf-8', errors='ignore').split('\n'): # slower
    #     if 'ioctl' in line.lower():
    for line in result.stderr.split(b'\n'):
        # print(line.decode('utf-8', errors='ignore'))
        if b"ioctl" in line.lower():
            return None
        if email.encode() not in line:
            if b"bad passphrase" in line.lower():
                return False
            # print(line.decode('utf-8', errors='ignore'))
    for line in result.stdout.split(b'\n'):
        if b"bad passphrase" in line.lower():
            return False
    return result.returncode == 0


def parse_gpg_agent_conf(gnupg_home):

    conf = gnupg_home / "gpg-agent.conf"
    opts = {}
    if not conf.is_file():
        return opts

    for raw in conf.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        try:
            parts = shlex.split(line, comments=True, posix=True)
        except ValueError:
            continue
        if not parts:
            continue

        key = parts[0]
        val = True if len(parts) == 1 else " ".join(parts[1:])
        opts[key] = val
    return opts


# can also delete the key with the subid from fingerprint of the .gpg file
def get_subkey_id(gpg_file):
    result = subprocess.run(
        ["gpg", "--decrypt", "--dry-run", gpg_file],
        capture_output=True,
        text=True,
        stderr=subprocess.STDOUT
    )
    for line in result.stdout.split('\n'):
        if 'encrypted' in line.lower():
            match = re.search(r'ID ([A-F0-9]+)', line)
            if match:
                return match.group(1)

    return None


# checks if the user can decrypt the encrypted file and confirm valid key
def gpg_can_decrypt(dbtarget):
    if not os.path.isfile(dbtarget):
        return True

    cmd = ["gpg", "--decrypt", "--dry-run", dbtarget]
    result = subprocess.run(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    return result.returncode == 0


# prepare for file output
def dict_to_list(cachedata: dict[str, dict[Any, dict[str, Any]]]) -> list[dict[str, Any]]:
    data_to_write = []
    for root, versions in cachedata.items():
        for modified_ep, metadata in versions.items():
            row = {
                "checksum": metadata.get("checksum") or '',
                "size": '' if metadata.get("size") is None else metadata["size"],
                "modified_time": '' if metadata.get("modified_time") is None else metadata["modified_time"],
                "modified_ep": '' if modified_ep is None else modified_ep,
                # "owner": metadata.get("owner"),
                # "domain": metadata.get("domain"),
                "root": root,
            }
            data_to_write.append(row)
    return data_to_write


# prepare for file output
def dict_to_list_sys(cachedata: dict[str, dict[str, Any]]) -> list[dict[str,  Any]]:
    data_to_write = []
    for root, metadata in cachedata.items():
        row = metadata.copy()
        row['root'] = root
        data_to_write.append(row)
    return data_to_write


def dict_string(data: list[dict]) -> str:
    if not data:
        return ""
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys(), delimiter='|', quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(data)
    return output.getvalue()


def encrypt_to_text(notes: str, email: str) -> str | None:

    cmd = ["gpg", "--batch", "--yes", "--armor", "--encrypt", "-r", email]

    res = subprocess.run(
        cmd,
        input=notes.encode("utf-8"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if res.returncode != 0:
        return None  # or raise with res.stderr.decode()
    return res.stdout.decode("utf-8")


def decrypt_from_text(cipher_text: str) -> str | None:
    cmd = ["gpg", "--decrypt"]
    res = subprocess.run(
        cmd,
        input=cipher_text.encode("utf-8"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    if res.returncode != 0:
        stderr_text = res.stderr.decode("utf-8", errors="replace")
        for line in stderr_text.splitlines():
            print(line)
        return None
    return res.stdout.decode("utf-8")
