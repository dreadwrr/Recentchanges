import getpass
import glob
import os
import subprocess
import tempfile
import traceback
from typing import Any
from .config import get_json_settings
from .config import set_json_settings
from .rntchangesfunctions import name_of
from .rntchangesfunctions import removefile


def iskey(email):
    try:
        result = subprocess.run(
            ["gpg", "--list-secret-keys"],
            capture_output=True,
            text=True,
            check=True
        )

        return (email in result.stdout), result.stdout
    except FileNotFoundError as e:
        err = f"File not found gpg not in path {e}"
        return None, err
    except subprocess.CalledProcessError as e:
        err = f"Error running gpg: {e}, type: {type(e).__name__} \n {e.stderr}"
        return None, err


# setup keypair called by set_recent_helper script
def import_key(argv):
    if len(argv) < 2:
        print("import_key <keyfile> <email>")
        return 1
    keyfile = argv[0]
    email = argv[1]
    if not os.path.isfile(keyfile):
        print("import_key Missing keyfile: ", keyfile)
        return 1

    passphrase = None
    if "--passphrase-fd" in argv:
        idx = argv.index("--passphrase-fd")
        if idx + 1 >= len(argv):
            print("import_key Missing value for --passphrase-fd")
            return 1
        try:
            fd = int(argv[idx + 1])
        except ValueError:
            print("import_key Invalid --passphrase-fd value:", argv[idx + 1])
            return 1

        print("reading from file descriptor: ", fd)
        try:
            with os.fdopen(fd, "rb", closefd=False) as fd_reader:
                passphrase = fd_reader.read().rstrip(b"\r\n")
        except OSError as e:
            print(f"import_key Failed to read fd {fd}: {e}")
            return 1

    if not passphrase:
        print("import_key No passphrase")
        return 1

    try:
        subprocess.run(
            [
                "gpg",
                "--batch",
                "--yes",
                "--pinentry-mode", "loopback",
                "--passphrase-fd", "0",
                "--import",
                str(keyfile),
            ],
            input=passphrase,
            check=True
        )  # works not as secure as passing passphrase in commandline. conversly putting the passphrase to a file although safer is not ideal
        # with open(ftarget, "rb") as keyfile:
        # subprocess.run(
        #     [
        #         "gpg",
        #         "--batch",
        #         "--yes",
        #         "--pinentry-mode", "loopback",
        #         "--passphrase", passphrase,
        #         "--import",
        #     ],
        #     stdin=keyfile,
        #     check=True
        # )
        input_data = "trust\n5\ny\nquit\n"

        result = subprocess.run(["gpg", "--command-fd", "0", "--edit-key", email], input=input_data, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            print("failed to import", keyfile, " GPG failed:", result.stderr)
        return 0

    except subprocess.CalledProcessError as e:
        print(f"import_key failed to import from keyfile {keyfile} return_code: {e.returncode} err: {e}")
        combined = "\n".join(filter(None, [
            e.stdout.decode(errors="replace") if e.stdout else "",
            e.stderr.decode(errors="replace") if e.stderr else "",
        ]))
        if combined:
            print("[GPG OUTPUT]\n" + combined)
        return 1


def check_for_gpg():
    try:
        result = subprocess.run(
            ["gpg", "--list-keys"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def set_gpg(appdata_local, sub_dir='gpg'):
    gpg_local = appdata_local / sub_dir
    gpg_default = gpg_local / "gpg.exe"
    gnupg_home = gpg_local / "gnupghome"

    os.environ["PATH"] = str(gpg_local) + ";" + os.environ["PATH"]
    os.environ["GNUPGHOME"] = str(gnupg_home)
    # print(subprocess.run(["gpgconf", "--list-dirs", "homedir"], text=True, capture_output=True).stdout)
    return gpg_default, gnupg_home


def find_gnupg_home(json_file, j_settings=None):
    """ try to find gnupg home for exclusion purposes in build index """
    gnupg_home = None
    gpg_home = os.environ.get("GNUPGHOME")
    try:
        if gpg_home:
            gnupg_home = gpg_home
        else:
            if j_settings:
                gnupg_home = j_settings.get("gnupghome")
            else:
                setting = get_json_settings(["gnupghome"], filepath=json_file)
                gnupg_home = setting.get("gnupghome")
            if not gnupg_home:
                result = subprocess.run(["gpgconf", "--list-dirs", "homedir"], capture_output=True, text=True)
                if result.returncode == 0:
                    gpg_home = result.stdout.strip()
                    if gpg_home:
                        gnupg_home = gpg_home
                        set_json_settings({"gnupghome": gpg_home}, None, filepath=json_file)
        return gnupg_home
    except OSError as e:
        print(f"Couldnt get gnupg_home for exclusion file: {json_file} {type(e).__name__} err: {e}")
        return None


# setup keypair for user
def genkey(appdata_local, USR, email, name, dbtarget, CACHE_F, CACHE_S, flth, TEMPD, passphrase=None):

    if not passphrase:
        passphrase = getpass.getpass("Enter passphrase for new GPG key: ")
    p = passphrase
    if not p:
        return False

    param_lines = [
        "%echo Generating a GPG key",
        "Key-Type: RSA",
        "Key-Length: 4096",
        "Subkey-Type: RSA",
        "Subkey-Length: 4096",
        f"Name-Real: {name}",
        f"Name-Email: {email}",
        "Expire-Date: 0",
        # Passphrase: {p},
        "%commit",
        "%echo done",
    ]
    params = "\n".join(param_lines) + "\n"
    with tempfile.TemporaryDirectory(dir=TEMPD) as kp:

        ftarget = os.path.join(kp, 'keyparams.conf')

        try:

            with open(ftarget, "w", encoding="utf-8") as f:
                f.write(params)

            cmd = [
                "gpg",
                "--batch",
                "--pinentry-mode", "loopback",
                "--passphrase-fd", "0",
                "--generate-key",
                ftarget
            ]
            subprocess.run(
                cmd,
                input=(p + "\n").encode(),
                check=True
            )
            # Open the params file and pass it as stdin
            # with open(ftarget, "rb") as param_file:
            #     subprocess.run(
            #         cmd, check=True, stdin=param_file)

        except subprocess.CalledProcessError as e:
            print(f"Failed to generate GPG key: {e}")
            if e.stderr:
                print(e.stderr.decode(errors="replace"))
            return False
        except Exception as e:
            print(f'Unable to make GPG key: {type(e).__name__} {e} {traceback.format_exc()}')
            return False
        finally:
            removefile(ftarget)
    clear_gpg(dbtarget, CACHE_F, CACHE_S, flth)
    print(f"GPG key generated for {email}.")
    return True


# required for batch deleting keys
def get_key_fingerprint(email):
    cmd = ["gpg", "--list-keys", "--with-colons", email]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True
    )
    for line in result.stdout.split('\n'):
        if line.startswith('fpr:'):
            return line.split(':')[9]
    return None


def clear_gpg(dbtarget, CACHE_F, CACHE_S, flth):
    """ delete ctimecache & db .gpg & profile .gpgs """
    systimeche = name_of(CACHE_S)
    dbopt = name_of(dbtarget, '.db')
    file_path = os.path.dirname(CACHE_S)
    pattern = os.path.join(file_path, f"{systimeche}*")
    for r in (CACHE_F, dbopt, dbtarget, flth, *glob.glob(pattern)):
        # p = Path(r)
        try:
            removefile(r)
        except subprocess.CalledProcessError as e:
            print(f"Error clearing {r}: {e}")
        except FileNotFoundError:
            pass


def delete_gpg_keys(usr, email, dbtarget, CACHE_F, CACHE_S, flth):

    # def instruct_out():
    #     print("To trust a gpg key")
    #     print(f"gpg --edit-key {email}")
    #     print("trust")
    #     print("5")
    #     print("y")
    #     print("quit")

    def exec_delete_keys(email, fingerprint):
        silent: dict[str, Any] = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}

        subprocess.run(["gpg", "--batch", "--yes", "--delete-secret-keys", fingerprint], **silent)
        subprocess.run(["gpg", "--batch", "--yes", "--delete-keys", fingerprint], **silent)
        print("Keys cleared for", email, " fingerprint: ", fingerprint)

    while True:

        uinp = input(f"Warning recent.gpg will be cleared. Reset\\delete gpg keys for {email} (Y/N): ").strip().lower()
        if uinp == 'y':
            confirm = input("Are you sure? (Y/N): ").strip().lower()
            if confirm == 'y':

                result = False

                # look for key
                fingerprint = get_key_fingerprint(email)
                if fingerprint:
                    result = True
                    exec_delete_keys(email, fingerprint)

                clear_gpg(dbtarget, CACHE_F, CACHE_S, flth)
                if result:
                    # print(f"\nDelete {dbtarget} if it exists as it uses the old key pair.")
                    return 0
                else:
                    print(f"No key found for {email}")
                    return 2

            else:
                uinp = 'n'

        if uinp == 'n':
            # instruct_out()
            return 1
        else:
            print("Invalid input, please enter 'Y' or 'N'.")
