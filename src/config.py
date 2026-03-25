import json
import tomlkit
from pathlib import Path

# toml

# def load_toml(conf_path):  #tomllib standard library. does not preserve commenting**
#     if not conf_path.is_file():
#         print("Unable to find config file:", conf_path)
#         sys.exit(1)
#     try:
#         with open(conf_path, 'rb') as f:
#             config = tomllib.load(f)
#     except Exception as e:
#         print(f"Failed to parse TOML: {e}")
#         sys.exit(1)
#     return config


def load_toml(conf_path):  # tomlkit
    conf_path = Path(conf_path)
    if not conf_path.is_file():
        print("Unable to find config file:", conf_path)
        return None
    text = conf_path.read_text(encoding="utf-8")
    try:
        config = tomlkit.parse(text)
    except Exception as e:
        print(f"Failed to parse TOML: {e}")
        return None
    return config


# from qt memory
def dump_toml(keyValuePairs, doc, filePath):
    if keyValuePairs:
        for keyName, settings in keyValuePairs.items():
            if keyName in doc:
                for settingName, newValue in settings.items():
                    if newValue is not None:
                        doc[keyName][settingName] = newValue
            else:
                print(f"Key {keyName} not found in the TOML file.")
    with open(filePath, "w", encoding="utf-8") as f:
        f.write(tomlkit.dumps(doc))


def update_toml_values(keyValuePairs, filePath):
    # similar to update_toml_setting but integrated with key pairs
    try:
        with open(filePath, "r", encoding="utf-8") as f:
            doc = tomlkit.parse(f.read())
        dump_toml(keyValuePairs, doc, filePath)
        return True
    except Exception as e:
        print(f"Failed to update toml {filePath} setting. check key value pair {type(e).__name__} {e}")
        return False


def update_toml_setting(keyName, settingName, newValue, filePath):
    # update toml file to disable\enable a setting
    try:
        # config = toml.load(file_path)    removes commenting **      tomblib
        # if keyf in config and stng in config[keyf]:
        #     config[keyf][stng] = False
        #     with open(file_path, 'w') as file:
        #         toml.dump(config, file)

        with open(filePath, "r", encoding="utf-8") as f:
            doc = tomlkit.parse(f.read())

        doc[keyName][settingName] = newValue

        with open(filePath, "w", encoding="utf-8") as f:
            f.write(tomlkit.dumps(doc))
    except Exception as e:
        print(f"Failed to update toml {filePath} setting. check key value pair {type(e).__name__} {e}")
        raise


# end toml


# json mem
def update_dict(updates, data, drive=None):
    if drive:  # Drive-info
        if updates is None:
            data.pop(drive, None)  # Remove entire drive listing
        else:
            if drive not in data or not isinstance(data[drive], dict):
                data[drive] = {}
            target = data[drive]
            for k, v in updates.items():
                if v is None:
                    target.pop(k, None)
                else:
                    target[k] = v
    else:
        # Top-level
        if updates is not None:
            target = data
            for k, v in updates.items():
                if v is None:
                    target.pop(k, None)
                else:
                    target[k] = v


def update_j_settings(updates, data: dict, drive=None, filepath="usrprofile.json"):
    ''' sync memory and json file '''
    update_dict(updates, data, drive)
    dump_j_settings(data, filepath)
# end json mem


def dump_j_settings(data, filepath="usrprofile.json"):
    ''' save json '''
    try:
        with open(filepath, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
    except IOError as e:
        print(f"Error writing to {filepath}: {e}")
    except TypeError as e:
        print("dump_j_settings input data has mixed types while saving", filepath, e)


def set_json_settings(updates=None, drive=None, filepath="usrprofile.json"):
    ''' change a setting in json file '''
    path = Path(filepath)
    if path.is_file():
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError:
            data = {}
    else:
        data = {}

    update_j_settings(updates, data, drive, filepath)
    # path.write_text(json.dumps(data, indent=4))


def get_json_settings(keys=None, drive=None, filepath="usrprofile.json"):
    ''' load json '''
    path = Path(filepath)
    if not path.is_file():
        return {} if keys is None else {k: None for k in keys}

    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        return {} if keys is None else {k: None for k in keys}

    target = data.get(drive, {}) if drive else data

    if keys is None:
        return target

    return {k: target.get(k) for k in keys}
