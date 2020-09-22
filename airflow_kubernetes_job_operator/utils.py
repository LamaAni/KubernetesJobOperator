import os
import random
import re

REPO_PATH = os.path.dirname(__file__)


def repo_reslove(src: str):
    if src.startswith("."):
        if src.startswith("./"):
            src = os.path.join(REPO_PATH, src[2:])
        elif src.startswith("../"):
            src = os.path.join(REPO_PATH, src)
    return os.path.abspath(src)


def get_dict_path_value(yaml: dict, path_names):
    value = yaml
    cur_path = []
    for name in path_names:
        cur_path.append(name)
        path_string = ".".join(map(lambda v: str(v), path_names))

        if isinstance(value, dict):
            assert name in value, "Missing path:" + path_string
        elif isinstance(value, list):
            assert len(value) > name, "Missing path:" + path_string
        else:
            raise Exception("Expected path " + path_string + " to be a list or a dictionary")

        value = value[name]
    return value


def set_dict_path_value(yaml: dict, path_names: list, value, if_not_exists=False):
    name_to_set = path_names[-1]
    col = get_dict_path_value(yaml, path_names[:-1])

    if isinstance(col, list):
        assert isinstance(name_to_set, int), "To set a list value you must have an integer key."
        if name_to_set > -1 and len(col) > name_to_set:
            if if_not_exists:
                return
            col[name_to_set] = value
        else:
            col.append(value)
    else:
        if if_not_exists and name_to_set in col:
            return
        col[name_to_set] = value


def randomString(stringLength=10):
    """Create a random string

    Keyword Arguments:
        stringLength {int} -- The length of the string (default: {10})

    Returns:
        string -- A random string
    """
    letters = "abcdefghijklmnopqrstvwxyz0123456789"
    return "".join(random.choice(letters) for i in range(stringLength))


def to_kubernetes_valid_name(name, max_length=50, start_trim_offset=10):
    """Returns a kubernetes valid name, and truncates, after a start
    offset, any exccess chars.

    Arguments:
        name {[type]} -- [description]

    Keyword Arguments:
        max_length {int} -- [description] (default: {50})
        start_trim_offset {int} -- [description] (default: {10})

    Returns:
        [type] -- [description]
    """
    assert start_trim_offset < max_length, "start_trim_offset must be smaller then max_length"
    name = re.sub(r"[^a-z0-9]", "-", name.lower())

    if len(name) > max_length:
        first_part = name[0:start_trim_offset] if start_trim_offset > 0 else ""
        second_part = name[start_trim_offset:]
        second_part = second_part[-max_length + start_trim_offset + 2 :]
        name = first_part + "--" + second_part
    return name
