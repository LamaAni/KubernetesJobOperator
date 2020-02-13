import random
import re


def get_from_dictionary_path(dictionary, path_names):
    val = None
    col = dictionary
    cur_path = []
    for name in path_names:
        cur_path.append(name)
        path_string = ".".join(map(lambda v: str(v), path_names))
        if isinstance(col, dict):
            assert name in col, "Missing path:" + path_string
        elif isinstance(col, list):
            assert len(col) > name, "Missing path:" + path_string
        else:
            raise Exception("Expected path " + path_string + " to be a list or a dictionary")
        val = col[name]
        col = val
    return val


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
