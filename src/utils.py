import random
import string


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
            raise Exception(
                "Expected path " + path_string + " to be a list or a dictionary"
            )
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
