import random
import string


def get_from_dictionary_path(path_names):
    val = None
    col = job_yaml
    cur_path = []
    for name in path_names:
        cur_path.append(name)
        if isinstance(col, dict):
            assert name in col, "Missing path:"+'.'.join(cur_path)
        if isinstance(col, list):
            assert len(col) > name, "Missing path:"+'.'.join(cur_path)
        else:
            raise Exception(
                "Expected path "+'.'.join(cur_path) +
                " to be a list or a dictionary")
        val = col[name]
    return val


def randomString(stringLength=10):
    """Create a random string

    Keyword Arguments:
        stringLength {int} -- The length of the string (default: {10})

    Returns:
        string -- A random string
    """
    letters = "abcdefghijklmnopqrstvwxyz0123456789"
    return ''.join(random.choice(letters) for i in range(stringLength))
