import os
import random
import re
import inspect

REPO_PATH = os.path.dirname(__file__)


def resolve_path(src: str, basepath: str = REPO_PATH) -> str:
    """resolve path in the current repo.

    Args:
        src (str): The path to resolve.
        basepath (str, optional): The base path to search in. Defaults to REPO_PATH.

    Returns:
        str: The absolute path.
    """
    if src.startswith("."):
        if src.startswith("./"):
            src = os.path.join(basepath, src[2:])
        elif src.startswith("../"):
            src = os.path.join(basepath, src)
    return os.path.abspath(src)


def resolve_relative_path(src: str, caller_offset=0):
    """Resolve a path relative to the caller.

    Args:
        src (str): The path to resolve.
        caller_offset (int, optional): The caller to resolve relative to (the caller file).
            0 means the direct caller.
            1 means the caller of the caller.
            etc..
            Defaults to 0.

    Returns:
        [type]: [description]
    """

    if os.path.isabs(src):
        return os.path.abspath(src)

    caller_offset = caller_offset if caller_offset > -1 else 0
    caller_offset += 1

    stack = inspect.stack()
    frame = stack[caller_offset]
    return resolve_path(src=src, basepath=os.path.dirname(frame.filename))


def random_string(stringLength=10):
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
        second_part = second_part[-max_length + start_trim_offset + 2 :]  # noqa: E203
        name = first_part + "--" + second_part
    return name
