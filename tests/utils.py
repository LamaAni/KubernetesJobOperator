import sys
import logging
import os


def get_is_no_color():
    val = os.environ.get("NO_COLOR", "--no-color" in sys.argv)
    if not isinstance(val, bool):
        val = val.strip().lower()
        os.environ["NO_COLOR"] = val
    return val


def colorize(val, color, add_reset=True):
    if get_is_no_color():
        return val
    return color + val + ("\033[0m" if add_reset else "")


class style:
    GRAY = lambda x: colorize(str(x), "\033[90m")  # noqa: E731
    LIGHT_GRAY = lambda x: colorize(str(x), "\033[37m")  # noqa: E731
    BLACK = lambda x: colorize(str(x), "\033[30m")  # noqa: E731
    RED = lambda x: colorize(str(x), "\033[31m")  # noqa: E731
    GREEN = lambda x: colorize(str(x), "\033[32m")  # noqa: E731
    YELLOW = lambda x: colorize(str(x), "\033[33m")  # noqa: E731
    BLUE = lambda x: colorize(str(x), "\033[34m")  # noqa: E731
    MAGENTA = lambda x: colorize(str(x), "\033[35m")  # noqa: E731
    CYAN = lambda x: colorize(str(x), "\033[36m")  # noqa: E731
    WHITE = lambda x: colorize(str(x), "\033[97m")  # noqa: E731
    UNDERLINE = lambda x: colorize(str(x), "\033[4m")  # noqa: E731
    RESET = lambda x: colorize(str(x), "\033[0m")  # noqa: E731


logging.basicConfig(level="INFO", format=style.GRAY("[%(asctime)s][%(levelname)7s]") + " %(message)s")

CUR_DIRECTORY = os.path.abspath(os.path.dirname(__file__))


def load_raw_formatted_file(fpath):
    text = ""
    with open(fpath, "r", encoding="utf-8") as filedata:
        text = filedata.read()
    return text
