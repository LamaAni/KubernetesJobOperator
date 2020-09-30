import sys
import logging
import os
import yaml
import inspect
from airflow_kubernetes_job_operator.kube_api import KubeApiConfiguration
from airflow_kubernetes_job_operator.utils import resolve_path


CUR_DIRECTORY = os.path.abspath(os.path.dirname(__file__))


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


def load_yaml_objects(fpath):
    rslt = None
    with open(resolve_path(fpath, basepath=os.path.dirname(__file__)), "r") as raw:
        yaml_string = raw.read()
        rslt = yaml.safe_load_all(yaml_string)
    return list(rslt)


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


def load_raw_formatted_file(fpath):
    text = ""
    with open(fpath, "r", encoding="utf-8") as filedata:
        text = filedata.read()
    return text


def load_default_kube_config():
    try:
        config = KubeApiConfiguration.load_kubernetes_configuration()
        assert config is not None

        print_version = str(sys.version).replace("\n", " ")
        logging.info(
            f"""
        -----------------------------------------------------------------------
        Context: {KubeApiConfiguration.get_active_context_info(config)}
        home directory: {os.path.expanduser('~')}
        Config host: {config.host} 
        Config filepath: {config.filepath}
        Default namespace: {KubeApiConfiguration.get_default_namespace(config)}
        Executing dags in python version: {print_version}
        -----------------------------------------------------------------------
        """
        )
    except Exception as ex:
        logging.error(
            """
    -----------------------------------------------------------------------
    Failed to retrive config, kube config could not be loaded.
    ----------------------------------------------------------------------
    """,
            ex,
        )


def resolve_file(fpath: str, offset=1):
    offset = offset if offset > -1 else 0

    frame = inspect.stack()[offset]
    cur_path = os.path.dirname(frame.filename)
    
    if fpath.startswith("."):
        if fpath.startswith("./"):
            fpath = os.path.join(cur_path, fpath[2:])
        else:
            fpath = os.path.join(cur_path, fpath)
    return os.path.abspath(fpath)


logging.basicConfig(level="INFO", format=style.GRAY("[%(asctime)s][%(levelname)7s]") + " %(message)s")
print_version = str(sys.version).replace("\n", " ")
logging.info(f"Running in python version: {print_version}")
