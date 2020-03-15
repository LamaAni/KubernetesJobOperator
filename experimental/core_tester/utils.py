import logging
import os

logging.basicConfig(level="INFO")

CUR_DIRECTORY = os.path.abspath(os.path.dirname(__file__))


def load_raw_formatted_file(fpath):
    text = ""
    with open(fpath, "r", encoding="utf-8") as filedata:
        text = filedata.read()
    return text

