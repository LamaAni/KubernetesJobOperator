import os
import re

default_args = {"owner": "tester", "start_date": "1/1/2020", "retries": 0}


def name_from_file(fpath):
    return re.sub(r"[^a-zA-Z0-9-]", "-", os.path.splitext(os.path.basename(fpath))[0])
