#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    setup.py
    ~~~~~~~~

    An airflow job operator that executes a task as a Kubernetes job on a cluster,
given a job yaml configuration or an image uri.

    :copyright: (c) 2020 by zav.
    :license: see LICENSE for more details.
"""

import codecs
import os
import re
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    """Taken from pypa pip setup.py:
    intentionally *not* adding an encoding option to open, See:
       https://github.com/pypa/virtualenv/issues/201#issuecomment-3145690
    """
    return codecs.open(os.path.join(here, *partss), "r").read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name="airflow_kubernetes_job_operator",
    version=find_version("airflow_kubernetes_job_operator", "__init__.py"),
    description="An airflow job operator that executes a task as a Kubernetes job on a cluster, given a job yaml configuration or an image uri.",
    long_description=read("README.md"),
    classifiers=[],
    author="Zav Shotan",
    author_email="",
    url="https://github.com/LamaAni/KubernetesJobOperator",
    packages=["airflow_kubernetes_job_operator"],
    platforms="any",
    license="LICENSE",
    install_requires=["apache-airflow>=1.10.3",],
)
