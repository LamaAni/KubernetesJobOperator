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

setup(
    name="airflow_kubernetes_job_operator",
    version="0.2.7",
    description="An airflow job operator that executes a task as a Kubernetes job on a cluster, given a job yaml configuration or an image uri.",
    long_description="Please see readme.md",
    classifiers=[],
    author="Zav Shotan",
    author_email="",
    url="https://github.com/LamaAni/KubernetesJobOperator",
    packages=["airflow_kubernetes_job_operator"],
    platforms="any",
    license="LICENSE",
    install_requires=["PyYAML>=5.0", "kubernetes>=9.0.0", "urllib3>=1.25.0"],
    python_requires=">=3.6",
    include_package_data=True,
)
