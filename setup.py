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

import os
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))


def get_version():
    version_file_path = os.path.join(here, "package_version.txt")
    if not os.path.isfile(version_file_path):
        return "debug"
    version = None
    with open(version_file_path, "r") as raw:
        version = raw.read()

    return version


setup(
    name="airflow_kubernetes_job_operator",
    version=get_version(),
    description=(
        "An airflow job operator that executes a task as a Kubernetes job on a cluster, "
        "given a job yaml configuration or an image uri."
    ),
    long_description="Please see readme.md @ https://github.com/LamaAni/KubernetesJobOperator",
    classifiers=[],
    author="Zav Shotan",
    author_email="",
    url="https://github.com/LamaAni/KubernetesJobOperator",
    packages=["airflow_kubernetes_job_operator", "airflow_kubernetes_job_operator/kube_api"],
    platforms="any",
    license="LICENSE",
    install_requires=[
        "PyYAML>=5.0",
        "kubernetes>=8.0.1",
        "urllib3>=1.25.0",
        "zthreading>=0.1.13",
        "python-dateutil>=2.8.1",
    ],
    python_requires=">=3.6",
    include_package_data=True,
)
