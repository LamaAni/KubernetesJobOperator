from logging import log
import os
from tests.utils import logging

from zthreading.events import Event
from airflow_kubernetes_job_operator.kube_api.client import KubeApiRestClient
from airflow_kubernetes_job_operator.kube_api.watchers import NamespaceWatchQuery

client = KubeApiRestClient()
query = NamespaceWatchQuery()
query.bind_logger()
rslt = client.async_query(query)

logging.info("Starting watch...")
query.join()
