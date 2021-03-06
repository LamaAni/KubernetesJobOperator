import logging
from typing import Type, Dict
from enum import Enum
from airflow_kubernetes_job_operator.kube_api.utils import not_empty_string
from airflow_kubernetes_job_operator.kube_api.config import DEFAULT_KUBE_CONFIG_LOCATIONS
from airflow_kubernetes_job_operator.kube_api.queries import LogLine
from airflow_kubernetes_job_operator.utils import resolve_path
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow_kubernetes_job_operator.collections import (
    JobRunnerDeletePolicy,
    KubernetesJobOperatorDefaultExecutionResource,
)


DEFAULT_EXECUTION_OBJECT_PATHS: Dict[KubernetesJobOperatorDefaultExecutionResource, str] = {
    KubernetesJobOperatorDefaultExecutionResource.Job: resolve_path("./templates/job_default.yaml"),
    KubernetesJobOperatorDefaultExecutionResource.Pod: resolve_path("./templates/pod_default.yaml"),
}

AIRFLOW_CONFIG_SECTION_NAME = "kubernetes_job_operator"


def get(
    key: str,
    default=None,
    otype: Type = None,
    collection=None,
    allow_empty: bool = False,
):
    otype = otype or str if default is None else default.__class__
    collection = collection or AIRFLOW_CONFIG_SECTION_NAME
    val = None
    try:
        val = conf.get(AIRFLOW_CONFIG_SECTION_NAME, key)
    except AirflowConfigException as ex:
        logging.debug(ex)

    if issubclass(otype, Enum):
        allow_empty = False

    if val is None or (not allow_empty and len(val.strip()) == 0):
        assert default is not None, f"Airflow configuration {collection}.{key} not found, and no default value"
        return default

    if otype == bool:
        return val.lower() == "true"

    elif issubclass(otype, Enum):
        val = val.strip()
        return otype(val.strip())
    else:
        return otype(val)


# ------------------------------
# Airflow config values

# Job runner
DEFAULT_DELETE_POLICY: JobRunnerDeletePolicy = get("delete_policy", JobRunnerDeletePolicy.IfSucceeded)

# Default bodies
DEFAULT_EXECTION_OBJECT: KubernetesJobOperatorDefaultExecutionResource = get(
    "default_execution_object", KubernetesJobOperatorDefaultExecutionResource.Job
)
DEFAULT_KUBERNETES_MAX_RESOURCE_NAME_LENGTH = get("max_job_name_length", 50)

# api config
LogLine.detect_kubernetes_log_level = get("detect_kubernetes_log_level", True)
LogLine.show_kubernetes_log_timestamps = get("show_kubernetes_timestamps", False)

# task config
DEFAULT_TASK_STARTUP_TIMEOUT: int = get("startup_timeout_seconds", 120)
DEFAULT_VALIDATE_BODY_ON_INIT: bool = get("validate_body_on_init", False)

# Runner config.
SHOW_RUNNER_ID_IN_LOGS: bool = get("show_runner_id", False)

# Client config
KUBE_CONFIG_EXTRA_LOCATIONS: str = get("kube_config_extra_locations", "", otype=str, allow_empty=True)
if not_empty_string(KUBE_CONFIG_EXTRA_LOCATIONS):
    for loc in KUBE_CONFIG_EXTRA_LOCATIONS.split(",").reverse():
        log = loc.strip()
        if len(loc) == 0:
            continue
        DEFAULT_KUBE_CONFIG_LOCATIONS.insert(0, loc)
