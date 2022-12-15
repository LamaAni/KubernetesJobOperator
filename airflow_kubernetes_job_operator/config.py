import logging
from typing import Type, Dict
from enum import Enum
from airflow_kubernetes_job_operator.kube_api import config as kube_api_config
from airflow_kubernetes_job_operator.kube_api.utils import not_empty_string
from airflow_kubernetes_job_operator.kube_api.queries import LogLine, GetPodLogs
from airflow_kubernetes_job_operator.utils import resolve_path
from airflow.version import version
from airflow.configuration import conf, log
from airflow.exceptions import AirflowConfigException
from airflow_kubernetes_job_operator.collections import (
    JobRunnerDeletePolicy,
    KubernetesJobOperatorDefaultExecutionResource,
)
import re


DEFAULT_EXECUTION_OBJECT_PATHS: Dict[KubernetesJobOperatorDefaultExecutionResource, str] = {
    KubernetesJobOperatorDefaultExecutionResource.Job: resolve_path("./templates/job_default.yaml"),
    KubernetesJobOperatorDefaultExecutionResource.Pod: resolve_path("./templates/pod_default.yaml"),
}

AIRFLOW_CONFIG_SECTION_NAME = "kubernetes_job_operator"

AIRFLOW_VERSION = [int(re.sub(r"\D", "", v)) for v in version.split(".")]
AIRFLOW_MAJOR_VERSION = AIRFLOW_VERSION[0]
AIRFLOW_MINOR_VERSION = AIRFLOW_VERSION[1]
AIRFLOW_PATCH_VERSION = AIRFLOW_VERSION[2]
IS_AIRFLOW_ONE = AIRFLOW_MAJOR_VERSION < 2


def conf_get_no_warnings_no_errors(*args, **kwargs):
    old_level = log.level
    try:
        # must be suppressed manually since the config
        # always prints (rather then raise a warning)
        log.level = logging.ERROR
        val = conf.get(*args, **kwargs)
    except AirflowConfigException:
        val = None
    finally:
        log.level = old_level
    return val


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
        val = conf_get_no_warnings_no_errors(AIRFLOW_CONFIG_SECTION_NAME, key)
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
        loc = loc.strip()
        if len(loc) == 0:
            continue
        kube_api_config.DEFAULT_KUBE_CONFIG_LOCATIONS.insert(0, loc)

GetPodLogs.enable_kube_api_events = get(
    "log_query_enable_api_events",
    default=GetPodLogs.enable_kube_api_events,
    otype=bool,
)

GetPodLogs.api_event_match_regexp = get(
    "log_query_api_event_match_regexp",
    default=GetPodLogs.api_event_match_regexp,
    otype=str,
)

GetPodLogs.emit_api_events_as_log = get(
    "log_query_emit_api_events_as_log",
    default=GetPodLogs.emit_api_events_as_log,
    otype=bool,
)
