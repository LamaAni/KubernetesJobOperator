from typing import Type, Dict
from enum import Enum
from airflow_kubernetes_job_operator.utils import resolve_path
from airflow.configuration import conf
from airflow_kubernetes_job_operator.collections import (
    JobRunnerDeletePolicy,
    KubernetesJobOperatorDefaultExecutionResource,
)

airflow_section_name = "kube_job_operator"


def get(key: str, default=None, collection=None, otype: Type = None) -> str:
    otype = otype or str
    collection = collection or airflow_section_name
    return otype(conf.get(airflow_section_name, key, fallback=default))


def get_enum(key: str, default: Enum, collection=None):
    collection = collection or airflow_section_name
    type_class: Type[Enum] = default.__class__
    val: str = get(key, "", collection=collection)
    try:
        return default if val == "" else type_class(val)  # type:ignore
    except Exception:
        raise Exception(
            f"Invalid ariflow configuration {collection or airflow_section_name}.{key}: {val}. "
            + f" Accepted values: {[str(v) for v in type_class]}"
        )


# Job runner
DEFAULT_DELETE_POLICY: JobRunnerDeletePolicy = get_enum("delete_policy", JobRunnerDeletePolicy.IfSucceeded)

# Default bodies
DEFAULT_EXECTION_OBJECT: KubernetesJobOperatorDefaultExecutionResource = get_enum(
    "default_execution_object", KubernetesJobOperatorDefaultExecutionResource.Job
)
DEFAULT_EXECUTION_OBJECT_PATHS: Dict[KubernetesJobOperatorDefaultExecutionResource, str] = {
    KubernetesJobOperatorDefaultExecutionResource.Job: resolve_path("./templates/job_default.yaml"),
    KubernetesJobOperatorDefaultExecutionResource.Pod: resolve_path("./templates/pod_default.yaml"),
}

# task config
DEFAULT_TASK_STARTUP_TIMEOUT = get("startup_timeout_seconds", 120, otype=int)
DEFAULT_VALIDATE_BODY_ON_INIT = get("validate_body_on_init", False, otype=bool)
