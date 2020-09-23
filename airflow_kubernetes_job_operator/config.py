from typing import Type, Dict
from enum import Enum
from airflow_kubernetes_job_operator.utils import repo_reslove
from airflow.configuration import conf
from airflow_kubernetes_job_operator.collections import (
    JobRunnerDeletePolicy,
    KubernetesJobOperatorDefaultExecutionObject,
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
DEFAULT_EXECTION_OBJECT: KubernetesJobOperatorDefaultExecutionObject = get_enum(
    "default_execution_object", KubernetesJobOperatorDefaultExecutionObject.Job
)
DEFAULT_EXECUTION_OBJECT_PATHS: Dict[KubernetesJobOperatorDefaultExecutionObject, str] = {
    KubernetesJobOperatorDefaultExecutionObject.Job: repo_reslove("./templates/job_default.yaml"),
    KubernetesJobOperatorDefaultExecutionObject.Pod: repo_reslove("./templates/pod_default.yaml"),
}

# task config
DEFAULT_TASK_STARTUP_TIMEOUT = get("startup_timeout_seconds", 120, otype=int)
DEFAULT_VALIDATE_BODY_ON_INIT = get("validate_body_on_init", False, otype=bool)
