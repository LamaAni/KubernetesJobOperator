from enum import Enum


class JobRunnerException(Exception):
    pass


class JobRunnerDeletePolicy(Enum):
    Never = "Never"
    Always = "Always"
    IfFailed = "IfFailed"
    IfSucceeded = "IfSucceeded"

    def __str__(self) -> str:
        return self.value


class KubernetesJobOperatorDefaultExecutionResource(Enum):
    Pod = "Pod"
    Job = "Job"
