from airflow.utils.decorators import apply_defaults
from airflow.operators import BaseOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


class KubernetesBaseJobOperator(BaseOperator):
    job_yaml = None

    @apply_defaults
    def __init__(self, job_yaml, *args, **kwargs):
        super(BashOperator, self).__init__(*args, **kwargs)

        assert job_yaml is not None and (
            isinstance(job_yaml, (dict, str))
        ), "job_yaml must either be a string yaml or a dict object"

        self.job_yaml = job_yaml
