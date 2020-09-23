from airflow import AirflowException


class KubernetesJobOperatorException(AirflowException):
    pass
