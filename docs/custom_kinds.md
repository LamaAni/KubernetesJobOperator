# Custom kind execution

When executing in kubernetes one sometimes would like to use custom resources, such as custom controllers or other types of kubernetes resources available.

# TL;DR

## Define the resource

```python
from airflow_kubernetes_job_operator.kube_api import KubeApiConfiguration, KubeResourceState, KubeResourceKind

def parse_my_resource_state(body)->KubeResourceState:
    # Resource state parsing here. Must return at least each of the following:
    # - KubeResourceState.Running -> The task was accepted and is now running
    # - KubeResourceState.Failed -> The task failed. The executor should stop.
    # - KubeResourceState.Succeeded -> The task succeeded. The executor should stop.
    raise Exception("Not implemeneted")

# will override existing kinds.
kind: KubeResourceKind = KubeApiConfiguration.register_kind(
    name="MySpecialKind",
    api_version="my-special-api/api-ver-1",
    parse_kind_state=parse_my_resource_state,
)
```

An example for a parse method can be found in [KubeResourceKind.parse_state_job](airflow_kubernetes_job_operator/kube_api/collections)

NOTE! The resource kind must have a resource state parser method so the executor knows when to start and stop.

NOTE! The definition above must be called before the DAG executes. For example, put this definitions in a `utils` file and call it from the DAG.

## Define the task yaml

Use in yaml (`my_custom_task.yaml`):

```yaml
apiVersion: my-special-api/api-ver-1
kind: MySpecialKind
metadata:
  name: test-custom # not required. Will be a prefix to task name
  finalizers:
    - foregroundDeletion
  labels:
    app: test-task-pod
spec:
  mySpecValue: 22
---
apiVersion: v1
kind: Service
metadata:
  name: test-service # not required, will be a prefex to task name.
spec:
  selector:
    app: test-task-pod
  ports:
    - port: 8080
      targetPort: 8080
```

## Use in a DAG

```python
from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_job_operator import KubernetesJobOperator
from airflow.utils.dates import days_ago

default_args = {"owner": "tester", "start_date": days_ago(2), "retries": 0}
dag = DAG("job-tester", default_args=default_args, description="Test base job operator", schedule_interval=None)

job_task_from_yaml = KubernetesJobOperator(dag=dag, task_id="from-yaml", body_filepath="./my_custom_task.yaml")
```
