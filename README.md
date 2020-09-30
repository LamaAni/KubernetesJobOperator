# Airflow KubernetesJobOperator

An airflow operator that executes a task in a kubernetes cluster, given a yaml configuration or an image url.

### BETA

This repository is in beta testing. Contributions are welcome.

### Supports

1. Running tasks as Kubernetes Jobs.
1. Running tasks as Kubernetes Pods.
1. Running tasks as multi resources schemas (multiple resources) as a task (similar to `kubectl apply`).
1. Running tasks as [custom resources](docs/custom_kinds.md).
1. Auto detection of kubernetes namespace and config.
1. Pod and participating resources logs -> airflow.
1. Full kubernetes error logs on failure.
1. Never/Always/OnFailure/OnSuccess kubernetes resources delete policy.

### Two operator classes are available:

1. KubernetesJobOperator - Supply a kubernetes configuration (yaml file, yaml string or a list of python dictionaries) as the body of the task.
1. KubernetesLegacyJobOperator - Defaults to a kubernetes job definition, and supports the same arguments as the KubernetesPodOperator. i.e. replace with the KubernetesPodOperator for legacy support.

# Install

To install using pip @ https://pypi.org/project/airflow-kubernetes-job-operator,

```shell
pip install airflow_kubernetes_job_operator
```

To install from master branch,

```shell
pip install git+https://github.com/LamaAni/KubernetesJobOperator.git@master
```

To install from a release (tag)

```shell
pip install git+https://github.com/LamaAni/KubernetesJobOperator.git@[tag]
```

# TL;DR

Example airflow DAG,

```python
from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_job_operator import KubernetesJobOperator
from airflow_kubernetes_job_operator.kubernetes_legacy_job_operator import KubernetesLegacyJobOperator
from airflow.utils.dates import days_ago

default_args = {"owner": "tester", "start_date": days_ago(2), "retries": 0}
dag = DAG("job-tester", default_args=default_args, description="Test base job operator", schedule_interval=None)

job_task = KubernetesJobOperator(
    task_id="from-image",
    dag=dag,
    image="ubuntu",
    command=["bash", "-c", 'echo "all ok"'],
)

body = {"kind": "Pod"}  # The body or a yaml string (must be valid)
job_task_from_body = KubernetesJobOperator(dag=dag, task_id="from-body", body=body)

body_filepath = "./my_yaml_file.yaml" # Can be relative to this file, or abs path.
job_task_from_yaml = KubernetesJobOperator(dag=dag, task_id="from-yaml", body_filepath=body_filepath)

# Legacy compatibility to KubernetesPodOperator
legacy_job_task = KubernetesLegacyJobOperator(
    task_id="legacy-image-job",
    image="ubuntu",
    cmds=["bash", "-c", 'echo "all ok"'],
    dag=dag,
    is_delete_operator_pod=True,
)
```

Example (multi resource) task yaml:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: test-job # not required. Will be a prefix to task name
  finalizers:
    - foregroundDeletion
spec:
  template:
    metadata:
      labels:
        app: test-task-pod
    spec:
      restartPolicy: Never
      containers:
        - name: job-executor
          image: ubuntu
          command:
            - bash
            - -c
            - |
              #/usr/bin/env bash
              echo "OK"
  backoffLimit: 0
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

# Configuration

Airflow config extra sections,

```ini
[kubernetes_job_operator]
# The task kube resources delete policy. Can be: Never, Always, IfFailed, IfSucceeded
delete_policy=IfSucceeded
# The default object type to execute with (legacy, or image). Can be: Pod, Job
default_execution_object=Job

# Logs
detect_kubernetes_log_level=True
show_kubernetes_timestamps=False
# Shows the runner id in the log (for all runner logs.)
show_runner_id=False

# Tasks (Defaults)
# Wait to first connect to kubernetes.
startup_timeout_seconds=120
# if true, will parse the body when building the dag. Otherwise only while executing.
validate_body_on_init=False

# Comma seperated list of where to look for the kube config file. Will be added to the top
# of the search list, in order.
kube_config_extra_locations=

```

To set these values through the environment follow airflow standards,

```
export AIRFLOW__[section]__[item] = value
```

# Why would this be better than the [KubernetesPodOperator](https://github.com/apache/airflow/blob/master/airflow/contrib/operators/kubernetes_pod_operator.p)?

The KubernetesJobOperator allows for more execution options thank the KubernetesPodOperator such as multiple resource execution, custom resource executions, creation event and pod log tracking, proper resource deletion after task is complete, on error and when task is cancelled and more.

Further, in the KubernetesPodOperator the monitoring between the worker pod and airflow is done by an internal loop which executes and consumes worker resources. In this operator, an async threaded approach was taken which reduces resource consumption on the worker. Further, since the logging and parsing of data is done outside of the main worker thread the worker is "free" do handle other tasks without interruption.

Finally, using the KubernetesJobOperator you are free to use other resources like the kubernetes [Job](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/) to execute your tasks. These are better designed for use in kubernetes and are "up with the times".

# Contribution

Are welcome, please post issues or PR's if needed.

# Implementations still missing:

Add an issue (or better submit PR) if you need these.

1. XCom
1. Examples (other than TL;DR)

# Licence

Copyright ©
`Zav Shotan` and other [contributors](https://github.com/LamaAni/postgres-xl-helm/graphs/contributors).
It is free software, released under the MIT licence, and may be redistributed under the terms specified in `LICENSE`.
