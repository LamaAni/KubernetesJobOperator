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
    task_id="test-job",
    dag=dag,
    image="ubuntu",
    command=["bash", "-c", 'echo "all ok"'],
)

body = {"kind": "Pod"}  # The body or a yaml string (must be valids)
job_task_from_body = KubernetesJobOperator(dag=dag, task_id="test-job-from-body", body=body)

body_filepath = "./my_yaml_file.yaml"
job_task_from_yaml = KubernetesJobOperator(dag=dag, task_id="test-job-from-yaml", body_filepath=body_filepath)

# Legacy compatibility to KubernetesPodOperator
legacy_job_task = KubernetesLegacyJobOperator(
    task_id="legacy-test-job",
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
  name: test-job
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
  name: test-service
spec:
  selector:
    app: test-task-pod
  ports:
    - port: 8080
      targetPort: 8080
---

```

# Why/When would this be better than the [KubernetesPodOperator](https://github.com/apache/airflow/blob/master/airflow/contrib/operators/kubernetes_pod_operator.p)?

The kubernetes Job allows for more execution options such as retries/timeouts/deadlines/replicas/etc.. which cannot be defined directly on a pod.

Also, the connection between the kubernetes pod and the airflow worker can be lost, due to communication issues,
pod deletions or just pod scheduling problems in the cluster. The Kubernetes Job is a "definition" like resource, and therefore would lose its execution state only if deliberately deleted. A job will also recover automatically from pod manual deletions and pod scheduling errors.

You can find a description of the kubernetes Job resource [here](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/)

# Contribution

Feel free to ping me in issues or directly on LinkedIn to contribute.

# Implementations still missing:

Add an issue (or better submit PR) if you need these.

1. XCom
1. Examples (other than TL;DR)

# Licence

Copyright ©
`Zav Shotan` and other [contributors](https://github.com/LamaAni/postgres-xl-helm/graphs/contributors).
It is free software, released under the MIT licence, and may be redistributed under the terms specified in `LICENSE`.
