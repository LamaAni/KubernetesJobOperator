# Airflow KubernetesJobOperator

An airflow job operator that executes a task as a Kubernetes job on a cluster, given
a job yaml configuration or an image uri.

Two operators are available:

1. KubernetesJobOperator - The main kubernetes job operator.
1. KubernetesLegacyJobOperator - A operator that accepts the same parameters as
   the KubernetesPodOperator and allows seamless integration between old and new.

Still missing (on both):

1. XCom

# ALPHA

This repository is in alpha testing, and is published to allow contribution.

# TL;DR

```python
from airflow import DAG
from src.kubernetes_job_operator import KubernetesJobOperator
from airflow.utils.dates import days_ago

default_args = {"owner": "tester", "start_date": days_ago(2), "retries": 0}
dag = DAG("bjo", default_args=default_args, description="Test base job operator", schedule_interval=None)

job_yaml=... # loaded from file.

job_task = KubernetesJobOperator(task_id="test-job", job_yaml=job_yaml, dag=dag)

# when using image and commands notation
legacy_job_task = KubernetesLegacyJobOperator(
    task_id="legacy-test-job",
    image="ubuntu",
    cmds=["bash", "-c", 'echo "all ok"'],
    dag=dag,
    is_delete_operator_pod=True,
)
```

And the job yaml:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: test-job
  finalizers:
    - foregroundDeletion
spec:
  template:
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
```

# Why not use the [kubernetes pod operator](https://github.com/apache/airflow/blob/master/airflow/contrib/operators/kubernetes_pod_operator.p)?

The kubernetes Job allows for more execution options such as retries/timeouts/deadlines/replicas/etc.. which cannot be defined directly on a pod.

Also, the connection between the pod and the worker can be lost, due to communication issues,
pod deletions or just pod scheduling issues in the cluster. The Kubernetes Job is more like a "definition" of pending execution, we therefore would lose the state of the job only if the job is deliberately deleted. A job will also recover automatically from pod manual deletions and pod scheduling errors.

You can find a description of the kubernetes Job resource [here](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/)

## Open issues needed for beta release

1. Correction of the kuberntes watcher to allow restart of the watch process
   if connection is lost.

# Contribution

Feel free to ping me in issues or directly on LinkedIn to contribute.

# Licence

Copyright ©
`Zav Shotan` and other [contributors](https://github.com/LamaAni/postgres-xl-helm/graphs/contributors).
It is free software, released under the MIT licence, and may be redistributed under the terms specified in `LICENSE`.
