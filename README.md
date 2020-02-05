# Python Airflow KubernetesJobOperator

An implementation of a job operator that allows running a task as a kubernetes job.
Will forward all job logs from the job pods to the airflow log.

# ALPHA
This repository is in alpha, and should not be used
for production. 

# Why to use

Even though a single pod approach (As the one taken in the [kubernetes pod operator](https://github.com/apache/airflow/blob/master/airflow/contrib/operators/kubernetes_pod_operator.p))
will produce a task that would run in a pod, the kubernetes Job object allows 
other definitions such as retries/timeouts/etc.. that cannot be applied to a pod.
Also, multiple executions of the same thing, i.e. multiple/parallel tasks are not supported.

Finally, the connection between the pod and the worker can be lost, due to communication issues,
pod death or just pod scheduling issues while for the a Job, which behaves as a definition in kubernetes,
we would lose the state of the job only if the job is deliberately deleted.

# Contribution
Feel free to ping me in issues or directly on linkedin to contribute.

# Licence

Copyright ©
`Zav Shotan` and other [contributors](https://github.com/LamaAni/postgres-xl-helm/graphs/contributors).
It is free software, released under the MIT licence, and may be redistributed under the terms specified in `LICENSE`.
