import os
import json
import kubernetes
import kubernetes.config.kube_config
from airflow_kubernetes_job_operator.kube_api import (
    KubeApiRestClient,
    GetAPIResources,
    GetAPIVersions,
)


client = KubeApiRestClient()
kubernetes.config.kube_config.Configuration.set_default(client.kube_config)

versions = kubernetes.client.CoreApi().get_api_versions()
kubernetes.client.AppsV1Api().patch_namespaced_deployment

rsp = client.query(GetAPIVersions())
rsp = client.query(GetAPIResources())

with open(os.path.join(os.path.dirname(__file__), "api_resources.json"), "w") as writer:
    resources = rsp["resources"]
    writer.write(json.dumps(resources))
