import kubernetes
from airflow_kubernetes_job_operator.kube_api.exceptions import KubeApiException


def clean_dictionary_nulls(d: dict):
    if d is None:
        return {}
    for k in list(d.keys()):
        if d[k] is None:
            del d[k]

    return d


def unqiue_with_order(lst):
    ulst = []
    uvals = set()
    for v in lst:
        if v in uvals:
            continue
        uvals.add(v)
        ulst.append(v)
    return ulst


def get_apply_uri_from_kind(kind: str, namespace: str):
    if kind == "Job":
        return f"/apis/batch/v1/namespaces/{namespace}/jobs"
    elif kind == "Pod":
        return f"/apis/batch/v1/namespaces/{namespace}/pods"
    elif kind == "Deployment":
        return f"/apis/batch/v1/namespaces/{namespace}/deployments"
    else:
        raise KubeApiException("Unable to resolve kind: " + kind)


def apply(
    yaml_config: dict,
    namespace: str = None,
    client: kubernetes.client.CoreV1Api = None,
    pretty: bool = False,
    dry_run: bool = False,
    field_manager: str = None,
    timeout: int = None,
):
    client = client or kubernetes.client.CoreV1Api()
    if isinstance(yaml_config, list):
        # multi apply.
        for oc in yaml_config:
            apply(oc, namespace, client)
        return

    assert yaml_config is not None and isinstance(
        yaml_config, dict
    ), "The yaml config must be a dictionary or a collection of dictionaries"

    kind = yaml_config["kind"]
    metadata = yaml_config["metadata"]
    spec = yaml_config["spec"]

    namespace = metadata.get("namespace", namespace)
    assert namespace is not None, "You must provide the namespace, either in the yaml config or directly."

    path_params = {
        namespace: namespace,
    }

    query_params = {
        "fieldManager": field_manager,
        "pretty": pretty,
        "dry_run": dry_run,
    }

    clean_dictionary_nulls(query_params)

    header_params = {
        "Accept": self.api_client.select_header_accept(
            ["application/json", "application/yaml", "application/vnd.kubernetes.protobuf"]
        )
    }
    form_params = []
    local_var_files = {}
    collection_formats = {}

    client.api_client.call_api(
        get_apply_uri_from_kind(kind, namespace),
        "POST",
        path_params,
        query_params,
        header_params,
        body=yaml_config,
        post_params=form_params,
        files=local_var_files,
        response_type="V1Job",
        auth_settings=["BearerToken"],
        async_req=False,
        _return_http_data_only=False,
        _preload_content=True,
        _request_timeout=timeout,
        collection_formats=collection_formats,
    )


def get_object_status(
    name: str,
    kind: str,
    namespace: str,
    pretty=False,
    timeout: int = None,
):

    assert name is not None, "You must provide the object name"
    assert kind is not None, "You must provide the object kind"
    assert namespace is not None, "you must provide the namespace"

    local_var_params = locals()

    collection_formats = {}
    path_params = {
        "name": name,
        "namespace": namespace,
    }

    query_params = []
    if "pretty" in local_var_params:
        query_params.append(("pretty", local_var_params["pretty"]))  # noqa: E501

    header_params = {}
    form_params = []
    local_var_files = {}
    header_params = {
        "Accept": self.api_client.select_header_accept(
            ["application/json", "application/yaml", "application/vnd.kubernetes.protobuf"]
        )
    }
    # HTTP header `Accept`
    header_params["Accept"] = self.api_client.select_header_accept(
        ["application/json", "application/yaml", "application/vnd.kubernetes.protobuf"]
    )  # noqa: E501

    # Authentication setting
    auth_settings = ["BearerToken"]  # noqa: E501

    return self.api_client.call_api(
        "/apis/batch/v1/namespaces/{namespace}/jobs/{name}/status",
        "GET",
        path_params,
        query_params,
        header_params,
        body=body_params,
        post_params=form_params,
        files=local_var_files,
        response_type=kube,  # noqa: E501
        auth_settings=auth_settings,
        async_req=local_var_params.get("async_req"),
        _return_http_data_only=local_var_params.get("_return_http_data_only"),  # noqa: E501
        _preload_content=local_var_params.get("_preload_content", True),
        _request_timeout=local_var_params.get("_request_timeout"),
        collection_formats=collection_formats,
    )
