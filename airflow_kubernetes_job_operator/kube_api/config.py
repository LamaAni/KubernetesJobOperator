import os
from typing import List
from kubernetes.config import kube_config, incluster_config, load_kube_config, list_kube_config_contexts
from kubernetes.config.kube_config import Configuration

from airflow_kubernetes_job_operator.kube_api.exceptions import KubeApiException
from airflow_kubernetes_job_operator.kube_api.utils import join_locations_list, not_empty_string


DEFAULT_KUBE_CONFIG_LOCATIONS = join_locations_list(
    [kube_config.KUBE_CONFIG_DEFAULT_LOCATION],
    os.environ.get("KUBERNETES_JOB_OPERATOR_DEFAULT_CONFIG_LOCATIONS", None),
)
DEFAULT_SERVICE_ACCOUNT_PATH = os.path.dirname(incluster_config.SERVICE_CERT_FILENAME)
DEFAULT_USE_ASYNCIO_ENV_NAME = "KUBERNETES_API_CLIENT_USE_ASYNCIO"

KURBETNTES_API_ACCEPT = [
    "application/json",
    "application/yaml",
    "application/vnd.kubernetes.protobuf",
]

KUBERENTES_API_CONTENT_TYPES = [
    "application/json",
    "application/json-patch+json",
    "application/merge-patch+json",
    "application/strategic-merge-patch+json",
]

DEFAULT_AUTO_RECONNECT_MAX_ATTEMPTS = 30
DEFAULT_AUTO_RECONNECT_WAIT_BETWEEN_ATTEMPTS = 5


class KubeApiConfiguration:
    _default_kube_config: kube_config.Configuration = None
    _default_namespace: str = None

    @classmethod
    def set_default_kube_config(cls, config: Configuration):
        assert isinstance(config, Configuration), ValueError(
            "Config must be of type kubernetes.config.kube_config.Configuration"
        )
        cls._default_kube_config = config

    @classmethod
    def set_default_namespace(cls, namespace: str):
        assert not_empty_string(namespace), ValueError("namespace must be a non empty string")
        cls._default_namespace = namespace

    @classmethod
    def find_default_config_file(cls, extra_config_locations: List[str] = None):
        default_config_file = None
        config_possible_locations = join_locations_list(
            extra_config_locations,
            DEFAULT_KUBE_CONFIG_LOCATIONS,
        )
        for loc in config_possible_locations:
            loc = loc if "~" not in loc else os.path.expanduser(loc)
            if os.path.isfile(loc):
                default_config_file = loc
                break
        return default_config_file

    @classmethod
    def load_kubernetes_configuration_from_file(
        cls,
        config_file: str = None,
        is_in_cluster: bool = None,
        extra_config_locations: List[str] = None,
        context: str = None,
        persist: bool = False,
        default_namespace: str = None,
        set_as_default: bool = False,
    ) -> Configuration:
        """Loads a kubernetes configuration

        Args:
            config_file (str, optional): The configuration file path. Defaults to None = search for config.
            is_in_cluster (bool, optional): If true, the client will expect to run inside a cluster
                and to load the cluster config. Defaults to None = auto detect.
            extra_config_locations (List[str], optional): Extra locations to search for a configuration.
                Defaults to None.
            context (str, optional): The context name to run in. Defaults to None = active context.
            persist (bool, optional): If True, config file will be updated when changed (e.g GCP token refresh).
        """

        def load_in_cluster():
            configuration = kube_config.Configuration()

            loader = incluster_config.InClusterConfigLoader(
                incluster_config.SERVICE_TOKEN_FILENAME, incluster_config.SERVICE_CERT_FILENAME
            )
            loader._load_config()

            configuration.host = loader.host
            configuration.ssl_ca_cert = loader.ssl_ca_cert
            configuration.api_key["authorization"] = "bearer " + loader.token

            return configuration

        def load_from_file(fpath):
            configuration = kube_config.Configuration()

            load_kube_config(
                config_file=fpath,
                context=context,
                client_configuration=configuration,
                persist_config=persist,
            )

            configuration.filepath = fpath
            return configuration

        configuration: kube_config.Configuration = None

        # case in cluster.
        if is_in_cluster is True:
            configuration = load_in_cluster()
        # case a file was sent
        elif config_file is not None:
            configuration = load_from_file(config_file)
        # case there was a default config.
        elif cls._default_kube_config is not None and configuration is None:
            configuration = cls._default_kube_config
        # search for config.
        else:
            default_config_file = cls.find_default_config_file()
            if default_config_file is not None:
                configuration = load_from_file(default_config_file)
            elif os.path.isfile(incluster_config.SERVICE_TOKEN_FILENAME):
                configuration = load_in_cluster()

        if configuration is not None:
            configuration.filepath = configuration.filepath if hasattr(configuration, "filepath") else None
            configuration.default_namespace = default_namespace

            if set_as_default:
                cls.set_default_kube_config(configuration)

        return configuration

    @classmethod
    def get_default_namespace(cls, configuration: Configuration):
        """Returns the default namespace for the current config."""
        namespace: str = None  # type:ignore
        try:
            in_cluster_namespace_fpath = os.path.join(DEFAULT_SERVICE_ACCOUNT_PATH, "namespace")
            if os.path.exists(in_cluster_namespace_fpath):
                with open(in_cluster_namespace_fpath, "r", encoding="utf-8") as nsfile:
                    namespace = nsfile.read()
            elif configuration.default_namespace is not None:
                return configuration.default_namespace
            elif hasattr(configuration, "filepath") and configuration.filepath is not None:
                (
                    contexts,
                    active_context,
                ) = list_kube_config_contexts(config_file=configuration.filepath)

                namespace = (
                    active_context.get("context", {}).get("namespace", "default")
                    if isinstance(active_context, dict)
                    else "default"
                )
            elif cls._default_namespace is not None:
                return cls._default_namespace
            else:
                return "default"
        except Exception as e:
            raise KubeApiException(
                "Could not resolve current namespace, you must provide a namespace or a context file",
                e,
            )
        return namespace
