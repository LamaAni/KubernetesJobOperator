import os
from typing import List, Callable
from kubernetes.config import kube_config, incluster_config, load_kube_config, list_kube_config_contexts
from kubernetes.config.kube_config import Configuration

from airflow_kubernetes_job_operator.kube_api.exceptions import KubeApiException
from airflow_kubernetes_job_operator.kube_api.utils import join_locations_list, not_empty_string
from airflow_kubernetes_job_operator.kube_api.collections import KubeResourceKind

DEFAULT_KUBE_CONFIG_LOCATIONS: List[str] = join_locations_list(
    [kube_config.KUBE_CONFIG_DEFAULT_LOCATION],
    os.environ.get("KUBE_API_DEFAULT_CONFIG_LOCATIONS", None),
)

DEFAULT_SERVICE_ACCOUNT_PATH = os.path.dirname(incluster_config.SERVICE_CERT_FILENAME)

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
        """Sets the default kube configuration. Will be chosen as
        default.

        Args:
            config (Configuration): The config.
        """
        assert isinstance(config, Configuration), ValueError(
            "Config must be of type kubernetes.config.kube_config.Configuration"
        )
        cls._default_kube_config = config

    @classmethod
    def set_default_namespace(cls, namespace: str):
        """Set the global default namespace. Will be chosen if
        a configuration namespace is not found.

        Args:
            namespace (str): The namespace
        """
        assert not_empty_string(namespace), ValueError("namespace must be a non empty string")
        cls._default_namespace = namespace

    def add_kube_config_search_location(file_path: str):
        """Add a global search location for kube configuration files.
        The location will be added to the top of the list and would be
        chosen first if exists.

        Args:
            file_path (str): The path to the configuration file.
        """
        DEFAULT_KUBE_CONFIG_LOCATIONS.insert(0, file_path)

    @classmethod
    def find_default_config_file(cls, extra_config_locations: List[str] = None) -> str:
        """Returns the first valid config file.

        Args:
            extra_config_locations (List[str], optional): Extra search locations. Defaults to None.

        Returns:
            str: The config file path.
        """
        config_possible_locations = join_locations_list(
            extra_config_locations,
            DEFAULT_KUBE_CONFIG_LOCATIONS,
        )

        default_config_file = None

        for loc in config_possible_locations:
            loc = loc if "~" not in loc else os.path.expanduser(loc)
            if os.path.isfile(loc):
                default_config_file = loc
                break
        return default_config_file

    @classmethod
    def load_kubernetes_configuration(
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
            configuration.api_key["authorization"] = (
                loader.token if loader.token.lower().strip().startswith("bearer ") else "bearer " + loader.token
            )
            default_namespace_file = os.path.join(DEFAULT_SERVICE_ACCOUNT_PATH, "namespace")
            if os.path.exists(default_namespace_file):
                with open(default_namespace_file, "r") as raw:
                    configuration.default_namespace = raw.read()
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
            default_config_file = cls.find_default_config_file(extra_config_locations=extra_config_locations)
            if default_config_file is not None:
                configuration = load_from_file(default_config_file)
            elif os.path.isfile(incluster_config.SERVICE_TOKEN_FILENAME):
                configuration = load_in_cluster()

        if configuration is not None:
            configuration.filepath = configuration.filepath if hasattr(configuration, "filepath") else None
            configuration.default_namespace = (
                default_namespace or configuration.default_namespace
                if hasattr(configuration, "default_namespace")
                else None
            )

            if set_as_default:
                cls.set_default_kube_config(configuration)

        return configuration

    @classmethod
    def get_active_context_info(cls, configuration: Configuration):
        """Returns the current configuration info from the config file

        Args:
            configuration (Configuration): The configuration.

        Returns:
            dict: The context info.
        """
        if (
            not hasattr(configuration, "filepath")
            or configuration.filepath is None
            or not os.path.isfile(configuration.filepath)
        ):
            return {}

        (contexts, active_context) = list_kube_config_contexts(config_file=configuration.filepath)
        return active_context or {}

    @classmethod
    def get_default_namespace(cls, configuration: Configuration):
        """Returns the default namespace for the config."""
        try:
            if hasattr(configuration, "default_namespace") and configuration.default_namespace is not None:
                return configuration.default_namespace
            elif (
                hasattr(configuration, "filepath")
                and configuration.filepath is not None
                and os.path.isfile(configuration.filepath)
            ):
                context_info = cls.get_active_context_info(configuration)
                return context_info.get("context", {}).get("namespace", "default")
            elif cls._default_namespace is not None:
                return cls._default_namespace
            else:
                return "default"
        except Exception as e:
            raise KubeApiException(
                "Could not resolve current namespace, you must provide a namespace or a context file",
                e,
            )

    @classmethod
    def register_kind(
        cls,
        name: str,
        api_version: str,
        parse_kind_state: Callable = None,
        auto_include_in_watch: bool = True,
    ) -> KubeResourceKind:
        """Register a new kubernetes kind that will be used by the kube_api. If the object
        kind has a pase_kind_state, this would be a traceable kind. i.e. you could
        create jobs with it.

        Args:
            name (str): The name of the kind (Pod,Job, ??)
            api_version (str): The api (api/v1, batch)
            parse_kind_state (Callable, optional)->KubeResourceState: Method to transcribe the current
                status to a KubeResourceState. Defaults to None. An object is watchable if it returns three
                states at lease, KubeResourceState.Running, KubeResourceState.Succeeded, KubeResourceState.Failed
            auto_include_in_watch (bool, optional): If true, auto watch changes in this kind when
                watching a namespace. Defaults to True.

        Note:
            There are default parse methods available as static methods on KubeResourceKind class.
        """
        return KubeResourceKind.register_global_kind(
            KubeResourceKind(
                name=name,
                api_version=api_version,
                parse_kind_state=parse_kind_state,
                auto_include_in_watch=auto_include_in_watch,
            )
        )
