from kubernetes.stream.ws_client import ApiException as KubernetesNativeApiException


class KubeApiException(Exception):
    pass


class KubeApiWatcherException(KubeApiException):
    pass


class KubeApiWatcherParseException(KubeApiWatcherException):
    pass


class KubeApiClientException(KubeApiException):
    def __init__(self, *args, inner_exception: KubernetesNativeApiException = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.inner_exception: KubernetesNativeApiException = inner_exception
