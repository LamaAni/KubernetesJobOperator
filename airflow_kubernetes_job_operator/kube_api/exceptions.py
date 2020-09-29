from kubernetes.stream.ws_client import ApiException as KubernetesNativeApiException


class KubeApiException(Exception):
    pass


class KubeApiWatcherException(KubeApiException):
    pass


class KubeApiWatcherParseException(KubeApiWatcherException):
    pass


class KubeApiClientException(KubeApiException):
    def __init__(self, *args, rest_api_exception: KubernetesNativeApiException = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.rest_api_exception: KubernetesNativeApiException = rest_api_exception
