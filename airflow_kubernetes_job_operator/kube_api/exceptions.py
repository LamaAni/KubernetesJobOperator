class KubeApiException(Exception):
    pass


class KubeApiWatcherException(KubeApiException):
    pass


class KubeApiWatcherParseException(KubeApiWatcherException):
    pass
