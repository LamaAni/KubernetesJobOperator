import hashlib
import kubernetes.client.api_client

kubernetes.client.CoreV1Api().create_namespaced_pod()

print(hashlib.sha256("asda".encode("utf-8")).hexdigest())
