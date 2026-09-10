"""Cluster defaults baked into generated manifests.

Override with the environment variables below, or by setting the module attribute
before generating manifests. Defaults are the values pargo has always emitted.
"""

from os import environ

#: Namespace written into every generated manifest.
NAMESPACE = environ.get("PARGO_NAMESPACE", "argo-workflows")

#: Service account the workflow pods and sensors run as.
SERVICE_ACCOUNT = environ.get("PARGO_SERVICE_ACCOUNT", "argo-service-account")
