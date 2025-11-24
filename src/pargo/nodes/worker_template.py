from typing import Any

from ..argo_types.workflows import (
    Parameter,
    RetryStrategy,
    Script,
    ScriptTemplate,
    SecretRef,
)


def worker_template(
    template_name: str,
    script_source: str,
    parameters: dict[str, Any],
    image: str,
    image_pull_policy: str,
    secrets: list[str] | None,
    parallelism: int | None,
    outpath: str,
    retry: int | RetryStrategy | None = None,
):
    if secrets:
        secrets = [SecretRef(secretRef=Parameter(name=secret)) for secret in secrets]

    if isinstance(retry, int):
        retry = RetryStrategy(limit=retry)

    default = (
        "{"
        + ",".join(f'"{k}": {{{{workflow.parameters.{k}}}}}' for k in parameters)
        + "}"
    )
    inputs = {"parameters": [Parameter(name="inputs", default=default)]}

    template = ScriptTemplate(
        name=template_name,
        script=Script(
            image=image,
            command=["python"],
            source=script_source,
            env=[
                Parameter(name="PARGO_DATA", value="{{inputs.parameters.inputs}}"),
                Parameter(name="PARGO_DIR", value="/tmp"),
            ],
            envFrom=secrets,
            imagePullPolicy=image_pull_policy,
        ),
        inputs=inputs,
        outputs={
            "parameters": [Parameter(name="outputs", valueFrom={"path": outpath})]
        },
        parallelism=parallelism,
        retryStrategy=retry,
    )

    return template
