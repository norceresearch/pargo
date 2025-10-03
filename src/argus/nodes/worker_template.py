from typing import Any

from ..argo_types.workflows import (
    ArgoParameter,
    ArgoScript,
    ArgoScriptTemplate,
    ArgoSecretRef,
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
):
    if secrets:
        secrets = [
            ArgoSecretRef(secretRef=ArgoParameter(name=secret)) for secret in secrets
        ]

    default = (
        "{"
        + ",".join(f'"{k}": {{{{workflow.parameters.{k}}}}}' for k in parameters)
        + "}"
    )
    inputs = {"parameters": [ArgoParameter(name="inputs", default=default)]}

    template = ArgoScriptTemplate(
        name=template_name,
        script=ArgoScript(
            image=image,
            command=["python"],
            source=script_source,
            env=[
                ArgoParameter(name="ARGUS_DATA", value="{{inputs.parameters.inputs}}"),
                ArgoParameter(name="ARGUS_DIR", value="/tmp"),
            ],
            envFrom=secrets,
            imagePullPolicy=image_pull_policy,
        ),
        inputs=inputs,
        outputs={
            "parameters": [ArgoParameter(name="outputs", valueFrom={"path": outpath})]
        },
        parallelism=parallelism,
    )

    return template
