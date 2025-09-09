from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ArgoTTLStrategy(BaseModel):
    secondsAfterCompletion: int = 300


class ArgoPodGC(BaseModel):
    strategy: str = "OnPodCompletion"


class ArgoWorkflowMetadata(BaseModel):
    generateName: str | None = None
    name: str | None = None
    namespace: str = "argo-workflows"


class ArgoParameter(BaseModel):
    name: str
    value: Any = None
    default: Any = None
    valueFrom: dict[str, str] | None = None


class ArgoSecretRef(BaseModel):
    secretRef: ArgoParameter


class ArgoVolumeMount(BaseModel):
    name: str = "tmp"
    mountPath: str = "/tmp"


class ArgoVolume(BaseModel):
    name: str = "tmp"
    emptyDir: dict = {}


class ArgoWorkflowSpec(BaseModel):
    entrypoint: str
    arguments: dict[str, list[ArgoParameter]] | None = None
    volumes: list[ArgoVolume] | None = None
    templates: list[Any] = []
    ttlStrategy: ArgoTTLStrategy | None = None
    podGC: ArgoPodGC | None = None


class ArgoWorkflowTemplateRef(BaseModel):
    workflowTemplateRef: ArgoParameter


class ArgoCronWorkflowSpec(BaseModel):
    schedules: list[str] | None = None
    timezone: str | None = None
    concurrencyPolicy: str | None = None
    startingDeadlineSeconds: int | None = None
    successfulJobsHistoryLimit: int | None = None
    failedJobsHistoryLimit: int | None = None
    workflowSpec: ArgoWorkflowTemplateRef | None = None


class ArgoStep(BaseModel):
    name: str
    template: str
    when: str | None = None
    withItems: list[Any] | str | None = None
    withParam: Any = None
    arguments: dict[str, list[ArgoParameter]] | None = None


class ArgoStepsTemplate(BaseModel):
    name: str
    steps: list[list[ArgoStep]]


class ArgoScript(BaseModel):
    image: str | None
    command: list
    source: str = Field(..., alias="source")
    envFrom: list[ArgoSecretRef] | None = None
    env: list[ArgoParameter] | None = None
    volumeMounts: list[ArgoVolumeMount] | None = None
    imagePullPolicy: str | None = None


class ArgoScriptTemplate(BaseModel, frozen=True):
    name: str
    inputs: dict[str, list[ArgoParameter]] | None = None
    script: ArgoScript
    outputs: dict[str, list[ArgoParameter]] | None = None
    serviceAccountName: str = "argo-service-account"


class ArgoWorkflow(BaseModel):
    apiVersion: str = "argoproj.io/v1alpha1"
    kind: str = "Workflow"
    metadata: ArgoWorkflowMetadata
    spec: ArgoWorkflowSpec


class ArgoCronWorkflow(BaseModel):
    apiVersion: str = "argoproj.io/v1alpha1"
    kind: str = "Workflow"
    metadata: ArgoWorkflowMetadata
    spec: ArgoCronWorkflowSpec
