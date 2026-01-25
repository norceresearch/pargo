from __future__ import annotations

from typing import Any, Literal, TypeAlias

from pydantic import BaseModel, Field

from .primitives import (
    Metadata,
    Parameter,
    PodGC,
    PodMetadata,
    RetryStrategy,
    SecretRef,
    TemplateRef,
    TTLStrategy,
)

ParameterMap: TypeAlias = dict[str, list[Parameter]] | None


class Task(BaseModel):
    name: str
    template: str | None = None
    depends: str | None = None
    when: str | None = None
    withItems: list[Any] | str | None = None
    withParam: Any = None
    arguments: ParameterMap = None
    templateRef: None | TemplateRef = None


class StepsTemplate(BaseModel):
    name: str
    inputs: ParameterMap = None
    steps: list[list[Task]]
    outputs: ParameterMap = None


class DAGTemplate(BaseModel):
    name: str
    inputs: ParameterMap = None
    dag: dict[str, list[Task]]
    outputs: ParameterMap = None


class Script(BaseModel):
    image: str | None
    command: list
    source: str = Field(..., alias="source")
    envFrom: list[SecretRef] | None = None
    env: list[Parameter] | None = None
    terminationMessagePolicy: str = "FallbackToLogsOnError"
    imagePullPolicy: str | None = None


class ScriptTemplate(BaseModel):
    name: str
    inputs: ParameterMap = None
    script: Script
    outputs: ParameterMap = None
    serviceAccountName: str = "argo-service-account"
    parallelism: int | None = None
    retryStrategy: RetryStrategy | None = None


class Resource(BaseModel):
    action: Literal["create"] = "create"
    setOwnerReference: bool = True
    successCondition: str = "status.phase == Succeeded"
    failureCondition: str = "status.phase in (Failed, Error)"
    manifest: str


class ResourceTemplate(BaseModel):
    name: str
    inputs: ParameterMap = None
    resource: Resource
    serviceAccountName: str = "argo-service-account"
    parallelism: int | None = None
    retryStrategy: RetryStrategy | None = None


class WorkflowSpec(BaseModel):
    workflowTemplateRef: None | TemplateRef = None
    entrypoint: None | str = None
    arguments: ParameterMap = None
    templates: list[Any] | None = None
    ttlStrategy: TTLStrategy | None = None
    podGC: PodGC | None = None
    parallelism: int | None = None
    podMetadata: None | PodMetadata = None


class WorkflowResource(BaseModel):
    apiVersion: str = "argoproj.io/v1alpha1"
    kind: Literal["Workflow", "WorkflowTemplate"] = "WorkflowTemplate"
    metadata: Metadata
    spec: WorkflowSpec
