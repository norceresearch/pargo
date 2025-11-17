from __future__ import annotations

from typing import Literal, TypeAlias

from pydantic import BaseModel

from .primitives import (
    Metadata,
    Parameter,
)
from .workflows import WorkflowResource

ParameterMap: TypeAlias = dict[str, list[Parameter]] | None


class Source(BaseModel):
    resource: WorkflowResource


class ArgoWorkflow(BaseModel):
    group: str
    version: str
    resource: str
    operation: str
    source: Source


class TriggerTemplate(BaseModel):
    name: str
    conditions: str | None = None
    argoWorkflow: ArgoWorkflow


class Trigger(BaseModel):
    template: TriggerTemplate


class FilterData(BaseModel):
    path: str
    type: str
    value: list[str]


class Filters(BaseModel):
    data: list[FilterData]


class Dependency(BaseModel):
    name: str
    eventSourceName: str
    eventName: str
    filters: Filters | None = None


class EventTemplate(BaseModel):
    serviceAccountName: str


class EventSpec(BaseModel):
    eventBusName: str
    template: EventTemplate
    dependencies: list[Dependency]
    triggers: list[Trigger]


class EventSensor(BaseModel):
    apiVersion: str
    kind: Literal["Sensor"] = "Sensor"
    metadata: Metadata
    spec: EventSpec
