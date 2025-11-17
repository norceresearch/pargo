from __future__ import annotations

from typing import Any, Literal, TypeAlias

from pydantic import BaseModel

PodMetadata: TypeAlias = dict[Literal["annotations", "labels"], dict[str, str]]
RetryPolicy: TypeAlias = Literal["Always", "OnFailure", "OnError", "OnTransientError"]
PodGCStrategy: TypeAlias = Literal[
    "OnPodCompletion", "OnPodSuccess", "OnWorkflowCompletion", "OnWorkflowSuccess"
]


class Metadata(BaseModel):
    generateName: str | None = None
    name: str | None = None
    namespace: str = "argo-workflows"


class Parameter(BaseModel):
    name: str
    value: Any = None
    default: Any = None
    valueFrom: dict[str, str] | None = None


class TTLStrategy(BaseModel):
    secondsAfterCompletion: int = 300


class PodGC(BaseModel):
    strategy: PodGCStrategy = "OnPodCompletion"


class Backoff(BaseModel):
    cap: None | str = None
    duration: str = "1m"
    factor: int = 2
    maxDuration: None | str = None


class RetryStrategy(BaseModel):
    backoff: None | Backoff = Backoff()
    expression: None | str = None
    limit: int = 2
    retryPolicy: RetryPolicy = "Always"


class TemplateRef(BaseModel):
    name: str


class SecretRef(BaseModel):
    secretRef: Parameter
