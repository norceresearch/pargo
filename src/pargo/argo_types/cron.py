from __future__ import annotations

from typing import Literal, TypeAlias

from pydantic import BaseModel

from .primitives import (
    Metadata,
    Parameter,
)
from .workflows import WorkflowSpec

ParameterMap: TypeAlias = dict[str, list[Parameter]] | None


class CronWorkflowSpec(BaseModel):
    schedules: list[str] | None = None
    timezone: str | None = None
    concurrencyPolicy: str | None = None
    startingDeadlineSeconds: int | None = None
    successfulJobsHistoryLimit: int | None = None
    failedJobsHistoryLimit: int | None = None
    workflowSpec: WorkflowSpec | None = None


class CronWorkflow(BaseModel):
    apiVersion: str = "argoproj.io/v1alpha1"
    kind: Literal["CronWorkflow"] = "CronWorkflow"
    metadata: Metadata
    spec: CronWorkflowSpec
