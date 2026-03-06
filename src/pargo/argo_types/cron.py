from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from .primitives import (
    Metadata,
)
from .workflows import WorkflowSpec


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
