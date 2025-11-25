from __future__ import annotations

from json import dumps, loads
from os import environ
from typing import Any, Callable

from pydantic import Field

from ..argo_types.workflows import RetryStrategy
from .import_path import import_path
from .node import Node
from .run import pargo_path, run_step
from .worker_template import worker_template

StepTask = Callable[..., None | dict]


class StepNode(Node):  # FIXME Rename to Step to be consitent with When, Foreach
    """Class for worker tasks."""

    task: StepTask = Field(description="Callable worker task. Can return a dict")
    image: str | None = Field(
        default=None, description="Overwrite workflow image for the StepTask"
    )
    secrets: list[str] | None = Field(
        default=None, description="Overwrite workflow secrets for the StepTask"
    )
    parallelism: int | None = Field(
        default=None, description="Overwrite workflow parallelism for the StepTask"
    )
    retry: int | RetryStrategy | None = Field(
        default=None, description="Overwrite workflow retry for the StepTask"
    )

    @property
    def task_name(self):
        """Name of the task."""
        return self.task.__name__

    @property
    def argo_name(self):
        """Argo friendly name of the task."""
        return self.task.__name__.lower().replace("_", "-")

    @property
    def task_module(self):
        """Module of the task."""
        if self.task.__module__ and self.task.__module__ != "__main__":
            return self.task.__module__
        else:
            return import_path(self.task)

    def run(self, write_data: bool = True):
        """Run the step locally"""
        data_path = pargo_path() / "data.json"
        data = loads(data_path.read_text())
        environ["PARGO_DATA"] = dumps(data)
        result = run_step(self.task_name, self.task_module, write_data=write_data)
        return result

    def get_templates(
        self,
        step_counter: int,
        default_image: str,
        image_pull_policy: str,
        default_secrets: list[str] | None,
        default_parameters: dict[str, Any],
        default_retry: int | RetryStrategy | None,
    ):
        """Returns a single item list with the configures ScriptTemplate @private"""
        template_name = f"step-{step_counter}-{self.argo_name}"
        script_source = f'from {run_step.__module__} import run_step\nrun_step("{self.task_name}", "{self.task_module}")'

        template = worker_template(
            template_name=template_name,
            script_source=script_source,
            parameters=default_parameters,
            image=self.image or default_image,
            image_pull_policy=image_pull_policy,
            secrets=self.secrets or default_secrets,
            parallelism=self.parallelism,
            outpath="/tmp/data.json",
            retry=self.retry or default_retry,
        )

        return [template]
