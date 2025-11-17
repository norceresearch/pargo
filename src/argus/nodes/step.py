from __future__ import annotations

from json import dumps, loads
from os import environ
from typing import Any, Callable

from ..argo_types.workflows import RetryStrategy
from .node import Node
from .run import argus_path, run_step
from .worker_template import worker_template

StepTask = Callable[..., None | dict]


class StepNode(Node):
    task: StepTask
    image: str | None = None
    secrets: list[str] | None = None
    parallelism: int | None = None
    retry: int | RetryStrategy | None = None

    @property
    def task_name(self):
        return self.task.__name__

    @property
    def argo_name(self):
        return self.task.__name__.lower().replace("_", "-")

    def run(self, write_data: bool = True):
        data_path = argus_path() / "data.json"
        data = loads(data_path.read_text())
        environ["ARGUS_DATA"] = dumps(data)
        result = run_step(
            self.task.__name__, self.task.__module__, write_data=write_data
        )
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
        template_name = f"step-{step_counter}-{self.argo_name}"
        script_source = f'from {run_step.__module__} import run_step\nrun_step("{self.task_name}", "{self.task.__module__}")'

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
