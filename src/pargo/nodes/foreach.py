from __future__ import annotations

from json import dumps
from typing import Any, Callable

from loguru import logger
from pydantic import Field

from ..argo_types.workflows import (
    DAGTemplate,
    Parameter,
    RetryStrategy,
    Task,
)
from .import_path import import_path
from .node import Node
from .run import merge_foreach, run_foreach
from .step import StepNode, StepTask
from .worker_template import worker_template

ForeachTask = Callable[..., list[Any]]


class Foreach(Node):
    """
    Class for executing steps for each item.
    """

    task: ForeachTask | dict[str,list[Any]] = Field(
        description="Callable or directly a dict with a list to iterate over."
    )
    image: str | None = Field(
        default=None, description="Overwrite workflow image for the ForeachTask"
    )
    secrets: list[str] | None = Field(
        default=None, description="Overwrite workflow secrets for the ForeachTask"
    )
    retry: int | RetryStrategy | None = Field(
        default=None, description="Overwrite workflow retry for the ForeachTask"
    )
    _then: Node | None = None
    _prev: str = "foreach"

    def __init__(
        self,
        task: ForeachTask | Callable | list[Any],
        item_name: str = "item",
        **kwargs,
    ):
        super().__init__(task=task, item_name=item_name, **kwargs)

    @property
    def task_name(self):
        """Name of the foreach-task."""
        return self.task.__name__

    @property
    def argo_name(self):
        """Argo-friendly name of the foreach-task."""
        return self.task_name.lower().replace("_", "-")

    @property
    def task_module(self):
        """Module of the task."""
        if self.task.__module__ and self.task.__module__ != "__main__":
            return self.task.__module__
        else:
            return import_path(self.task)

    def then(self, node: StepTask | Node, **kwargs) -> Foreach:
        """Set the task to execute for each item."""
        if self._prev != "foreach":
            raise RuntimeError(".then(...) must follow Foreach(...) ")
        if callable(node):
            node = StepNode(task=node, **kwargs)
        self._then = node
        self._prev = "then"
        return self

    def run(self, data: dict[str, Any], items: dict[str, Any] = {}):
        """Run the Foreach-block locally"""
        logger.info("Running foreach loop")

        if callable(self.task):
            collection = run_foreach(self.task_name, self.task_module, data)
        elif isinstance(self.task, dict):
            collection = self.task
        name,elements = next(iter(collection.items()))

        results = []
        for i, element in enumerate(elements):
            logger.info(f"Processing item {i}: {element}")
            result = self._then.run(data, {**items, name: element})
            results.append(result)

        if results:
            data = merge_foreach(results)

        logger.info("Foreach loop finished")
        return data

    def get_templates(
        self,
        step_counter: int,
        default_image: str,
        image_pull_policy: str,
        default_secrets: list[str] | None,
        default_parameters: dict[str, Any],
        default_retry: int | RetryStrategy | None,
        when_level: int = 0,
        foreach_level: int = 0,
    ):
        """Returns a list with the configured templates (DAGTemplate and ScriptTemplates). @private"""
        block_name = f"step-{step_counter}-foreach-{foreach_level}"
        foreach_name = None
        merge_name = f"step-{step_counter}-merge-{foreach_level}"
        # then_name = block_name + "-" + self._then.argo_name
        # merge_name = block_name + "-merge"

        # templates = [self._get_dag(block_name, default_parameters)]

        if callable(self.task):
            foreach_name = f"step-{step_counter}-{self.argo_name}"
            script_source = f'from {run_foreach.__module__} import run_foreach\nrun_foreach("{self.task_name}", "{self.task_module}")'
            foreach_template = worker_template(
                template_name=foreach_name,
                script_source=script_source,
                parameters=default_parameters,
                image=self.image or default_image,
                image_pull_policy=image_pull_policy,
                secrets=self.secrets or default_secrets,
                parallelism=None,
                outpath="/tmp/foreach.json",
                retry=self.retry or default_retry,
            )

        then_templates = self._then.get_templates(
            step_counter=step_counter,
            default_image=default_image,
            image_pull_policy=image_pull_policy,
            default_secrets=default_secrets,
            default_parameters=default_parameters,
            default_retry=self.retry or default_retry,
            when_level=when_level,
            foreach_level=foreach_level + 1,
        )
        then_templates[0].script.env.append(
            Parameter(
                name="PARGO_ITEM",
                value=f'{{"{self.item_name}": "{{{{inputs.parameters.item}}}}"}}',
            )
        )
        then_templates[0].inputs["parameters"].append(Parameter(name="item"))

        script_source = (
            f"from {merge_foreach.__module__} import merge_foreach\nmerge_foreach()"
        )
        merge_template = worker_template(
            template_name=merge_name,
            script_source=script_source,
            parameters=default_parameters,
            image=default_image,
            image_pull_policy=image_pull_policy,
            secrets=self.secrets or default_secrets,
            parallelism=None,
            outpath="/tmp/data.json",
            retry=None,
        )

        block_template = self._get_dag(block_name,foreach_name,then_templates[0].name,merge_name,default_parameters)
        if callable(self.task):
            templates = [block_template,foreach_template,*then_templates,merge_template]
        else:
            templates = [block_template,*then_templates,merge_template]

        return templates

    def _get_dag(self, block_name: str, foreach_name:str|None, then_name: str, merge_name: str, default_parameters: dict[str, Any]):
        default = ",".join(
            f'"{k}": {{{{workflow.parameters.{k}}}}}' for k in default_parameters
        )
        default = f"{{{default}}}"

        expression = (
            f'tasks["{merge_name}"].status == "Succeeded" ? '
            f'tasks["{merge_name}"].outputs.parameters.outputs : '
            f"inputs.parameters.inputs"
        )

        dag_template = DAGTemplate(
            name=block_name,
            inputs={"parameters": [Parameter(name="inputs", default=default)]},
            dag={"tasks": []},
            outputs={
                "parameters": [
                    Parameter(
                        name="outputs",
                        valueFrom={"expression": expression},
                    )
                ]
            },
        )

        if callable(self.task):
            parameters = [
                Parameter(
                    name="inputs",
                    value="{{inputs.parameters.inputs}}",
                )
            ]
            dag_template.dag["tasks"].append(
                Task(
                    name=foreach_name,
                    template=foreach_name,
                    arguments={"parameters": parameters},
                )
            )
            with_param = f"{{{{tasks.{foreach_name}.outputs.parameters.outputs}}}}"
        elif isinstance(self.task, list):
            with_param = dumps([dumps(task) for task in self.task])
        else:
            with_param = None

        parameters = [
            Parameter(
                name="inputs",
                value="{{inputs.parameters.inputs}}",
            ),
            Parameter(
                name="item",
                value="{{item}}",
            ),
        ]
        dag_template.dag["tasks"].append(
            Task(
                name=then_name,
                template=then_name,
                arguments={"parameters": parameters},
                withParam=with_param,
                depends=f"{foreach_name}.Succeeded" if callable(self.task) else None,
            )
        )

        parameters = [
            Parameter(
                name="inputs",
                value=f"{{{{tasks.{then_name}.outputs.parameters.outputs}}}}",
            )
        ]
        dag_template.dag["tasks"].append(
            Task(
                name=merge_name,
                template=merge_name,
                arguments={"parameters": parameters},
                depends=f"{then_name}.Succeeded",
            )
        )

        return dag_template
