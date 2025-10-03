from __future__ import annotations

from json import dumps, loads
from os import environ
from typing import Any, Callable

from loguru import logger

from ..argo_types.workflows import (
    ArgoParameter,
    ArgoStep,
    ArgoStepsTemplate,
)
from .node import Node
from .run import argus_path, merge_foreach, run_foreach
from .step import StepNode, StepTask
from .worker_template import worker_template

ForeachTask = Callable[..., list[Any]]


class Foreach(Node):
    task: ForeachTask | list[Any]
    item_name: str = "item"
    image: str | None = None
    secrets: list[str] | None = None
    _then: StepNode | None = None
    _prev: str = "foreach"

    def __init__(
        self,
        task: ForeachTask | Callable | list[Any],
        item_name: str = "item",
        **kwargs,
    ):
        super().__init__(task=task, item_name=item_name, **kwargs)

    @property
    def argo_name(self):
        return "foreach"

    def then(self, task: StepTask, **kwargs) -> Foreach:
        if self._prev != "foreach":
            raise RuntimeError(".then(...) must follow Foreach(...) ")
        self._then = StepNode(task=task, **kwargs)
        self._prev = "then"
        return self

    def run(self):
        logger.info("Running foreach loop")
        data_path = argus_path() / "data.json"
        if callable(self.task):
            data = loads(data_path.read_text())
            environ["ARGUS_DATA"] = dumps(data)
            items = run_foreach(self.task.__name__, self.task.__module__)
        elif isinstance(self.task, list):
            items = self.task

        results = []
        for i, item in enumerate(items):
            logger.info(f"Processing item {i}: {item}")
            environ["ARGUS_ITEM"] = dumps({self.item_name: dumps(item)})
            result = self._then.run(write_data=False)
            results.append(result)
        data_path.write_text(dumps(results))

        environ["ARGUS_DATA"] = dumps(results)
        merge_foreach()

        logger.info("Foreach loop finished")

    def get_templates(
        self,
        step_counter: int,
        default_image: str,
        image_pull_policy: str,
        default_secrets: list[str] | None,
        default_parameters: dict[str, Any],
    ):
        block_name = f"step-{step_counter}-{self.argo_name}"
        then_name = block_name + "-" + self._then.argo_name
        merge_name = block_name + "-merge"

        templates = [self.get_steps(block_name, default_parameters)]

        if callable(self.task):
            foreach_name = (
                block_name + "-" + self.task.__name__.lower().replace("_", "-")
            )
            script_source = f'from {run_foreach.__module__} import run_foreach\nrun_foreach("{self.task.__name__}", "{self.task.__module__}")'
            template = worker_template(
                template_name=foreach_name,
                script_source=script_source,
                parameters=default_parameters,
                image=self.image or default_image,
                image_pull_policy=image_pull_policy,
                secrets=self.secrets or default_secrets,
                parallelism=None,
                outpath="/tmp/foreach.json",
            )
            templates.append(template)

        template = self._then.get_templates(
            step_counter=step_counter,
            default_image=default_image,
            image_pull_policy=image_pull_policy,
            default_secrets=default_secrets,
            default_parameters=default_parameters,
        )
        template[0].name = then_name
        template[0].script.env.append(
            ArgoParameter(
                name="ARGUS_ITEM",
                value=f'{{"{self.item_name}": "{{{{inputs.parameters.item}}}}"}}',
            )
        )
        template[0].inputs["parameters"].append(ArgoParameter(name="item"))
        templates.extend(template)

        script_source = (
            f"from {merge_foreach.__module__} import merge_foreach\nmerge_foreach()"
        )
        template = worker_template(
            template_name=merge_name,
            script_source=script_source,
            parameters=default_parameters,
            image=default_image,
            image_pull_policy=image_pull_policy,
            secrets=self.secrets or default_secrets,
            parallelism=None,
            outpath="/tmp/data.json",
        )
        templates.append(template)

        return templates

    def get_steps(self, block_name: str, default_parameters: dict[str, Any]):
        then_name = block_name + "-" + self._then.argo_name
        merge_name = block_name + "-merge"
        default = ",".join(
            f'"{k}": {{{{workflow.parameters.{k}}}}}' for k in default_parameters
        )
        default = f"{{{default}}}"

        steps = ArgoStepsTemplate(
            name=block_name,
            inputs={"parameters": [ArgoParameter(name="inputs", default=default)]},
            steps=[],
            outputs={
                "parameters": [
                    ArgoParameter(
                        name="outputs",
                        valueFrom={
                            "parameter": f"{{{{steps.{merge_name}.outputs.parameters.outputs}}}}"
                        },
                    ),
                ]
            },
        )

        if callable(self.task):
            foreach_name = (
                block_name + "-" + self.task.__name__.lower().replace("_", "-")
            )
            parameters = [
                ArgoParameter(
                    name="inputs",
                    value="{{inputs.parameters.inputs}}",
                )
            ]
            steps.steps.append(
                [
                    ArgoStep(
                        name=foreach_name,
                        template=foreach_name,
                        arguments={"parameters": parameters},
                    )
                ]
            )
            with_param = f"{{{{steps.{foreach_name}.outputs.parameters.outputs}}}}"
        elif isinstance(self.task, list):
            with_param = dumps([dumps(task) for task in self.task])

        parameters = [
            ArgoParameter(
                name="inputs",
                value="{{inputs.parameters.inputs}}",
            ),
            ArgoParameter(
                name="item",
                value="{{item}}",
            ),
        ]
        steps.steps.append(
            [
                ArgoStep(
                    name=then_name,
                    template=then_name,
                    arguments={"parameters": parameters},
                    withParam=with_param,
                )
            ]
        )

        parameters = [
            ArgoParameter(
                name="inputs",
                value=f"{{{{steps.{then_name}.outputs.parameters.outputs}}}}",
            )
        ]
        steps.steps.append(
            [
                ArgoStep(
                    name=merge_name,
                    template=merge_name,
                    arguments={"parameters": parameters},
                )
            ]
        )
        return steps
