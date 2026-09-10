"""Example workflows, used by pargo's integration tests and as runnable documentation.

Every task lives in `pargo` itself, so a container image with pargo installed is
enough to run these — no application code required. Run one locally with
`python -m pargo.examples`, or generate manifests with `wf.to_yaml()`.
"""

from json import dumps
from os import environ

from .nodes.foreach import Foreach
from .nodes.when import When
from .utils import add_item, add_y, choice, double, echo_item, get_items, triple, void
from .workflow import Workflow

# ponytail: overridable so CI can point at the image it just built and loaded into kind.
IMAGE = environ.get("PARGO_IMAGE", "pargo:test")

# Local images are not in a registry, so the default "Always" pull policy cannot work.
PULL_POLICY = environ.get("PARGO_IMAGE_PULL_POLICY", "IfNotPresent")


def divide_by_zero():
    """A task that always fails, to exercise workflow failure handling."""
    _ = 1 / 0


basicflow = Workflow.new(
    name="pargo-example-basic",
    parameters={"x": 1},
    image=IMAGE,
    image_pull_policy=PULL_POLICY,
).next(double)


whenflow = (
    Workflow.new(
        name="pargo-example-when",
        parameters={"x": 1},
        image=IMAGE,
        image_pull_policy=PULL_POLICY,
    )
    .next(When(choice).then(double))
    .next(When(choice).then(double).otherwise(triple))
    .next(void)
)


foreachflow = (
    Workflow.new(
        name="pargo-example-foreach",
        parameters={"x": 1},
        image=IMAGE,
        image_pull_policy=PULL_POLICY,
    )
    .next(Foreach(get_items).then(echo_item))
    .next(Foreach([]).then(echo_item))
    .next(Foreach([1, 2, 3]).then(add_item))
    .next(Foreach([5, 2, 4], item_name="y").then(add_y))
    .next(void)
)


advancedflow = (
    Workflow.new(
        name="pargo-example-advanced",
        parameters={
            "x": 1,
            "param2": "value2",
            "param3": 3.0,
            "param4": dumps({"c": 1}),
        },
        image=IMAGE,
        image_pull_policy=PULL_POLICY,
    )
    .next(double)
    .next(When(choice).then(double).otherwise(triple))
    .next(Foreach(get_items).then(double))
    .next(double)
)


failureflow = Workflow.new(
    name="pargo-example-failure",
    image=IMAGE,
    image_pull_policy=PULL_POLICY,
    retry=0,
).next(divide_by_zero)


#: Every example, with the workflow phase Argo should end up in.
EXAMPLES = {
    basicflow: "Succeeded",
    whenflow: "Succeeded",
    foreachflow: "Succeeded",
    advancedflow: "Succeeded",
    failureflow: "Failed",
}


if __name__ == "__main__":
    for workflow in EXAMPLES:
        if workflow is not failureflow:
            workflow.run()
