# What is Pargo?

Pargo is a lightweight Python library for creating Argo Workflows. Key features:

- Generate manifests that can be synced to Argo Workflows using GitOps such as Flux or ArgoCD.
- Generate cron-templates when `schedules` is provided for scheduled execution.
- Generate sensor-templates when `trigger_on` is provided for triggered execution.
- Local execution of individual workflows with pure Python for simple development and debugging

The library is inspired by [`metaflow`](https://github.com/Netflix/metaflow) and the [AWS CDK for defining step functions](https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_stepfunctions-readme.html). It provides a simple way of executing python code locally or remotely on Argo Workflows. It is not a library to generate all-purpose Argo manifests using python. Then the [hera project](https://github.com/argoproj-labs/hera) is a much better alternative, for instance to make DAGs that don't not run sequentially.

# Installation

Not possible to install yet. Should be available on [github](https://github.com/norceresearch) and PyPI in a while.

# Basic usage

```python
from pargo import Workflow

def echo():
    print("Print. Or print not. There is no try.")

basicflow = (
    Workflow.new(name="basicflow")
    .next(echo)
)


if __name__ == "__main__":
    basicflow.run()
```

The workflow can now be run locally by executing the script, `python echoflow.py`

# Parameters

It is possible to provide input parameters to workflows. Parameters can also be passed to subsequent steps by letting task functions return a dict with named parameters.

```python
from pargo import Workflow

def double(x: int):
    return {"x":2*x}

doubleflow = (
    Workflow.new(
        name="doubleflow",
        parameters={"x": 1},
    )
    .next(double)
    .next(double)
)


if __name__ == "__main__":
    doubleflow.run()
```

When the workflow is finished, `x=4`.

# Foreach

Steps can be executed for each item:

```python
from pargo import Foreach, Workflow

def echo_item(item: str):
    print(item)

(
    Workflow.new(name="foreachflow")
    .next(Foreach(["Hello","World"]).then(echo_item))
)
```

This runs in parallel remotely and in sequence locally. To limit the number of pods executed in parallel, `parallelism` can be set for the full Workflow or individual steps:

```python
from pargo import Foreach, Workflow

def echo_item(item: str):
    print(item)

(
    Workflow.new(name="foreachflow",parallelism=5) # Up to five pods can run in parallel
    .next(Foreach(["Hello","World"]).then(echo_item,parallelism=1)) # Only run one pod concurrently for this step
)
```

# When

Steps can be executed conditionally

```python
from pargo import When, Workflow
import random

def choice():
    return random.choice([True, False])

def echo_when_true():
    print("Wow, it was True")

def echo_when_false():
    print("Oh no, it was False")

(
    Workflow.new(name="contitionalflow")
    .next(When(choice).then(echo_when_true))
    .next(When(choice).then(echo_when_true).otherwise(echo_when_false))
)
```

In the first step, the task is executed if the choice is `True`. Since no `otherwise` step is provided, nothing is done if the choice is `False` and the wokflow moves to the next step. The second step conditionally executes one of the tasks based on the choice result.
