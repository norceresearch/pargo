from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from .workflow import Workflow


class Condition(BaseModel):
    items: list[str]

    def __and__(self, other):
        if len(self.items) > 1:
            raise ValueError("Invalid: cannot do (A | B) & C")

        if isinstance(other, Workflow):
            return Condition(items=[self.items[0] + " && " + other.name])
        else:
            if len(other.items) > 1:
                raise ValueError("Invalid: cannot do (A | B) & (C | D)")
            return Condition(items=[self.items[0] + " && " + other.items[0]])

    def __or__(self, other):
        if isinstance(other, Workflow):
            return Condition(items=self.items + [other.name])
        else:
            return Condition(items=self.items + other.items)

    @property
    def names(self):
        seen = set()
        for item in self.items:
            for part in item.split(" && "):
                seen.add(part)
        return sorted(list(seen))

    def __iter__(self):
        return iter(self.items)

    def __repr__(self):
        return str(self.items)

    def __len__(self):
        return len(self.items)
