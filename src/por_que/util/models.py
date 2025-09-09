from typing import Any


def get_item_or_attr(thing: Any, name: str) -> Any:
    if hasattr(thing, name):
        return getattr(thing, name)

    if name in thing:
        return thing[name]

    raise ValueError(f'{name} is not an attribute on or item in {thing}')
