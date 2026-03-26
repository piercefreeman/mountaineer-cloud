import types
from typing import Any, Union, get_args, get_origin


def unwrap_nullable_annotation(annotation: Any) -> Any:
    annotation_origin = get_origin(annotation)
    if annotation_origin in (Union, types.UnionType):
        non_null_args = [
            arg
            for arg in get_args(annotation)
            if arg is not type(None)  # noqa: E721
        ]
        if len(non_null_args) != 1:
            return None
        return non_null_args[0]
    return annotation
