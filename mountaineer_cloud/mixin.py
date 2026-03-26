from typing import Any, cast

from pydantic import model_validator
from pydantic.fields import FieldInfo

from mountaineer_cloud.primitives.storage import (
    CloudFile,
    get_cloud_field_definition,
    get_cloud_file_core_type,
)


class CloudMixin:
    """
    Binds cloud field definitions back onto runtime field values.

    This mixin is required because cloud field configuration is declared in
    global scope via `Field(...)` or `CloudField(...)`. At that point the field
    object has no access to the resolved type-hinted field type, the eventual
    model class, or the model instance via `self`. We patch that gap after
    model construction and on attribute assignment so bound values like
    `CloudFile[...]` can still resolve their per-field configuration correctly.
    """

    @model_validator(mode="after")
    def _bind_cloud_fields(self):
        """
        After a model has been constructed, bind any `CloudField(...)` values defined at the class-level
        into the hydrated instances of the primitives themselves (like `CloudFile[...]`).

        """
        model_fields = cast(
            dict[str, FieldInfo], getattr(type(self), "model_fields", {})
        )
        for field_name, field_info in model_fields.items():
            bound_value = self._bind_cloud_value(
                field_name,
                getattr(self, field_name),
                field_info,
            )
            if bound_value is not getattr(self, field_name):
                object.__setattr__(self, field_name, bound_value)
        return self

    def __setattr__(self, name: str, value: Any) -> None:
        model_fields = cast(
            dict[str, FieldInfo], getattr(type(self), "model_fields", {})
        )
        if name in model_fields:
            value = self._bind_cloud_value(name, value, model_fields[name])
        super().__setattr__(name, value)

    def _bind_cloud_value(
        self,
        field_name: str,
        value: Any,
        field_info: FieldInfo,
    ) -> Any:
        definition = get_cloud_field_definition(field_info)
        if definition is None or value is None:
            return value

        if get_cloud_file_core_type(field_info.annotation) is None:
            raise ValueError(
                f"{self.__class__.__name__}.{field_name} uses CloudField(...) "
                "but is not annotated as CloudFile[CoreType]."
            )

        if isinstance(value, CloudFile):
            return value.bind(
                definition=definition,
                owner=self,
                field_name=field_name,
            )

        if isinstance(value, str):
            return CloudFile(value).bind(
                definition=definition,
                owner=self,
                field_name=field_name,
            )

        return value
