from typing import Any, cast

from pydantic import model_validator
from pydantic.fields import FieldInfo

from mountaineer_cloud.primitives.base import (
    get_cloud_core_type,
    get_cloud_field_definition,
    get_cloud_primitive_type,
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
            current_value = getattr(self, field_name)
            bound_value = self._bind_cloud_value(
                field_name,
                current_value,
                field_info,
            )
            if bound_value is not current_value:
                # Hack to skip our internal __setattr__ implementation
                object.__setattr__(self, field_name, bound_value)
        return self

    def __setattr__(self, name: str, value: Any) -> None:
        """
        If we're explicitly set a new value for a field, resolve their metadata defined at the Field() level like we
        do during model construction in _bind_cloud_fields().

        """
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
        # Make sure this field has actually set their metadata via CloudField()
        definition = get_cloud_field_definition(field_info)
        if definition is None or value is None:
            return value

        primitive_type = get_cloud_primitive_type(field_info.annotation)
        if primitive_type is None or get_cloud_core_type(field_info.annotation) is None:
            raise ValueError(
                f"{self.__class__.__name__}.{field_name} uses "
                f"{definition.field_factory_name}(...) but is not annotated as "
                "CloudPrimitive[CoreType]."
            )

        if not issubclass(primitive_type, definition.primitive_type):
            raise ValueError(
                f"{self.__class__.__name__}.{field_name} uses "
                f"{definition.field_factory_name}(...) but is annotated as "
                f"{primitive_type.__name__}[CoreType] instead of "
                f"{definition.primitive_type.__name__}[CoreType]."
            )

        return definition.bind_field_value(
            value,
            owner=self,
            field_name=field_name,
        )
