import types
from abc import ABC, abstractmethod
from typing import Any, Generic, Self, TypeVar, Union, cast, get_args, get_origin
from weakref import ref as weakref_ref

from pydantic.fields import FieldInfo

T = TypeVar("T")


class CloudFieldDefinitionBase(ABC):
    field_factory_name = "CloudField"

    @property
    @abstractmethod
    def primitive_type(self) -> type["CloudValueBase[Any]"]:
        """
        The runtime primitive type produced by this field definition.
        """

    def bind_field_value(
        self,
        value: Any,
        *,
        owner: Any,
        field_name: str,
    ) -> Any:
        primitive_type = self.primitive_type
        if isinstance(value, primitive_type):
            return value.bind(
                definition=self,
                owner=owner,
                field_name=field_name,
            )
        return self.coerce_field_value(
            value,
            owner=owner,
            field_name=field_name,
        )

    @abstractmethod
    def coerce_field_value(
        self,
        value: Any,
        *,
        owner: Any,
        field_name: str,
    ) -> Any:
        """
        Convert raw model values into the bound runtime primitive type.
        """


class CloudValueBase(Generic[T], ABC):
    _cloud_definition: CloudFieldDefinitionBase | None
    _cloud_owner_ref: Any | None
    _cloud_field_name: str | None

    def _init_cloud_binding(self):
        self._cloud_definition = None
        self._cloud_owner_ref = None
        self._cloud_field_name = None

    def bind(
        self,
        *,
        definition: CloudFieldDefinitionBase,
        owner: Any | None = None,
        field_name: str | None = None,
    ) -> Self:
        self._cloud_definition = definition
        self._cloud_owner_ref = weakref_ref(owner) if owner is not None else None
        self._cloud_field_name = field_name
        return self

    def _require_definition(self) -> CloudFieldDefinitionBase:
        if self._cloud_definition is None:
            raise ValueError(
                f"{self.__class__.__name__} is not bound to a field definition. "
                f"Use it inside a model field declared with `{type(self)._field_factory_name()}(...)`."
            )
        return self._cloud_definition

    def _get_owner(self):
        if self._cloud_owner_ref is None:
            return None
        return self._cloud_owner_ref()

    @classmethod
    def _field_factory_name(cls) -> str:
        return "CloudField"


def _unwrap_nullable_annotation(annotation: Any) -> Any:
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


def get_cloud_primitive_type(annotation: Any) -> type[CloudValueBase[Any]] | None:
    annotation = _unwrap_nullable_annotation(annotation)
    if annotation is None:
        return None

    annotation_origin = get_origin(annotation)
    if not isinstance(annotation_origin, type):
        return None

    if not issubclass(annotation_origin, CloudValueBase):
        return None

    return cast(type[CloudValueBase[Any]], annotation_origin)


def get_cloud_core_type(annotation: Any) -> type[Any] | None:
    annotation = _unwrap_nullable_annotation(annotation)
    if annotation is None:
        return None

    primitive_type = get_cloud_primitive_type(annotation)
    if primitive_type is None:
        return None

    annotation_args = get_args(annotation)
    if len(annotation_args) != 1 or not isinstance(annotation_args[0], type):
        return None

    return annotation_args[0]


def get_cloud_field_definition(
    field_info: FieldInfo,
) -> CloudFieldDefinitionBase | None:
    for metadata in field_info.metadata:
        if isinstance(metadata, CloudFieldDefinitionBase):
            return metadata
    return None
