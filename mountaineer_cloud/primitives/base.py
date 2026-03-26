from abc import ABC, abstractmethod
from typing import Any, Generic, Self, TypeVar, cast, get_args, get_origin
from weakref import ref as weakref_ref

from pydantic.fields import FieldInfo

from mountaineer_cloud.typing import unwrap_nullable_annotation

T = TypeVar("T")


class CloudFieldDefinitionBase(ABC):
    field_factory_name = "CloudFileField"

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
    """
    Base class for cloud-aware runtime primitive values.

    Subclasses such as `CloudFile[...]` are still lightweight values at runtime,
    but they carry binding metadata that lets them recover the field
    configuration declared on the model and, when available, the owning model
    instance itself.
    """

    _cloud_definition: CloudFieldDefinitionBase | None
    """The concrete field definition currently bound to this runtime value."""

    _cloud_owner_ref: Any | None
    """A weak reference to the owning model instance, when this value is bound."""

    _cloud_field_name: str | None
    """The model field name this value is currently attached to, if known."""

    def _init_cloud_binding(self) -> None:
        """
        Reset all cloud binding metadata on a newly constructed value.

        Subclasses call this during `__new__` so fresh primitive instances start
        unbound until a field definition or model bind step attaches context.
        """
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
        """
        Bind this runtime value to its field definition and optional owner.

        `definition` provides the per-field configuration declared via the
        corresponding field factory, while `owner` and `field_name` let the
        primitive push updates back onto the model instance when needed.
        """
        self._cloud_definition = definition
        self._cloud_owner_ref = weakref_ref(owner) if owner is not None else None
        self._cloud_field_name = field_name
        return self

    def _require_definition(self) -> CloudFieldDefinitionBase:
        """
        Return the bound field definition or raise if the value is unbound.

        Cloud-aware primitives rely on their field definition for runtime
        behavior, so operations such as storage access should fail loudly when a
        value was created outside a configured model field.
        """
        if self._cloud_definition is None:
            raise ValueError(
                f"{self.__class__.__name__} is not bound to a field definition. "
                f"Use it inside a model field declared with `{type(self)._field_factory_name()}(...)`."
            )
        return self._cloud_definition

    def _get_owner(self) -> Any | None:
        """
        Resolve the live model instance that owns this value, if it still exists.

        The owner is stored as a weak reference so bound primitives do not keep
        their parent model alive unnecessarily.
        """
        if self._cloud_owner_ref is None:
            return None
        return self._cloud_owner_ref()

    @classmethod
    def _field_factory_name(cls) -> str:
        """
        Return the user-facing field factory name for error messages.

        Subclasses can override this when they are produced by a more specific
        field helper than the generic base default.
        """
        return "CloudFileField"


def get_cloud_primitive_type(annotation: Any) -> type[CloudValueBase[Any]] | None:
    """
    Extract the cloud-aware primitive class from a field type hint.

    Example:
        `file_url: CloudFile[AWSCore] | None = CloudFileField(bucket="uploads")`

        For that field:
        - the `annotation` passed here is `CloudFile[AWSCore] | None`
        - this function returns `CloudFile`

    """
    annotation = unwrap_nullable_annotation(annotation)
    if annotation is None:
        return None

    annotation_origin = get_origin(annotation)
    if not isinstance(annotation_origin, type):
        return None

    if not issubclass(annotation_origin, CloudValueBase):
        return None

    return cast(type[CloudValueBase[Any]], annotation_origin)


def get_cloud_core_type(annotation: Any) -> type[Any] | None:
    """
    Extract the provider core type parameter from a cloud-aware field hint.

    Example:
        `file_url: CloudFile[AWSCore] | None = CloudFileField(bucket="uploads")`

        For that field:
        - the `annotation` passed here is `CloudFile[AWSCore] | None`
        - `get_cloud_primitive_type(...)` returns `CloudFile`
        - this function returns `AWSCore`

    """
    annotation = unwrap_nullable_annotation(annotation)
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
    """
    Extract the concrete cloud field definition stored on a `FieldInfo`.

    Example:
        `file_url: CloudFile[AWSCore] | None = CloudFileField(bucket="uploads")`

        For that field:
        - the type hint `CloudFile[AWSCore] | None` is handled by
          `get_cloud_primitive_type(...)` and `get_cloud_core_type(...)`
        - the `= CloudFileField(bucket="uploads")` call produces the `FieldInfo`
        - this function reads `field_info.metadata` and returns the concrete
          field definition object attached there, such as
          `CloudFileFieldDefinition(...)`

    """
    for metadata in field_info.metadata:
        if isinstance(metadata, CloudFieldDefinitionBase):
            return metadata
    return None
