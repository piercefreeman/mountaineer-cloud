import types
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from io import BytesIO
from typing import (
    IO,
    Any,
    Generic,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
)
from weakref import ref as weakref_ref

from pydantic import Field, GetCoreSchemaHandler, model_validator
from pydantic.fields import FieldInfo
from pydantic_core import CoreSchema, PydanticUndefined, core_schema

from mountaineer_cloud.providers_common.s3_compat import (
    CloudRuntime,
    CompressionType,
    S3CompatibleMetadataBase,
    S3CompatiblePointerBase,
    StorageBackendType,
    resolve_cloud_runtime,
)

T = TypeVar("T")


@dataclass(frozen=True)
class CloudFieldDefinition:
    metadata: S3CompatibleMetadataBase


@dataclass(frozen=True)
class CloudFileBinding(Generic[T]):
    definition: CloudFieldDefinition


class CloudFile(str, Generic[T]):
    """
    String-backed pointer to an S3-compatible object.

    The runtime value remains a plain string for ORM compatibility, but bound
    instances also know how to upload and download their own content.
    """

    _cloud_binding: CloudFileBinding[Any] | None
    _cloud_owner_ref: Any | None
    _cloud_field_name: str | None

    def __new__(cls, value: str = ""):
        obj = str.__new__(cls, value)
        obj._cloud_binding = None
        obj._cloud_owner_ref = None
        obj._cloud_field_name = None
        return obj

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> CoreSchema:
        string_schema = handler.generate_schema(str)
        return core_schema.no_info_after_validator_function(cls, string_schema)

    def bind(
        self,
        *,
        definition: CloudFieldDefinition,
        owner: Any | None = None,
        field_name: str | None = None,
    ) -> "CloudFile[T]":
        self._cloud_binding = CloudFileBinding(definition=definition)
        self._cloud_owner_ref = weakref_ref(owner) if owner is not None else None
        self._cloud_field_name = field_name
        return cast("CloudFile[T]", self)

    def _require_binding(self) -> CloudFileBinding[T]:
        if self._cloud_binding is None:
            raise ValueError(
                "CloudFile is not bound to a CloudField definition. "
                "Use it inside a model field declared with `CloudField(...)`."
            )
        return cast("CloudFileBinding[T]", self._cloud_binding)

    def _get_owner(self):
        if self._cloud_owner_ref is None:
            return None
        return self._cloud_owner_ref()

    def _clone_with_value(self, value: str) -> "CloudFile[T]":
        binding = self._require_binding()
        cloned = type(self)(value)
        return cloned.bind(
            definition=binding.definition,
            owner=self._get_owner(),
            field_name=self._cloud_field_name,
        )

    def _apply_new_value(self, value: str) -> "CloudFile[T]":
        next_value = self._clone_with_value(value)

        owner = self._get_owner()
        if owner is not None and self._cloud_field_name is not None:
            setattr(owner, self._cloud_field_name, next_value)
            return cast("CloudFile[T]", getattr(owner, self._cloud_field_name))

        return next_value

    def _build_pointer(self, runtime: CloudRuntime[Any]) -> S3CompatiblePointerBase[Any]:
        binding = self._require_binding()

        class BoundPointer(S3CompatiblePointerBase[Any]):
            s3_object_metadata = binding.definition.metadata
            s3_session_manager = runtime.session_manager

        return BoundPointer(s3_object_path=str(self) or None)

    async def put_content(
        self,
        core: T,
        content: bytes,
        *,
        content_type: str | None = None,
        explicit_s3_path: str | None = None,
    ) -> "CloudFile[T]":
        return await self.put_fileobj(
            core,
            BytesIO(content),
            content_type=content_type,
            explicit_s3_path=explicit_s3_path,
        )

    async def put_fileobj(
        self,
        core: T,
        payload: IO[bytes],
        *,
        content_type: str | None = None,
        explicit_s3_path: str | None = None,
    ) -> "CloudFile[T]":
        runtime = await resolve_cloud_runtime(core)
        pointer = self._build_pointer(runtime)

        await pointer.put_content_into_pointer(
            payload=payload,
            content_type=content_type,
            explicit_s3_path=explicit_s3_path,
            session=runtime.session,
            config=runtime.config,
        )

        return self._apply_new_value(pointer.s3_object_path or "")

    async def copy_content(
        self,
        core: T,
        payload: IO[bytes],
        *,
        extension: str,
        content_type: str | None = None,
        explicit_s3_path: str | None = None,
    ) -> "CloudFile[T]":
        runtime = await resolve_cloud_runtime(core)
        pointer = self._build_pointer(runtime)

        await pointer.copy_content_into_pointer(
            payload=payload,
            extension=extension,
            content_type=content_type,
            explicit_s3_path=explicit_s3_path,
            session=runtime.session,
            config=runtime.config,
        )

        return self._apply_new_value(pointer.s3_object_path or "")

    @asynccontextmanager
    async def get_contents(self, core: T):
        runtime = await resolve_cloud_runtime(core)
        pointer = self._build_pointer(runtime)

        async with pointer.get_contents_from_pointer(
            session=runtime.session,
            config=runtime.config,
        ) as file:
            yield file

    async def get_content(self, core: T) -> bytes:
        async with self.get_contents(core) as file:
            return file.read()

def CloudField(
    *,
    bucket: str | None = None,
    prefix: str = "",
    suffix: str = "",
    key_bucket: str | None = None,
    key_prefix: str | None = None,
    key_suffix: str | None = None,
    compression: CompressionType = CompressionType.RAW,
    storage_backend: StorageBackendType = StorageBackendType.MEMORY,
    compression_brotli_level: int = 11,
    default: Any = None,
    default_factory: Callable[[], Any] | None = PydanticUndefined,
    **kwargs: Any,
) -> FieldInfo:
    resolved_bucket = key_bucket if key_bucket is not None else bucket
    resolved_prefix = key_prefix if key_prefix is not None else prefix
    resolved_suffix = key_suffix if key_suffix is not None else suffix
    field_factory = Field

    if resolved_bucket is None:
        raise ValueError("CloudField requires a `bucket` or `key_bucket` value.")

    try:
        from iceaxe import Field as IceaxeField
        from iceaxe.sql_types import ColumnType
    except ImportError:
        pass
    else:
        field_factory = IceaxeField
        if "explicit_type" not in kwargs:
            kwargs["explicit_type"] = ColumnType.VARCHAR

    definition = CloudFieldDefinition(
        metadata=S3CompatibleMetadataBase(
            key_bucket=resolved_bucket,
            key_prefix=resolved_prefix,
            key_suffix=resolved_suffix,
            pointer_compression=compression,
            pointer_storage_backend=storage_backend,
            pointer_compression_brotli_level=compression_brotli_level,
        )
    )

    if default_factory is not PydanticUndefined:
        field_info = field_factory(
            default_factory=default_factory,
            **kwargs,
        )
        field_info.metadata.append(definition)
        return field_info

    field_info = field_factory(
        default=default,
        **kwargs,
    )
    field_info.metadata.append(definition)
    return field_info


def get_cloud_field_definition(field_info: FieldInfo) -> CloudFieldDefinition | None:
    for metadata in field_info.metadata:
        if isinstance(metadata, CloudFieldDefinition):
            return metadata
    return None


def get_cloud_field_metadata(field_info: FieldInfo) -> S3CompatibleMetadataBase | None:
    definition = get_cloud_field_definition(field_info)
    if definition is None:
        return None
    return definition.metadata


def get_cloud_file_core_type(annotation: Any) -> type[Any] | None:
    annotation_origin = get_origin(annotation)
    if annotation_origin in (Union, types.UnionType):
        non_null_args = [
            arg for arg in get_args(annotation) if arg is not type(None)  # noqa: E721
        ]
        if len(non_null_args) != 1:
            return None
        annotation = non_null_args[0]
        annotation_origin = get_origin(annotation)

    if annotation_origin is not CloudFile:
        return None

    annotation_args = get_args(annotation)
    if len(annotation_args) != 1 or not isinstance(annotation_args[0], type):
        return None

    return annotation_args[0]


class CloudFileModelMixin:
    @model_validator(mode="after")
    def _bind_cloud_file_fields(self):
        for field_name, field_info in self.__class__.model_fields.items():
            bound_value = self._bind_cloud_file_value(
                field_name,
                getattr(self, field_name),
                field_info,
            )
            if bound_value is not getattr(self, field_name):
                object.__setattr__(self, field_name, bound_value)
        return self

    def __setattr__(self, name: str, value: Any) -> None:
        model_fields = getattr(self.__class__, "model_fields", {})
        if name in model_fields:
            value = self._bind_cloud_file_value(name, value, model_fields[name])
        super().__setattr__(name, value)

    def _bind_cloud_file_value(
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
