from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from io import BytesIO
from typing import (
    IO,
    Any,
    TypeVar,
    cast,
)

from pydantic import Field, GetCoreSchemaHandler
from pydantic.fields import FieldInfo
from pydantic_core import (
    CoreSchema,
    PydanticUndefined,
    PydanticUndefinedType,
    core_schema,
)

from mountaineer_cloud.primitives.base import (
    CloudFieldDefinitionBase,
    CloudValueBase,
    get_cloud_field_definition as get_base_cloud_field_definition,
)
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
class CloudFieldDefinition(CloudFieldDefinitionBase):
    bucket: str
    prefix: str = ""
    suffix: str = ""
    compression: CompressionType = CompressionType.RAW
    storage_backend: StorageBackendType = StorageBackendType.MEMORY
    compression_brotli_level: int = 11

    @property
    def primitive_type(self) -> type["CloudFile[Any]"]:
        return CloudFile

    @property
    def storage_metadata(self) -> S3CompatibleMetadataBase:
        return S3CompatibleMetadataBase(
            bucket=self.bucket,
            prefix=self.prefix,
            suffix=self.suffix,
            pointer_compression=self.compression,
            pointer_storage_backend=self.storage_backend,
            pointer_compression_brotli_level=self.compression_brotli_level,
        )

    def coerce_field_value(
        self,
        value: Any,
        *,
        owner: Any,
        field_name: str,
    ) -> Any:
        if isinstance(value, str):
            return CloudFile(value).bind(
                definition=self,
                owner=owner,
                field_name=field_name,
            )
        return value


class CloudFile(str, CloudValueBase[T]):
    """
    String-backed pointer to an S3-compatible object.

    The runtime value remains a plain string for ORM compatibility, but bound
    instances also know how to upload and download their own content.
    """

    _cloud_definition: CloudFieldDefinition | None
    _cloud_owner_ref: Any | None
    _cloud_field_name: str | None

    def __new__(cls, value: str = ""):
        obj = str.__new__(cls, value)
        obj._init_cloud_binding()
        return obj

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> CoreSchema:
        string_schema = handler.generate_schema(str)
        return core_schema.no_info_after_validator_function(cls, string_schema)

    def _require_definition(self) -> CloudFieldDefinition:
        if self._cloud_definition is None:
            raise ValueError(
                "CloudFile is not bound to a CloudField definition. "
                "Use it inside a model field declared with `CloudField(...)`."
            )
        return self._cloud_definition

    def _get_owner(self):
        if self._cloud_owner_ref is None:
            return None
        return self._cloud_owner_ref()

    def _clone_with_value(self, value: str) -> "CloudFile[T]":
        definition = self._require_definition()
        cloned = type(self)(value)
        return cloned.bind(
            definition=definition,
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

    def _build_pointer(
        self, runtime: CloudRuntime[Any]
    ) -> S3CompatiblePointerBase[Any]:
        definition = self._require_definition()

        class BoundPointer(S3CompatiblePointerBase[Any]):
            s3_object_metadata = definition.storage_metadata
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
    bucket: str,
    prefix: str = "",
    suffix: str = "",
    compression: CompressionType = CompressionType.RAW,
    storage_backend: StorageBackendType = StorageBackendType.MEMORY,
    compression_brotli_level: int = 11,
    default: Any = None,
    default_factory: Callable[[], Any]
    | None
    | PydanticUndefinedType = PydanticUndefined,
    **kwargs: Any,
) -> FieldInfo:
    field_factory = Field

    if bucket is None:
        raise ValueError("CloudField requires a `bucket` value.")

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
        bucket=bucket,
        prefix=prefix,
        suffix=suffix,
        compression=compression,
        storage_backend=storage_backend,
        compression_brotli_level=compression_brotli_level,
    )

    if default_factory is not PydanticUndefined:
        field_info = field_factory(
            default_factory=default_factory,
            **kwargs,
        )
    else:
        field_info = field_factory(
            default=default,
            **kwargs,
        )

    field_info.metadata.append(definition)
    return field_info


def get_cloud_field_definition(field_info: FieldInfo) -> CloudFieldDefinition | None:
    definition = get_base_cloud_field_definition(field_info)
    if isinstance(definition, CloudFieldDefinition):
        return definition
    return None


def get_cloud_field_metadata(field_info: FieldInfo) -> S3CompatibleMetadataBase | None:
    definition = get_cloud_field_definition(field_info)
    if definition is None:
        return None
    return definition.storage_metadata
