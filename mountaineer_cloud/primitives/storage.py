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
    get_cloud_field_definition as get_cloud_field_definition,
)
from mountaineer_cloud.providers_common.storage import (
    CompressionType,
    StorageBackendType,
    StorageMetadata,
    StorageProviderCore,
)

TStorageCore = TypeVar("TStorageCore", bound=StorageProviderCore[Any])


@dataclass(frozen=True)
class CloudFileFieldDefinition(CloudFieldDefinitionBase):
    field_factory_name = "CloudFileField"

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
    def storage_metadata(self) -> StorageMetadata:
        return StorageMetadata(
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


class CloudFile(str, CloudValueBase[TStorageCore]):
    """
    String-backed pointer to an S3-compatible object.

    The runtime value remains a plain string for ORM compatibility, but bound
    instances also know how to upload and download their own content.
    """

    _cloud_definition: CloudFileFieldDefinition | None
    _cloud_owner_ref: Any | None
    _cloud_field_name: str | None

    async def put_content(
        self,
        core: TStorageCore,
        content: bytes,
        *,
        content_type: str | None = None,
        explicit_s3_path: str | None = None,
    ) -> "CloudFile[TStorageCore]":
        return await self.put_fileobj(
            core,
            BytesIO(content),
            content_type=content_type,
            explicit_s3_path=explicit_s3_path,
        )

    async def put_fileobj(
        self,
        core: TStorageCore,
        payload: IO[bytes],
        *,
        content_type: str | None = None,
        explicit_s3_path: str | None = None,
    ) -> "CloudFile[TStorageCore]":
        definition = self._require_definition()
        stored_path = await core.storage_write(
            path=str(self) or None,
            metadata=definition.storage_metadata,
            payload=payload,
            content_type=content_type,
            explicit_storage_path=explicit_s3_path,
        )

        return self._apply_new_value(stored_path)

    async def copy_content(
        self,
        core: TStorageCore,
        payload: IO[bytes],
        *,
        extension: str,
        content_type: str | None = None,
        explicit_s3_path: str | None = None,
    ) -> "CloudFile[TStorageCore]":
        definition = self._require_definition()
        stored_path = await core.storage_write(
            path=str(self) or None,
            metadata=definition.storage_metadata,
            payload=payload,
            extension=extension,
            content_type=content_type,
            explicit_storage_path=explicit_s3_path,
            compress_payload=False,
        )

        return self._apply_new_value(stored_path)

    @asynccontextmanager
    async def get_contents(self, core: TStorageCore):
        definition = self._require_definition()
        async with core.storage_read(
            path=str(self) or None,
            metadata=definition.storage_metadata,
        ) as file:
            yield file

    async def get_content(self, core: TStorageCore) -> bytes:
        async with self.get_contents(core) as file:
            return file.read()

    #
    # Coerce from the string base class that's actually stored within the database
    # level varchar column into the CloudFile primitive.
    #

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

    def _require_definition(self) -> CloudFileFieldDefinition:
        if self._cloud_definition is None:
            raise ValueError(
                "CloudFile is not bound to a CloudFileField definition. "
                "Use it inside a model field declared with `CloudFileField(...)`."
            )
        return self._cloud_definition

    def _get_owner(self):
        if self._cloud_owner_ref is None:
            return None
        return self._cloud_owner_ref()

    def _clone_with_value(self, value: str) -> "CloudFile[TStorageCore]":
        definition = self._require_definition()
        cloned = type(self)(value)
        return cloned.bind(
            definition=definition,
            owner=self._get_owner(),
            field_name=self._cloud_field_name,
        )

    def _apply_new_value(self, value: str) -> "CloudFile[TStorageCore]":
        next_value = self._clone_with_value(value)

        owner = self._get_owner()
        if owner is not None and self._cloud_field_name is not None:
            setattr(owner, self._cloud_field_name, next_value)
            return cast(
                "CloudFile[TStorageCore]",
                getattr(owner, self._cloud_field_name),
            )

        return next_value

    @classmethod
    def _field_factory_name(cls) -> str:
        return "CloudFileField"


def CloudFileField(
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
    """
    Declare a `CloudFile[CoreType]` model field and attach its storage config.

    Runtime behavior:
    - `bucket`, `prefix`, and `suffix` define where generated object keys live.
      When a bound `CloudFile` uploads content, these values shape the final
      object path.
    - `compression` controls how bytes are encoded before upload and decoded on
      readback.
    - `storage_backend` controls whether temporary payload handling uses memory
      or disk while the provider runtime processes the file.
    - `compression_brotli_level` only affects Brotli compression quality when
      `compression=CompressionType.BROTLI`.
    - `default` and `default_factory` behave like normal Pydantic or Iceaxe
      field defaults for the stored string value.
    - `**kwargs` are forwarded to the underlying `Field(...)` or Iceaxe
      `Field(...)` call for normal schema and ORM configuration.
    """
    field_factory = Field

    if bucket is None:
        raise ValueError("CloudFileField requires a `bucket` value.")

    try:
        from iceaxe import Field as IceaxeField
        from iceaxe.sql_types import ColumnType
    except ImportError:
        pass
    else:
        field_factory = IceaxeField
        if "explicit_type" not in kwargs:
            kwargs["explicit_type"] = ColumnType.VARCHAR

    definition = CloudFileFieldDefinition(
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
