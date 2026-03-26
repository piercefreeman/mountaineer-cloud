import gzip
from abc import ABC
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from enum import Enum
from io import BytesIO
from logging import error
from tempfile import TemporaryFile
from typing import IO, TYPE_CHECKING, Any, ClassVar, Generic, TypeVar
from urllib.parse import urlparse
from uuid import uuid4

import aioboto3
from botocore.exceptions import ClientError
from pydantic import BaseModel

from mountaineer_cloud.providers.base import ProviderCore

if TYPE_CHECKING:
    import brotli
    from types_aiobotocore_s3.client import S3Client
else:
    brotli = None

try:
    import brotli

    BROTLI_AVAILABLE = True
except ImportError:
    BROTLI_AVAILABLE = False


class CompressionType(Enum):
    RAW = "RAW"
    BROTLI = "BROTLI"
    GZIP = "GZIP"


class StorageBackendType(Enum):
    DISK = "DISK"
    MEMORY = "MEMORY"


COMPRESSION_TO_EXTENSION = {
    CompressionType.RAW: "",
    CompressionType.BROTLI: ".br",
    CompressionType.GZIP: ".gz",
}


def get_brotli():
    if BROTLI_AVAILABLE:
        return brotli
    raise ImportError("Brotli is not available. Install it with `pip install brotli`")


class S3CompatibleMetadataBase(BaseModel):
    pointer_compression: CompressionType = CompressionType.RAW
    pointer_storage_backend: StorageBackendType = StorageBackendType.MEMORY

    # If using brotli compression, override the default level of compression
    # to balance compression speed and compression ratio
    pointer_compression_brotli_level: int = 11

    # Object path prefix
    # If you need a dynamic override you can also make a @property of
    # your child class
    # The suffix is usually the type of file
    bucket: str
    prefix: str = ""
    suffix: str = ""


T = TypeVar("T")
TConfig = TypeVar("TConfig")


@dataclass
class S3SessionManager(Generic[T]):
    """
    Captures the provider-specific details for S3-compatible storage backends.
    Instantiate one per provider and delegate get_client/make_url to it.
    """

    url_scheme: str
    endpoint_url: Callable[[T], str] | None = None
    region_name: Callable[[T], str] | None = None

    @asynccontextmanager
    async def get_client(
        self, session: aioboto3.Session, config: T
    ) -> AsyncGenerator["S3Client", None]:
        kwargs: dict[str, Any] = {}
        if self.endpoint_url is not None:
            kwargs["endpoint_url"] = self.endpoint_url(config)
        if self.region_name is not None:
            kwargs["region_name"] = self.region_name(config)
        async with session.client("s3", **kwargs) as client:
            yield client

    def make_url(
        self,
        metadata: S3CompatibleMetadataBase,
        *,
        extension: str,
        explicit_s3_path: str | None = None,
    ) -> str:
        if explicit_s3_path:
            return explicit_s3_path
        return (
            f"{self.url_scheme}://{metadata.bucket}/"
            f"{metadata.prefix}/{uuid4()}{extension}"
        )


CloudSessionFactory = Callable[[T], Awaitable[aioboto3.Session]]


@dataclass(frozen=True)
class S3CompatibleBackend(Generic[T]):
    session_manager: S3SessionManager[T]
    session_factory: CloudSessionFactory[T]


CLOUD_BACKEND_REGISTRY: dict[type[Any], S3CompatibleBackend[Any]] = {}


def register_cloud_backend(
    core_type: type[T],
    *,
    session_manager: S3SessionManager[T],
    session_factory: CloudSessionFactory[T],
):
    CLOUD_BACKEND_REGISTRY[core_type] = S3CompatibleBackend(
        session_manager=session_manager,
        session_factory=session_factory,
    )


def resolve_cloud_backend(core_type: type[Any]) -> S3CompatibleBackend[Any]:
    for candidate_type in core_type.__mro__:
        if candidate_type in CLOUD_BACKEND_REGISTRY:
            return CLOUD_BACKEND_REGISTRY[candidate_type]

    raise ValueError(
        f"No cloud backend registered for {core_type.__name__}. "
        "Register it with `register_cloud_backend(...)`."
    )


class StorageProviderCore(ProviderCore[TConfig], Generic[TConfig], ABC):
    s3_session_manager: ClassVar[S3SessionManager[Any]]

    def make_storage_url(
        self,
        metadata: S3CompatibleMetadataBase,
        *,
        extension: str,
        explicit_s3_path: str | None = None,
    ) -> str:
        return self.s3_session_manager.make_url(
            metadata,
            extension=extension,
            explicit_s3_path=explicit_s3_path,
        )

    @asynccontextmanager
    async def get_storage_client(self):
        async with self.s3_session_manager.get_client(
            self.session, self.config
        ) as client:
            yield client


@dataclass(frozen=True)
class CloudRuntime(Generic[T]):
    session_manager: S3SessionManager[T]
    session: aioboto3.Session
    config: T


async def resolve_cloud_runtime(core: Any) -> CloudRuntime[Any]:
    if isinstance(core, StorageProviderCore):
        return CloudRuntime(
            session_manager=core.s3_session_manager,
            session=core.session,
            config=core.config,
        )

    backend = resolve_cloud_backend(type(core))
    return CloudRuntime(
        session_manager=backend.session_manager,
        session=await backend.session_factory(core),
        config=core,
    )


class S3CompatiblePointerBase(BaseModel, Generic[T], ABC):
    """
    Core class to implement S3-compatible pointer functionality. Multiple hosts
    provide the same interface, so this class is designed to be subclassed to
    provide the appropriate functionality for the specific host.
    """

    s3_object_path: str | None = None
    s3_object_metadata: ClassVar[S3CompatibleMetadataBase]
    s3_session_manager: ClassVar[S3SessionManager[Any]]

    def make_url(
        self, *, extension: str, explicit_s3_path: str | None = None, config: T
    ) -> str:
        return self.s3_session_manager.make_url(
            self.s3_object_metadata,
            extension=extension,
            explicit_s3_path=explicit_s3_path,
        )

    @asynccontextmanager
    async def get_client(
        self, session: aioboto3.Session, config: T
    ) -> AsyncGenerator["S3Client", None]:
        async with self.s3_session_manager.get_client(session, config) as client:
            yield client

    @asynccontextmanager
    async def get_contents_from_pointer(self, *, session: aioboto3.Session, config: T):
        if not self.s3_object_path:
            raise ValueError("S3 object not found")

        s3_parsed = urlparse(self.s3_object_path)

        async with self.get_client(session, config) as s3:
            try:
                raw_contents = await s3.get_object(
                    Bucket=s3_parsed.netloc,
                    Key=s3_parsed.path.strip("/"),
                )
                with self.get_output_io() as output_file:
                    buffer_size = 24 * 1024
                    while True:
                        chunk = await raw_contents["Body"].read(buffer_size)
                        if not chunk:
                            break
                        output_file.write(chunk)
                    output_file.seek(0)
                    with self.unwrap_compressed_file(output_file) as decompressed_file:
                        yield decompressed_file

            except ClientError as e:
                error(f"Error encountered when accessing {self.s3_object_path}: {e}")
                raise

    async def put_content_into_pointer(
        self,
        *,
        payload: IO[bytes],
        content_type: str | None = None,
        explicit_s3_path: str | None = None,
        session: aioboto3.Session,
        config: T,
    ):
        with self.wrap_compressed_file(payload) as compressed_payload:
            compressed_extension = (
                self.s3_object_metadata.suffix
                + COMPRESSION_TO_EXTENSION[self.s3_object_metadata.pointer_compression]
            )

            return await self.copy_content_into_pointer(
                payload=compressed_payload,
                extension=compressed_extension,
                content_type=content_type,
                explicit_s3_path=explicit_s3_path,
                session=session,
                config=config,
            )

    async def copy_content_into_pointer(
        self,
        *,
        payload: IO[bytes],
        extension: str,
        content_type: str | None = None,
        explicit_s3_path: str | None = None,
        session: aioboto3.Session,
        config: T,
    ):
        s3_metadata_path = self.make_url(
            extension=extension, explicit_s3_path=explicit_s3_path, config=config
        )
        s3_parsed = urlparse(s3_metadata_path)

        optional_args = (
            {"ExtraArgs": {"ContentType": content_type}} if content_type else {}
        )

        async with self.get_client(session, config) as s3:
            try:
                await s3.upload_fileobj(
                    Bucket=s3_parsed.netloc,
                    Key=s3_parsed.path.strip("/"),
                    Fileobj=payload,
                    **optional_args,  # type: ignore
                )
            except ClientError as e:
                error(f"Error encountered when accessing {s3_metadata_path}: {e}")
                raise

        self.s3_object_path = s3_metadata_path

    @contextmanager
    def wrap_compressed_file(self, file: IO[bytes], buffer_size=24 * 1024):
        if self.s3_object_metadata.pointer_compression == CompressionType.RAW:
            yield file
        elif self.s3_object_metadata.pointer_compression == CompressionType.BROTLI:
            compressor = get_brotli().Compressor(
                quality=self.s3_object_metadata.pointer_compression_brotli_level,
            )
            with self.get_output_io() as output_file:
                while True:
                    chunk = file.read(buffer_size)
                    if not chunk:
                        break
                    output_file.write(compressor.process(chunk))
                output_file.write(compressor.finish())
                output_file.seek(0)
                yield output_file
        elif self.s3_object_metadata.pointer_compression == CompressionType.GZIP:
            with self.get_output_io() as output_file:
                with gzip.GzipFile(fileobj=output_file, mode="wb") as compressor:
                    while True:
                        chunk = file.read(buffer_size)
                        if not chunk:
                            break
                        compressor.write(chunk)
                output_file.seek(0)
                yield output_file
        else:
            raise ValueError(
                f"Unknown compression type {self.s3_object_metadata.pointer_compression}"
            )

    @contextmanager
    def unwrap_compressed_file(self, file: IO[bytes], buffer_size=24 * 1024):
        if self.s3_object_metadata.pointer_compression == CompressionType.RAW:
            yield file
        elif self.s3_object_metadata.pointer_compression == CompressionType.BROTLI:
            decompressor = get_brotli().Decompressor()
            with self.get_output_io() as output_file:
                while True:
                    chunk = file.read(buffer_size)
                    if not chunk:
                        break
                    output_file.write(decompressor.process(chunk))
                output_file.seek(0)
                yield output_file
        elif self.s3_object_metadata.pointer_compression == CompressionType.GZIP:
            with self.get_output_io() as output_file:
                with gzip.GzipFile(fileobj=file, mode="rb") as decompressor:
                    while True:
                        chunk = decompressor.read(buffer_size)
                        if not chunk:
                            break
                        output_file.write(chunk)
                output_file.seek(0)
                yield output_file
        else:
            raise ValueError(
                f"Unknown compression type {self.s3_object_metadata.pointer_compression}"
            )

    @contextmanager
    def get_output_io(self):
        if self.s3_object_metadata.pointer_storage_backend == StorageBackendType.DISK:
            with TemporaryFile() as output_file:
                yield output_file
        elif (
            self.s3_object_metadata.pointer_storage_backend == StorageBackendType.MEMORY
        ):
            yield BytesIO()
        else:
            raise ValueError(
                f"Unknown storage backend {self.s3_object_metadata.pointer_storage_backend}"
            )
