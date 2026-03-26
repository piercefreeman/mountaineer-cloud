import gzip
from abc import ABC
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from logging import error
from tempfile import TemporaryFile
from typing import IO, TYPE_CHECKING, Any, ClassVar, Generic, TypeVar
from urllib.parse import urlparse
from uuid import uuid4

import aioboto3
import botocore.loaders
from botocore.exceptions import ClientError

from mountaineer.cache import AsyncLoopObjectCache

from mountaineer_cloud.providers.base import ProviderCore
from mountaineer_cloud.providers_common.storage import (
    CompressionType,
    StorageBackendType,
    StorageMetadata,
    StorageProviderCore,
)

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client


COMPRESSION_TO_EXTENSION = {
    CompressionType.RAW: "",
    CompressionType.BROTLI: ".br",
    CompressionType.GZIP: ".gz",
}


def get_brotli():
    try:
        import brotli
    except ImportError as exc:
        raise ImportError(
            "Brotli is not available. Install it with `pip install brotli`"
        ) from exc
    else:
        return brotli


class S3CompatibleMetadataBase(StorageMetadata):
    pass


TConfig = TypeVar("TConfig")
TSession = TypeVar("TSession")
TProviderCore = TypeVar("TProviderCore", bound=ProviderCore[Any])

SessionMetadata = tuple[aioboto3.Session, datetime]
SessionBuilder = Callable[[], Awaitable[SessionMetadata]]


# Global loader for a central cache of botocore metadata. Workaround for the
# ~20MB memory allocation associated with JSONDecoder objects that is locked to
# each session. Bug: https://github.com/boto/botocore/issues/3078
BOTOCORE_LOADER = botocore.loaders.Loader()


def is_session_valid(expiration: datetime | None) -> bool:
    current_time = datetime.now(timezone.utc)
    return expiration is not None and current_time < expiration - timedelta(minutes=5)


def build_s3_session_expiration(
    *, lifetime: timedelta = timedelta(hours=23)
) -> datetime:
    return datetime.now(timezone.utc) + lifetime


def create_s3_session(
    *,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_session_token: str | None = None,
    region_name: str | None = None,
) -> aioboto3.Session:
    if aws_session_token is not None and region_name is not None:
        session = aioboto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name,
        )
    elif aws_session_token is not None:
        session = aioboto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )
    elif region_name is not None:
        session = aioboto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
    else:
        session = aioboto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
    session._session.register_component("data_loader", BOTOCORE_LOADER)
    return session


async def get_cached_s3_session(
    session_cache: AsyncLoopObjectCache[SessionMetadata],
    *,
    session_builder: SessionBuilder,
) -> aioboto3.Session:
    existing_metadata = session_cache.get_obj()
    if existing_metadata:
        session, expiration = existing_metadata
        if is_session_valid(expiration):
            return session

    async with session_cache.get_lock():
        existing_metadata = session_cache.get_obj()
        if existing_metadata:
            session, expiration = existing_metadata
            if is_session_valid(expiration):
                return session

        session, expiration = await session_builder()
        session_cache.set_obj((session, expiration))
        return session


async def provider_core_dependency(
    *,
    build_core: Callable[[], Awaitable[TProviderCore]],
) -> AsyncGenerator[TProviderCore, None]:
    core = await build_core()
    try:
        yield core
    finally:
        await core.aclose()


@dataclass
class S3SessionManager(Generic[TSession]):
    """
    Captures the provider-specific details for S3-compatible storage backends.
    Instantiate one per provider and delegate get_client/make_url to it.
    """

    url_scheme: str
    endpoint_url: Callable[[TSession], str] | None = None
    region_name: Callable[[TSession], str] | None = None

    @asynccontextmanager
    async def get_client(
        self, session: aioboto3.Session, config: TSession
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
        metadata: StorageMetadata,
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


class S3CompatibleStorageCore(StorageProviderCore[TConfig], Generic[TConfig], ABC):
    s3_session_manager: ClassVar[S3SessionManager[Any]]

    def make_storage_url(
        self,
        metadata: StorageMetadata,
        *,
        extension: str,
        explicit_storage_path: str | None = None,
    ) -> str:
        return self.s3_session_manager.make_url(
            metadata,
            extension=extension,
            explicit_s3_path=explicit_storage_path,
        )

    @asynccontextmanager
    async def get_storage_client(self) -> AsyncGenerator["S3Client", None]:
        async with self.s3_session_manager.get_client(
            self.session, self.config
        ) as client:
            yield client

    @asynccontextmanager
    async def storage_read(
        self,
        *,
        path: str | None,
        metadata: StorageMetadata,
    ) -> AsyncGenerator[IO[bytes], None]:
        s3_metadata = _coerce_s3_metadata(metadata)

        if not path:
            raise ValueError("S3 object not found")

        s3_parsed = urlparse(path)

        async with self.get_storage_client() as s3:
            try:
                raw_contents = await s3.get_object(
                    Bucket=s3_parsed.netloc,
                    Key=s3_parsed.path.strip("/"),
                )
                with self._get_output_io(s3_metadata) as output_file:
                    buffer_size = 24 * 1024
                    while True:
                        chunk = await raw_contents["Body"].read(buffer_size)
                        if not chunk:
                            break
                        output_file.write(chunk)
                    output_file.seek(0)
                    with self._unwrap_compressed_file(
                        output_file,
                        s3_metadata,
                    ) as decompressed_file:
                        yield decompressed_file

            except ClientError as e:
                error(f"Error encountered when accessing {path}: {e}")
                raise

    async def storage_write(
        self,
        *,
        path: str | None,
        metadata: StorageMetadata,
        payload: IO[bytes],
        content_type: str | None = None,
        explicit_storage_path: str | None = None,
        extension: str | None = None,
        compress_payload: bool = True,
    ) -> str:
        del path

        s3_metadata = _coerce_s3_metadata(metadata)

        if compress_payload:
            with self._wrap_compressed_file(payload, s3_metadata) as compressed_payload:
                compressed_extension = (
                    s3_metadata.suffix
                    + COMPRESSION_TO_EXTENSION[s3_metadata.pointer_compression]
                )
                return await self._upload_storage_payload(
                    payload=compressed_payload,
                    metadata=s3_metadata,
                    extension=compressed_extension,
                    content_type=content_type,
                    explicit_storage_path=explicit_storage_path,
                )

        if extension is None:
            raise ValueError(
                "storage_write requires an `extension` when `compress_payload=False`."
            )

        return await self._upload_storage_payload(
            payload=payload,
            metadata=s3_metadata,
            extension=extension,
            content_type=content_type,
            explicit_storage_path=explicit_storage_path,
        )

    async def _upload_storage_payload(
        self,
        *,
        payload: IO[bytes],
        metadata: S3CompatibleMetadataBase,
        extension: str,
        content_type: str | None = None,
        explicit_storage_path: str | None = None,
    ) -> str:
        s3_metadata_path = self.make_storage_url(
            metadata,
            extension=extension,
            explicit_storage_path=explicit_storage_path,
        )
        s3_parsed = urlparse(s3_metadata_path)

        async with self.get_storage_client() as s3:
            try:
                if content_type:
                    await s3.upload_fileobj(
                        Bucket=s3_parsed.netloc,
                        Key=s3_parsed.path.strip("/"),
                        Fileobj=payload,
                        ExtraArgs={"ContentType": content_type},
                    )
                else:
                    await s3.upload_fileobj(
                        Bucket=s3_parsed.netloc,
                        Key=s3_parsed.path.strip("/"),
                        Fileobj=payload,
                    )
            except ClientError as e:
                error(f"Error encountered when accessing {s3_metadata_path}: {e}")
                raise

        return s3_metadata_path

    @contextmanager
    def _wrap_compressed_file(
        self,
        file: IO[bytes],
        metadata: S3CompatibleMetadataBase,
        buffer_size: int = 24 * 1024,
    ):
        if metadata.pointer_compression == CompressionType.RAW:
            yield file
        elif metadata.pointer_compression == CompressionType.BROTLI:
            compressor = get_brotli().Compressor(
                quality=metadata.pointer_compression_brotli_level,
            )
            with self._get_output_io(metadata) as output_file:
                while True:
                    chunk = file.read(buffer_size)
                    if not chunk:
                        break
                    output_file.write(compressor.process(chunk))
                output_file.write(compressor.finish())
                output_file.seek(0)
                yield output_file
        elif metadata.pointer_compression == CompressionType.GZIP:
            with self._get_output_io(metadata) as output_file:
                with gzip.GzipFile(fileobj=output_file, mode="wb") as compressor:
                    while True:
                        chunk = file.read(buffer_size)
                        if not chunk:
                            break
                        compressor.write(chunk)
                output_file.seek(0)
                yield output_file
        else:
            raise ValueError(f"Unknown compression type {metadata.pointer_compression}")

    @contextmanager
    def _unwrap_compressed_file(
        self,
        file: IO[bytes],
        metadata: S3CompatibleMetadataBase,
        buffer_size: int = 24 * 1024,
    ):
        if metadata.pointer_compression == CompressionType.RAW:
            yield file
        elif metadata.pointer_compression == CompressionType.BROTLI:
            decompressor = get_brotli().Decompressor()
            with self._get_output_io(metadata) as output_file:
                while True:
                    chunk = file.read(buffer_size)
                    if not chunk:
                        break
                    output_file.write(decompressor.process(chunk))
                output_file.seek(0)
                yield output_file
        elif metadata.pointer_compression == CompressionType.GZIP:
            with self._get_output_io(metadata) as output_file:
                with gzip.GzipFile(fileobj=file, mode="rb") as decompressor:
                    while True:
                        chunk = decompressor.read(buffer_size)
                        if not chunk:
                            break
                        output_file.write(chunk)
                output_file.seek(0)
                yield output_file
        else:
            raise ValueError(f"Unknown compression type {metadata.pointer_compression}")

    @contextmanager
    def _get_output_io(self, metadata: S3CompatibleMetadataBase):
        if metadata.pointer_storage_backend == StorageBackendType.DISK:
            with TemporaryFile() as output_file:
                yield output_file
        elif metadata.pointer_storage_backend == StorageBackendType.MEMORY:
            yield BytesIO()
        else:
            raise ValueError(
                f"Unknown storage backend {metadata.pointer_storage_backend}"
            )


def _coerce_s3_metadata(metadata: StorageMetadata) -> S3CompatibleMetadataBase:
    if isinstance(metadata, S3CompatibleMetadataBase):
        return metadata

    return S3CompatibleMetadataBase.model_validate(metadata.model_dump(mode="json"))
