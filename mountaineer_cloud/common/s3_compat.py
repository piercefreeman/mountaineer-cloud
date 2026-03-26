import gzip
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, contextmanager
from enum import Enum
from io import BytesIO
from logging import error
from tempfile import TemporaryFile
from typing import (
    IO,
    TYPE_CHECKING,
    AsyncGenerator,
    ClassVar,
    Generic,
    TypeVar,
)
from urllib.parse import urlparse

import aioboto3
from botocore.exceptions import ClientError
from pydantic import BaseModel
from pydantic_settings import BaseSettings

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
    else:
        raise ImportError(
            "Brotli is not available. Install it with `pip install brotli`"
        )


class S3CompatibleMetadataBase(BaseModel):
    pointer_compression: CompressionType = CompressionType.RAW
    pointer_storage_backend: StorageBackendType = StorageBackendType.MEMORY

    # If using brotli compression, override the default level of compression
    # to balance compression speed and compression ratio
    pointer_compression_brotli_level: int = 11

    # Key prefix
    # If you need a dynamic override you can also make a @property of
    # your child class
    # The key suffix is usually the type of file
    key_bucket: str
    key_prefix: str = ""
    key_suffix: str = ""


T = TypeVar("T", bound=BaseSettings)


class S3CompatiblePointerBase(BaseModel, Generic[T], ABC):
    """
    Core class to implement S3-compatible pointer functionality. Multiple hosts (S3, B2, R2) provide
    the same interface, so this class is designed to be subclassed to provide the appropriate
    functionality for the specific host.

    """

    s3_object_path: str | None = None
    s3_object_metadata: ClassVar[S3CompatibleMetadataBase]

    @abstractmethod
    def make_url(
        self, *, extension: str, explicit_s3_path: str | None = None, config: T
    ) -> str: ...

    @abstractmethod
    @asynccontextmanager
    async def get_client(
        self, session: aioboto3.Session, config: T
    ) -> AsyncGenerator["S3Client", None]: ...

    @asynccontextmanager
    async def get_contents_from_pointer(self, *, session: aioboto3.Session, config: T):
        """
        Gets the contents of the current s3_object_path, if set.

        Internally we will stream data back from the server, to allow for file-based
        buffering if users want to avoid loading everything into memory.

        """
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
                    # While strictly speaking the in-memory backend approaches could
                    # read the whole blob into memory at once, to keep the logic
                    # simpler we do a streaming read here to avoid bringing everything
                    # into memory for the disk backend.
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
        """

        :param content_type: Used by S3 if serving the file from the content
            bucket. This lets browser assume the right MIME type of the
            displayed content.
        :param explicit_s3_path: In some cases clients may need to override
            the generation of the S3 path to enforce object-conditioned logic.
            If this is provided, we will ignore the key_prefix, key_suffix,
            and automatic compression extension. You're all on your own.

        """
        # We assume that we should follow the store-based preferences for both
        # the compression and the backend type
        with self.wrap_compressed_file(payload) as compressed_payload:
            # Guess the additional suffix that is required based on the extension
            # and the compression type
            compressed_extension = (
                self.s3_object_metadata.key_suffix
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
        """
        Puts the raw content into the S3 bucket. The only time clients should
        call this manually is if you need to perform your compression manually.
        Otherwise we have better default handling in `put_content_into_pointer`.

        """
        s3_metadata_path = self.make_url(
            extension=extension, explicit_s3_path=explicit_s3_path, config=config
        )
        s3_parsed = urlparse(s3_metadata_path)

        optional_args = (
            {
                "ExtraArgs": {
                    # Allowed parameters defined by boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS
                    # Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS
                    "ContentType": content_type,
                }
            }
            if content_type
            else {}
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
