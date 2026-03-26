from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager
from enum import Enum
from typing import IO, Generic, TypeVar

from pydantic import BaseModel

from mountaineer_cloud.providers.base import ProviderCore

TConfig = TypeVar("TConfig")


class CompressionType(Enum):
    RAW = "RAW"
    BROTLI = "BROTLI"
    GZIP = "GZIP"


class StorageBackendType(Enum):
    DISK = "DISK"
    MEMORY = "MEMORY"


class StorageMetadata(BaseModel):
    pointer_compression: CompressionType = CompressionType.RAW
    pointer_storage_backend: StorageBackendType = StorageBackendType.MEMORY

    # If using brotli compression, override the default level of compression
    # to balance compression speed and compression ratio.
    pointer_compression_brotli_level: int = 11

    bucket: str
    prefix: str = ""
    suffix: str = ""


class StorageProviderCore(ProviderCore[TConfig], Generic[TConfig], ABC):
    """
    Contract for provider cores that can read and write object storage payloads.

    Primitives such as `CloudFile[...]` depend on this interface instead of
    reaching into provider-specific pointer implementations directly.
    """

    @abstractmethod
    def storage_read(
        self,
        *,
        path: str | None,
        metadata: StorageMetadata,
    ) -> AbstractAsyncContextManager[IO[bytes]]:
        """
        Return an async context manager that yields the stored payload.
        """

    @abstractmethod
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
        """
        Persist payload data and return the resulting storage path.
        """
