from ..common.s3_compat import (
    CompressionType as CompressionType,
    StorageBackendType as StorageBackendType,
)
from . import dependencies as AWSDependencies  # noqa: F401
from .config import AWSConfig as AWSConfig
from .s3 import (
    S3Metadata as S3Metadata,
    S3PointerMixin as S3PointerMixin,
)
