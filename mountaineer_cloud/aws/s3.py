from mountaineer_cloud.common.s3_compat import (
    S3CompatibleMetadataBase,
    S3CompatiblePointerBase,
    S3SessionManager,
)

from .config import AWSConfig


class S3Metadata(S3CompatibleMetadataBase):
    pass


_session_manager = S3SessionManager[AWSConfig](url_scheme="s3")


class S3PointerMixin(S3CompatiblePointerBase[AWSConfig]):
    """
    Injects a single S3-backed object into a data model. We reference this
    object as the pointer since it will need to by hydrated by the S3 data
    in the remote location.

    """

    s3_session_manager = _session_manager
