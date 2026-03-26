from mountaineer_cloud.common.s3_compat import (
    S3CompatibleMetadataBase,
    S3CompatiblePointerBase,
    S3SessionManager,
)

from .config import CloudflareConfig


class R2Metadata(S3CompatibleMetadataBase):
    pass


_session_manager = S3SessionManager[CloudflareConfig](
    url_scheme="r2",
    endpoint_url=lambda c: f"https://{c.R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    region_name=lambda _: "auto",
)


class R2PointerMixin(S3CompatiblePointerBase[CloudflareConfig]):
    """
    Injects a single R2-backed object into a data model. We reference this
    object as the pointer since it will need to by hydrated by the R2 data
    in the remote location.

    """

    s3_session_manager = _session_manager
