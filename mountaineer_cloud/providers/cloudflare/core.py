from dataclasses import dataclass

from mountaineer_cloud.providers_common.s3_compat import (
    S3CompatibleStorageCore,
    S3SessionManager,
)

from .config import CloudflareConfig

_session_manager = S3SessionManager[CloudflareConfig](
    url_scheme="r2",
    endpoint_url=lambda c: f"https://{c.R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    region_name=lambda _: "auto",
)


@dataclass
class CloudflareCore(S3CompatibleStorageCore[CloudflareConfig]):
    s3_session_manager = _session_manager
