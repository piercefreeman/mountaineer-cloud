from dataclasses import dataclass

from mountaineer_cloud.providers_common.s3_compat import (
    S3CompatibleStorageCore,
    S3SessionManager,
)

from .config import DigitalOceanConfig

_session_manager = S3SessionManager[DigitalOceanConfig](
    url_scheme="spaces",
    endpoint_url=lambda c: f"https://{c.SPACES_REGION}.digitaloceanspaces.com",
    region_name=lambda c: c.SPACES_REGION,
)


@dataclass
class DigitalOceanCore(S3CompatibleStorageCore[DigitalOceanConfig]):
    s3_session_manager = _session_manager
