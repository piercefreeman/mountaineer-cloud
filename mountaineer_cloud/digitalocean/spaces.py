from mountaineer_cloud.common.s3_compat import (
    S3CompatibleMetadataBase,
    S3CompatiblePointerBase,
    S3SessionManager,
)

from .config import DigitalOceanConfig


class SpacesMetadata(S3CompatibleMetadataBase):
    pass


_session_manager = S3SessionManager[DigitalOceanConfig](
    url_scheme="spaces",
    endpoint_url=lambda c: f"https://{c.SPACES_REGION}.digitaloceanspaces.com",
    region_name=lambda c: c.SPACES_REGION,
)


class SpacesPointerMixin(S3CompatiblePointerBase[DigitalOceanConfig]):
    """
    Injects a single DigitalOcean Spaces-backed object into a data model. We reference this
    object as the pointer since it will need to be hydrated by the Spaces data
    in the remote location.

    """

    s3_session_manager = _session_manager
