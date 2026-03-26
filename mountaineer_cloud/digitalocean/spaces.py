from contextlib import asynccontextmanager
from uuid import uuid4

import aioboto3

from mountaineer_cloud.common.s3_compat import (
    S3CompatibleMetadataBase,
    S3CompatiblePointerBase,
)

from .config import DigitalOceanConfig


class SpacesMetadata(S3CompatibleMetadataBase):
    pass


class SpacesPointerMixin(S3CompatiblePointerBase[DigitalOceanConfig]):
    """
    Injects a single DigitalOcean Spaces-backed object into a data model. We reference this
    object as the pointer since it will need to be hydrated by the Spaces data
    in the remote location.

    """

    def make_url(
        self,
        *,
        extension: str,
        explicit_s3_path: str | None = None,
        config: DigitalOceanConfig,
    ) -> str:
        return (
            f"spaces://{self.s3_object_metadata.key_bucket}/{self.s3_object_metadata.key_prefix}/{uuid4()}{extension}"
            if not explicit_s3_path
            else explicit_s3_path
        )

    @asynccontextmanager
    async def get_client(self, session: aioboto3.Session, config: DigitalOceanConfig):
        # DigitalOcean Spaces endpoint URL format: https://<region>.digitaloceanspaces.com
        endpoint_url = f"https://{config.SPACES_REGION}.digitaloceanspaces.com"

        async with session.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=config.SPACES_REGION,
        ) as client:
            yield client
