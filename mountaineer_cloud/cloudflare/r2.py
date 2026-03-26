from contextlib import asynccontextmanager
from uuid import uuid4

import aioboto3

from mountaineer_cloud.common.s3_compat import (
    S3CompatibleMetadataBase,
    S3CompatiblePointerBase,
)

from .config import CloudflareConfig


class R2Metadata(S3CompatibleMetadataBase):
    pass


class R2PointerMixin(S3CompatiblePointerBase[CloudflareConfig]):
    """
    Injects a single R2-backed object into a data model. We reference this
    object as the pointer since it will need to by hydrated by the R2 data
    in the remote location.

    """

    def make_url(
        self,
        *,
        extension: str,
        explicit_s3_path: str | None = None,
        config: CloudflareConfig,
    ) -> str:
        return (
            f"r2://{self.s3_object_metadata.key_bucket}/{self.s3_object_metadata.key_prefix}/{uuid4()}{extension}"
            if not explicit_s3_path
            else explicit_s3_path
        )

    @asynccontextmanager
    async def get_client(self, session: aioboto3.Session, config: CloudflareConfig):
        # R2 endpoint URL format: https://<account_id>.r2.cloudflarestorage.com
        endpoint_url = f"https://{config.R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

        async with session.client(
            "s3",
            endpoint_url=endpoint_url,
            # R2 doesn't use AWS regions, but boto3 requires one
            region_name="auto",
        ) as client:
            yield client
