from contextlib import asynccontextmanager
from uuid import uuid4

import aioboto3

from mountaineer_cloud.common.s3_compat import (
    S3CompatibleMetadataBase,
    S3CompatiblePointerBase,
)

from .config import AWSConfig


class S3Metadata(S3CompatibleMetadataBase):
    pass


class S3PointerMixin(S3CompatiblePointerBase[AWSConfig]):
    """
    Injects a single S3-backed object into a data model. We reference this
    object as the pointer since it will need to by hydrated by the S3 data
    in the remote location.

    """

    def make_url(
        self, *, extension: str, explicit_s3_path: str | None = None, config: AWSConfig
    ) -> str:
        return (
            f"s3://{self.s3_object_metadata.key_bucket}/{self.s3_object_metadata.key_prefix}/{uuid4()}{extension}"
            if not explicit_s3_path
            else explicit_s3_path
        )

    @asynccontextmanager
    async def get_client(self, session: aioboto3.Session, config: AWSConfig):
        async with session.client("s3") as client:
            yield client
