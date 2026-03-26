"""
AWS test fixtures for pytest using moto server.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator
from unittest.mock import patch

from mountaineer_cloud.test_utilities.mock_aws import MockAWS, MotoServerManager


@asynccontextmanager
async def get_mock_aws(
    whitelisted_buckets: list[str] | None = None,
) -> AsyncIterator[MockAWS]:
    """
    Main entry point for getting a MockAWS instance. This maintains backwards compatibility
    with the original interface while using moto under the hood.

    The whitelisted_buckets parameter is kept for backwards compatibility but is no longer used
    since moto handles all bucket operations safely.
    """
    server = MotoServerManager()

    try:
        url = await server.start_service()
        async with MockAWS.create(url) as mock_aws:
            # Create S3 buckets upfront so they'll be immediately available for tests.
            for bucket in whitelisted_buckets or []:
                await mock_aws.mock_s3.create_bucket(Bucket=bucket)

            # Disable docker-in-docker for moto services
            await server.configure_service(
                batch_use_docker=False,
                lambda_use_docker=False,
            )

            # Patch out the session return functions for the clients that call the session
            # creation manually
            with (
                patch("aioboto3.Session", return_value=mock_aws.session),
                patch("aioboto3.session.Session", return_value=mock_aws.session),
            ):
                yield mock_aws
    finally:
        server.stop_all()
