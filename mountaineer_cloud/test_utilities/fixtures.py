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
    Main entry point for getting a MockAWS instance backed by moto.

    If provided, `whitelisted_buckets` are created up front so tests can rely on
    those buckets existing immediately.
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
