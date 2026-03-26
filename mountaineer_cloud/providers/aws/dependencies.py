"""
Utilities to control permission grants for AWS roles.
"""

from datetime import datetime, timezone

import aioboto3
from fastapi import Depends

from mountaineer import CoreDependencies
from mountaineer.cache import AsyncLoopObjectCache

from mountaineer_cloud.providers.base import provider_core_dependency
from mountaineer_cloud.providers_common.s3_compat import (
    create_s3_session,
    get_cached_s3_session,
    is_session_valid as shared_is_session_valid,
)

from .config import AWSConfig
from .core import AWSCore

GLOBAL_SESSIONS = AsyncLoopObjectCache[tuple[aioboto3.Session, datetime]]()
is_session_valid = shared_is_session_valid


async def _build_aws_session(config: AWSConfig) -> tuple[aioboto3.Session, datetime]:
    initial_session = create_s3_session(
        aws_access_key_id=config.AWS_ACCESS_KEY,
        aws_secret_access_key=config.AWS_SECRET_KEY,
        region_name=config.AWS_REGION_NAME,
    )

    async with initial_session.client("sts") as sts_client:
        assumed_role = await sts_client.assume_role(
            RoleArn=config.AWS_ROLE_ARN,
            RoleSessionName=config.AWS_ROLE_SESSION_NAME,
        )

    session_expiration = datetime.fromtimestamp(
        assumed_role["Credentials"]["Expiration"].timestamp(), timezone.utc
    )

    session = create_s3_session(
        aws_access_key_id=assumed_role["Credentials"]["AccessKeyId"],
        aws_secret_access_key=assumed_role["Credentials"]["SecretAccessKey"],
        aws_session_token=assumed_role["Credentials"]["SessionToken"],
        region_name=config.AWS_REGION_NAME,
    )
    return session, session_expiration


async def get_aws_session(
    config: AWSConfig = Depends(CoreDependencies.get_config_with_type(AWSConfig)),
) -> aioboto3.Session:
    """
    Assumes specific credentials given the default assumable role in the settings.
    By convention, we assume that backend services will only want one valid sessions
    to AWS at a time. We cache it globally until expiration to save us from having
    to do the round-trip of reauthentication on each request.
    """
    return await get_cached_s3_session(
        GLOBAL_SESSIONS,
        session_builder=lambda: _build_aws_session(config),
    )


async def build_aws_core(config: AWSConfig) -> AWSCore:
    return AWSCore(
        config=config,
        session=await get_aws_session(config),
    )


async def get_aws_core(
    config: AWSConfig = Depends(CoreDependencies.get_config_with_type(AWSConfig)),
):
    async for core in provider_core_dependency(
        build_core=lambda: build_aws_core(config),
    ):
        yield core
