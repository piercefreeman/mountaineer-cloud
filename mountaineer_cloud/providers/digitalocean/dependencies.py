"""
Utilities to handle authentication and session management for DigitalOcean Spaces.
"""

from datetime import datetime

import aioboto3
from fastapi import Depends

from mountaineer import CoreDependencies
from mountaineer.cache import AsyncLoopObjectCache

from mountaineer_cloud.providers_common.s3_compat import (
    build_s3_session_expiration,
    create_s3_session,
    get_cached_s3_session,
    is_session_valid as shared_is_session_valid,
    provider_core_dependency,
)

from .config import DigitalOceanConfig
from .core import DigitalOceanCore

GLOBAL_SESSIONS = AsyncLoopObjectCache[tuple[aioboto3.Session, datetime]]()
is_session_valid = shared_is_session_valid


async def _build_spaces_session(
    config: DigitalOceanConfig,
) -> tuple[aioboto3.Session, datetime]:
    session = create_s3_session(
        aws_access_key_id=config.SPACES_ACCESS_KEY_ID,
        aws_secret_access_key=config.SPACES_SECRET_ACCESS_KEY,
    )
    return session, build_s3_session_expiration()


async def get_spaces_session(
    config: DigitalOceanConfig = Depends(
        CoreDependencies.get_config_with_type(DigitalOceanConfig)
    ),
) -> aioboto3.Session:
    return await get_cached_s3_session(
        GLOBAL_SESSIONS,
        session_builder=lambda: _build_spaces_session(config),
    )


async def build_digitalocean_core(config: DigitalOceanConfig) -> DigitalOceanCore:
    return DigitalOceanCore(
        config=config,
        session=await get_spaces_session(config),
    )


async def get_digitalocean_core(
    config: DigitalOceanConfig = Depends(
        CoreDependencies.get_config_with_type(DigitalOceanConfig)
    ),
):
    async for core in provider_core_dependency(
        build_core=lambda: build_digitalocean_core(config),
    ):
        yield core
