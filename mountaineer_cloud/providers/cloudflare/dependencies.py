"""
Utilities to handle authentication and session management for Cloudflare R2.
"""

from datetime import datetime

import aioboto3
from fastapi import Depends

from mountaineer import CoreDependencies
from mountaineer.cache import AsyncLoopObjectCache

from mountaineer_cloud.providers.base import provider_core_dependency
from mountaineer_cloud.providers_common.s3_compat import (
    build_s3_session_expiration,
    create_s3_session,
    get_cached_s3_session,
    is_session_valid as shared_is_session_valid,
)

from .config import CloudflareConfig
from .core import CloudflareCore

GLOBAL_SESSIONS = AsyncLoopObjectCache[tuple[aioboto3.Session, datetime]]()
is_session_valid = shared_is_session_valid


async def _build_r2_session(
    config: CloudflareConfig,
) -> tuple[aioboto3.Session, datetime]:
    session = create_s3_session(
        aws_access_key_id=config.R2_ACCESS_KEY_ID,
        aws_secret_access_key=config.R2_SECRET_ACCESS_KEY,
    )
    return session, build_s3_session_expiration()


async def get_r2_session(
    config: CloudflareConfig = Depends(
        CoreDependencies.get_config_with_type(CloudflareConfig)
    ),
) -> aioboto3.Session:
    return await get_cached_s3_session(
        GLOBAL_SESSIONS,
        session_builder=lambda: _build_r2_session(config),
    )


async def build_cloudflare_core(config: CloudflareConfig) -> CloudflareCore:
    return CloudflareCore(
        config=config,
        session=await get_r2_session(config),
    )


async def get_cloudflare_core(
    config: CloudflareConfig = Depends(
        CoreDependencies.get_config_with_type(CloudflareConfig)
    ),
):
    async for core in provider_core_dependency(
        build_core=lambda: build_cloudflare_core(config),
    ):
        yield core
