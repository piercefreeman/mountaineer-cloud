"""
Utilities to handle authentication and session management for Cloudflare R2.
"""

from datetime import datetime, timedelta, timezone

import aioboto3
import botocore.loaders
from fastapi import Depends

from mountaineer import CoreDependencies
from mountaineer.cache import AsyncLoopObjectCache

from mountaineer_cloud.providers_common.s3_compat import register_cloud_backend

from .config import CloudflareConfig
from .core import CloudflareCore, _session_manager

GLOBAL_SESSIONS = AsyncLoopObjectCache[tuple[aioboto3.Session, datetime]]()

# Global loader for a central cache of botocore metadata. Workaround for the ~20MB memory
# allocation associated with JSONDecoder objects that is locked to each session.
# Bug: https://github.com/boto/botocore/issues/3078
BOTOCORE_LOADER = botocore.loaders.Loader()


async def get_r2_session(
    config: CloudflareConfig = Depends(
        CoreDependencies.get_config_with_type(CloudflareConfig)
    ),
) -> aioboto3.Session:
    existing_metadata = GLOBAL_SESSIONS.get_obj()
    if existing_metadata:
        session, expiration = existing_metadata
        if is_session_valid(expiration):
            return session

    async with GLOBAL_SESSIONS.get_lock():
        existing_metadata = GLOBAL_SESSIONS.get_obj()
        if existing_metadata:
            session, expiration = existing_metadata
            if is_session_valid(expiration):
                return session

        session = aioboto3.Session(
            aws_access_key_id=config.R2_ACCESS_KEY_ID,
            aws_secret_access_key=config.R2_SECRET_ACCESS_KEY,
        )
        session._session.register_component("data_loader", BOTOCORE_LOADER)

        session_expiration = datetime.now(timezone.utc) + timedelta(hours=23)
        GLOBAL_SESSIONS.set_obj((session, session_expiration))
        return session


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
    core = await build_cloudflare_core(config)
    try:
        yield core
    finally:
        await core.aclose()


def is_session_valid(expiration: datetime | None) -> bool:
    current_time = datetime.now(timezone.utc)
    return expiration is not None and current_time < expiration - timedelta(minutes=5)


register_cloud_backend(
    CloudflareConfig,
    session_manager=_session_manager,
    session_factory=get_r2_session,
)
