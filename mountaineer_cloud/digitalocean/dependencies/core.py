"""
Utilities to handle authentication and session management for DigitalOcean Spaces.
"""

from datetime import datetime, timedelta, timezone
from typing import Tuple

import aioboto3
import botocore.loaders
from fastapi import Depends

from mountaineer import CoreDependencies
from mountaineer.cache import AsyncLoopObjectCache

from mountaineer_cloud.digitalocean.config import DigitalOceanConfig

# Global cache for Spaces sessions
GLOBAL_SESSIONS = AsyncLoopObjectCache[Tuple[aioboto3.Session, datetime]]()

# Global loader for a central cache of botocore metadata. Workaround for the ~20MB memory
# allocation associated with JSONDecoder objects that is locked to each session.
# Bug: https://github.com/boto/botocore/issues/3078
BOTOCORE_LOADER = botocore.loaders.Loader()


async def get_spaces_session(
    config: DigitalOceanConfig = Depends(
        CoreDependencies.get_config_with_type(DigitalOceanConfig)
    ),
) -> aioboto3.Session:
    """
    Creates an authenticated aioboto3 session configured for DigitalOcean Spaces.
    Caches the session until expiration to avoid repeated authentication.

    Returns:
        An authenticated aioboto3 Session configured for Spaces
    """
    # First, non-blocking check for session validity
    existing_metadata = GLOBAL_SESSIONS.get_obj()
    if existing_metadata:
        session, expiration = existing_metadata
        if is_session_valid(expiration):
            return session

    async with GLOBAL_SESSIONS.get_lock():
        # Re-check the session validity in case it got updated while
        # waiting for the lock
        existing_metadata = GLOBAL_SESSIONS.get_obj()
        if existing_metadata:
            session, expiration = existing_metadata
            if is_session_valid(expiration):
                return session

        # Create a new session with Spaces-specific configuration
        session = aioboto3.Session(
            aws_access_key_id=config.SPACES_ACCESS_KEY_ID,
            aws_secret_access_key=config.SPACES_SECRET_ACCESS_KEY,
        )
        session._session.register_component("data_loader", BOTOCORE_LOADER)

        # Spaces sessions don't expire in the same way as AWS sessions
        # Setting a conservative expiration of 23 hours for cache management
        session_expiration = datetime.now(timezone.utc) + timedelta(hours=23)

        GLOBAL_SESSIONS.set_obj((session, session_expiration))
        return session


def is_session_valid(expiration: datetime | None) -> bool:
    """
    Check if the session is still valid.
    We consider a session invalid if it's within 5 minutes of expiration.

    Args:
        expiration: The expiration datetime of the session

    Returns:
        True if the session is still valid, False otherwise
    """
    current_time = datetime.now(timezone.utc)
    return expiration is not None and current_time < expiration - timedelta(minutes=5)
