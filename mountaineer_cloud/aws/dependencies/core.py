"""
Utilities to control permission grants for AWS roles.

"""

from datetime import datetime, timedelta, timezone

import aioboto3
import botocore.loaders
from fastapi import Depends

from mountaineer import CoreDependencies
from mountaineer.cache import AsyncLoopObjectCache

from mountaineer_cloud.aws.config import AWSConfig

GLOBAL_SESSIONS = AsyncLoopObjectCache[tuple[aioboto3.Session, datetime]]()

# Global loader for a central cache of botocore metadata. Workaround for the ~20MB memory
# allocation associated with JSONDecoder objects that is locked to each session.
# Bug: https://github.com/boto/botocore/issues/3078
BOTOCORE_LOADER = botocore.loaders.Loader()


async def get_aws_session(
    config: AWSConfig = Depends(CoreDependencies.get_config_with_type(AWSConfig)),
) -> aioboto3.Session:
    """
    Assumes specific credentials given the default assumable role in the settings.
    By convention, we assume that backend services will only want one valid sessions
    to AWS at a time. We cache it globally until expiration to save us from having
    to do the round-trip of reauthentication on each request.

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

        session = aioboto3.Session(
            aws_access_key_id=config.AWS_ACCESS_KEY,
            aws_secret_access_key=config.AWS_SECRET_KEY,
            region_name=config.AWS_REGION_NAME,
        )
        session._session.register_component("data_loader", BOTOCORE_LOADER)

        async with session.client("sts") as sts_client:
            assumed_role = await sts_client.assume_role(
                RoleArn=config.AWS_ROLE_ARN,
                RoleSessionName=config.AWS_ROLE_SESSION_NAME,
            )

        session_expiration = datetime.fromtimestamp(
            assumed_role["Credentials"]["Expiration"].timestamp(), timezone.utc
        )

        session = aioboto3.Session(
            aws_access_key_id=assumed_role["Credentials"]["AccessKeyId"],
            aws_secret_access_key=assumed_role["Credentials"]["SecretAccessKey"],
            aws_session_token=assumed_role["Credentials"]["SessionToken"],
            region_name=config.AWS_REGION_NAME,
        )
        session._session.register_component("data_loader", BOTOCORE_LOADER)

        GLOBAL_SESSIONS.set_obj((session, session_expiration))
        return session


def is_session_valid(expiration: datetime | None):
    current_time = datetime.now(timezone.utc)
    return expiration is not None and current_time < expiration - timedelta(minutes=5)
