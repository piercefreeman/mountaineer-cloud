from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mountaineer_cloud.aws.config import AWSConfig
from mountaineer_cloud.aws.dependencies.core import (
    get_aws_session,
    is_session_valid,
)


@pytest.fixture
def mock_app_config():
    return AWSConfig(
        AWS_ACCESS_KEY="test_access_key",
        AWS_SECRET_KEY="test_secret_key",
        AWS_REGION_NAME="us-east-1",
        AWS_ROLE_ARN="test_role_arn",
        AWS_ROLE_SESSION_NAME="test_session_name",
    )


@pytest.mark.parametrize(
    "minutes_difference,expected",
    [
        (60, True),  # Expires in an hour in the future
        (5.1, True),  # Exactly 5 minutes in the future
        (1, False),  # Expires in 1 minute
        (-4, False),  # Expired 4 minutes ago
    ],
)
def test_is_session_valid(minutes_difference, expected):
    expiration = datetime.now(timezone.utc) + timedelta(minutes=minutes_difference)
    assert is_session_valid(expiration) == expected


@pytest.mark.asyncio
async def test_get_aws_session(mock_app_config):
    with patch(
        "mountaineer_cloud.aws.dependencies.core.CoreDependencies.get_config_with_type"
    ) as mock_get_config:
        mock_get_config.return_value = lambda: mock_app_config

        with patch("aioboto3.Session") as mock_session_class:
            # Create a mock session with _session attribute
            mock_initial_session = MagicMock()
            mock_initial_session._session = MagicMock()
            mock_session_class.return_value = mock_initial_session

            # Setup the STS client mock
            mock_sts_client = AsyncMock()
            mock_sts_client.assume_role.return_value = {
                "Credentials": {
                    "AccessKeyId": "mocked_access_key_id",
                    "SecretAccessKey": "mocked_secret_access_key",
                    "SessionToken": "mocked_session_token",
                    "Expiration": datetime.now(timezone.utc) + timedelta(hours=1),
                }
            }
            mock_initial_session.client.return_value.__aenter__.return_value = (
                mock_sts_client
            )

            # Create a mock for the assumed role session
            mock_assumed_session = MagicMock()
            mock_assumed_session._session = MagicMock()

            # Set up the second call to Session to return the assumed role session
            mock_session_class.side_effect = [
                mock_initial_session,
                mock_assumed_session,
            ]

            # Call the function under test
            session = await get_aws_session(mock_app_config)

            # Verify initial session creation
            assert mock_session_class.call_args_list[0].kwargs == {
                "aws_access_key_id": mock_app_config.AWS_ACCESS_KEY,
                "aws_secret_access_key": mock_app_config.AWS_SECRET_KEY,
                "region_name": mock_app_config.AWS_REGION_NAME,
            }

            # Verify assumed session creation
            assert mock_session_class.call_args_list[1].kwargs == {
                "aws_access_key_id": "mocked_access_key_id",
                "aws_secret_access_key": "mocked_secret_access_key",
                "aws_session_token": "mocked_session_token",
                "region_name": mock_app_config.AWS_REGION_NAME,
            }

            # Check that the global state is set for future use
            from mountaineer_cloud.aws.dependencies.core import GLOBAL_SESSIONS

            session_payload = GLOBAL_SESSIONS.get_obj()
            assert session_payload is not None
            global_session, session_expiration = session_payload
            assert session == global_session
            assert global_session is not None
            assert session_expiration is not None
            assert session_expiration > datetime.now(timezone.utc)
