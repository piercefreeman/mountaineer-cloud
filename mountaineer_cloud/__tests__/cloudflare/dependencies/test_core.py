from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from mountaineer_cloud.cloudflare.config import CloudflareConfig
from mountaineer_cloud.cloudflare.dependencies.core import (
    get_r2_session,
    is_session_valid,
)


@pytest.fixture
def mock_app_config():
    return CloudflareConfig(
        R2_ACCESS_KEY_ID="test_key_id",
        R2_SECRET_ACCESS_KEY="test_secret_key",
        R2_ACCOUNT_ID="test_account_id",
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
async def test_get_r2_session(mock_app_config):
    with patch(
        "mountaineer_cloud.cloudflare.dependencies.core.CoreDependencies.get_config_with_type"
    ) as mock_get_config:
        mock_get_config.return_value = lambda: mock_app_config

        with patch("aioboto3.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session._session = MagicMock()
            mock_session_class.return_value = mock_session

            # Test the session creation
            session = await get_r2_session(mock_app_config)

            # Verify session creation with expected values
            mock_session_class.assert_called_with(
                aws_access_key_id=mock_app_config.R2_ACCESS_KEY_ID,
                aws_secret_access_key=mock_app_config.R2_SECRET_ACCESS_KEY,
            )

            # Check that the global state is set for future use
            from mountaineer_cloud.cloudflare.dependencies.core import GLOBAL_SESSIONS

            cached_data = GLOBAL_SESSIONS.get_obj()
            assert cached_data is not None
            cached_session, expiration = cached_data

            assert session == cached_session
            assert expiration is not None
            assert expiration > datetime.now(timezone.utc)
