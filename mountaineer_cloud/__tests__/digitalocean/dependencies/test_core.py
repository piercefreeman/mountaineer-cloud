from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mountaineer_cloud.providers.digitalocean import (
    DigitalOceanConfig,
    DigitalOceanCore,
)
from mountaineer_cloud.providers.digitalocean.dependencies import (
    GLOBAL_SESSIONS,
    build_digitalocean_core,
    get_digitalocean_core,
    get_spaces_session,
    is_session_valid,
)


@pytest.fixture
def mock_app_config():
    return DigitalOceanConfig(
        SPACES_ACCESS_KEY_ID="test_key_id",
        SPACES_SECRET_ACCESS_KEY="test_secret_key",
        SPACES_REGION="nyc3",
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
async def test_get_spaces_session(mock_app_config):
    with patch(
        "mountaineer_cloud.providers.digitalocean.dependencies.CoreDependencies.get_config_with_type"
    ) as mock_get_config:
        mock_get_config.return_value = lambda: mock_app_config

        with patch("aioboto3.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session._session = MagicMock()
            mock_session_class.return_value = mock_session

            # Test the session creation
            session = await get_spaces_session(mock_app_config)

            # Verify session creation with expected values
            mock_session_class.assert_called_with(
                aws_access_key_id=mock_app_config.SPACES_ACCESS_KEY_ID,
                aws_secret_access_key=mock_app_config.SPACES_SECRET_ACCESS_KEY,
            )

            cached_data = GLOBAL_SESSIONS.get_obj()
            assert cached_data is not None
            cached_session, expiration = cached_data

            assert session == cached_session
            assert expiration is not None
            assert expiration > datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_build_digitalocean_core(mock_app_config):
    with patch(
        "mountaineer_cloud.providers.digitalocean.dependencies.get_spaces_session",
        new=AsyncMock(),
    ) as mock_get_session:
        mock_get_session.return_value = MagicMock()
        core = await build_digitalocean_core(mock_app_config)

    assert isinstance(core, DigitalOceanCore)
    assert core.config == mock_app_config
    assert core.session is not None
    mock_get_session.assert_awaited_once_with(mock_app_config)


@pytest.mark.asyncio
async def test_get_digitalocean_core(mock_app_config):
    generator = get_digitalocean_core(mock_app_config)
    core = await anext(generator)

    assert isinstance(core, DigitalOceanCore)
    assert core.config == mock_app_config

    await generator.aclose()
