import pytest

from mountaineer_cloud.providers.resend import ResendConfig, ResendCore
from mountaineer_cloud.providers.resend.dependencies import (
    DEFAULT_USER_AGENT,
    build_resend_core,
    get_resend_core,
)


@pytest.fixture
def mock_app_config():
    return ResendConfig(
        RESEND_API_KEY="re_test_key",
    )


@pytest.mark.asyncio
async def test_build_resend_core(mock_app_config: ResendConfig):
    core = await build_resend_core(mock_app_config)

    assert isinstance(core, ResendCore)
    assert core.config == mock_app_config
    assert str(core.session.base_url) == "https://api.resend.com"
    assert core.session.headers["Authorization"] == "Bearer re_test_key"
    assert core.session.headers["User-Agent"] == DEFAULT_USER_AGENT

    await core.aclose()


@pytest.mark.asyncio
async def test_get_resend_core(mock_app_config: ResendConfig):
    generator = get_resend_core(mock_app_config)
    core = await anext(generator)

    assert isinstance(core, ResendCore)
    assert core.config == mock_app_config

    await generator.aclose()
