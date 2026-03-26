import httpx
from fastapi import Depends

from mountaineer import CoreDependencies

from mountaineer_cloud.providers.base import provider_core_dependency

from .config import ResendConfig
from .core import ResendCore

DEFAULT_USER_AGENT = "mountaineer-cloud"


async def build_resend_core(config: ResendConfig) -> ResendCore:
    return ResendCore(
        config=config,
        session=httpx.AsyncClient(
            base_url=config.RESEND_BASE_URL,
            headers={
                "Authorization": f"Bearer {config.RESEND_API_KEY}",
                "User-Agent": DEFAULT_USER_AGENT,
            },
            timeout=config.RESEND_TIMEOUT_SECONDS,
        ),
    )


async def get_resend_core(
    config: ResendConfig = Depends(
        CoreDependencies.get_config_with_type(ResendConfig)
    ),
):
    async for core in provider_core_dependency(
        build_core=lambda: build_resend_core(config),
    ):
        yield core
