from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast

from mountaineer_cloud.providers.aws.config import AWSConfig
from mountaineer_cloud.providers.aws.core import AWSCore
from mountaineer_cloud.providers.aws.dependencies import get_aws_core
from mountaineer_cloud.providers.base import ProviderCore
from mountaineer_cloud.providers.cloudflare.config import CloudflareConfig
from mountaineer_cloud.providers.cloudflare.core import CloudflareCore
from mountaineer_cloud.providers.cloudflare.dependencies import get_cloudflare_core
from mountaineer_cloud.providers.digitalocean.config import DigitalOceanConfig
from mountaineer_cloud.providers.digitalocean.core import DigitalOceanCore
from mountaineer_cloud.providers.digitalocean.dependencies import (
    get_digitalocean_core,
)
from mountaineer_cloud.providers.resend.config import ResendConfig
from mountaineer_cloud.providers.resend.core import ResendCore
from mountaineer_cloud.providers.resend.dependencies import get_resend_core

TConfig = TypeVar("TConfig")
TProviderCore = TypeVar("TProviderCore", bound=ProviderCore[Any])
TRequiredCloudBase = TypeVar("TRequiredCloudBase", bound=ProviderCore[Any])

ProviderInjectionFunction = Callable[..., AsyncGenerator[TProviderCore, None]]


@dataclass(frozen=True)
class ProviderDefinition(Generic[TConfig, TProviderCore]):
    config_class: type[TConfig]
    core_class: type[TProviderCore]
    injection_function: ProviderInjectionFunction[TProviderCore]


SUPPORTED_PROVIDERS: list[ProviderDefinition[Any, Any]] = [
    ProviderDefinition(
        config_class=AWSConfig,
        core_class=AWSCore,
        injection_function=get_aws_core,
    ),
    ProviderDefinition(
        config_class=CloudflareConfig,
        core_class=CloudflareCore,
        injection_function=get_cloudflare_core,
    ),
    ProviderDefinition(
        config_class=DigitalOceanConfig,
        core_class=DigitalOceanCore,
        injection_function=get_digitalocean_core,
    ),
    ProviderDefinition(
        config_class=ResendConfig,
        core_class=ResendCore,
        injection_function=get_resend_core,
    ),
]


def resolve_cloud_by_config(
    app_config: object,
    required_cloud_base: type[TRequiredCloudBase],
) -> list[ProviderDefinition[Any, TRequiredCloudBase]]:
    """
    Used by clients to get a dynamic list of providers that match the current app config.
    This is useful in situations where you know you need an email-provider but you don't
    know which one the user has configured.

    """
    matching_providers: list[ProviderDefinition[Any, TRequiredCloudBase]] = []

    for provider in SUPPORTED_PROVIDERS:
        if not isinstance(app_config, provider.config_class):
            continue
        if not issubclass(provider.core_class, required_cloud_base):
            continue

        matching_providers.append(
            cast(ProviderDefinition[Any, TRequiredCloudBase], provider)
        )

    return matching_providers
