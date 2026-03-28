import importlib
import inspect
import pkgutil

import mountaineer_cloud.providers as providers_package
from mountaineer_cloud.providers.aws import AWSConfig, AWSCore, AWSDependencies
from mountaineer_cloud.providers.base import ProviderCore
from mountaineer_cloud.providers.cloudflare import (
    CloudflareConfig,
    CloudflareCore,
    CloudflareDependencies,
)
from mountaineer_cloud.providers.definition import (
    SUPPORTED_PROVIDERS,
    ProviderDefinition,
    resolve_cloud_by_config,
)
from mountaineer_cloud.providers.digitalocean import (
    DigitalOceanConfig,
    DigitalOceanCore,
    DigitalOceanDependencies,
)
from mountaineer_cloud.providers.resend import (
    ResendConfig,
    ResendCore,
    ResendDependencies,
)
from mountaineer_cloud.providers_common.email import EmailProviderCore
from mountaineer_cloud.providers_common.storage import StorageProviderCore


def test_supported_providers_registry():
    assert SUPPORTED_PROVIDERS == [
        ProviderDefinition(
            config_class=AWSConfig,
            core_class=AWSCore,
            injection_function=AWSDependencies.get_aws_core,
        ),
        ProviderDefinition(
            config_class=CloudflareConfig,
            core_class=CloudflareCore,
            injection_function=CloudflareDependencies.get_cloudflare_core,
        ),
        ProviderDefinition(
            config_class=DigitalOceanConfig,
            core_class=DigitalOceanCore,
            injection_function=DigitalOceanDependencies.get_digitalocean_core,
        ),
        ProviderDefinition(
            config_class=ResendConfig,
            core_class=ResendCore,
            injection_function=ResendDependencies.get_resend_core,
        ),
    ]


def test_supported_providers_include_every_defined_core():
    discovered_core_classes: list[type[ProviderCore]] = []

    for module_info in pkgutil.iter_modules(providers_package.__path__):
        if not module_info.ispkg:
            continue

        module_name = f"{providers_package.__name__}.{module_info.name}"
        provider_module = importlib.import_module(module_name)

        provider_core_classes = [
            core_class
            for _, core_class in inspect.getmembers(provider_module, inspect.isclass)
            if issubclass(core_class, ProviderCore)
            and core_class is not ProviderCore
            and core_class.__module__ == f"{module_name}.core"
        ]

        assert provider_core_classes, f"No provider core exported by {module_name}"
        discovered_core_classes.extend(provider_core_classes)

    assert {provider.core_class for provider in SUPPORTED_PROVIDERS} == set(
        discovered_core_classes
    )


def test_resolve_cloud_by_config_returns_all_email_matches():
    class EmailAppConfig(AWSConfig, ResendConfig):
        pass

    app_config = EmailAppConfig(
        AWS_ACCESS_KEY="aws_access_key",
        AWS_SECRET_KEY="aws_secret_key",
        AWS_REGION_NAME="us-east-1",
        AWS_ROLE_ARN="arn:aws:iam::123456789012:role/test-role",
        AWS_ROLE_SESSION_NAME="test-session",
        RESEND_API_KEY="re_test_key",
    )

    matches = resolve_cloud_by_config(app_config, EmailProviderCore)

    assert [provider.config_class for provider in matches] == [
        AWSConfig,
        ResendConfig,
    ]
    assert [provider.core_class for provider in matches] == [AWSCore, ResendCore]
    assert [provider.injection_function for provider in matches] == [
        AWSDependencies.get_aws_core,
        ResendDependencies.get_resend_core,
    ]


def test_resolve_cloud_by_config_returns_empty_for_unsupported_capability():
    app_config = ResendConfig(RESEND_API_KEY="re_test_key")

    matches = resolve_cloud_by_config(app_config, StorageProviderCore)

    assert matches == []
