# mountaineer-cloud

Shared cloud primitives for Python webservices, in particular ones built off of [Mountaineer](https://github.com/piercefreeman/mountaineer) or [FastAPI](https://github.com/fastapi/fastapi).

Most webapps themselves are cloud-agnostic and can be distributed within any Linux environment. This works pretty well when your whole application is stateless and just talks to a database, but falls down once you actually start needing infrastructure that spawns across your whole cluster. This package exists for the places where that abstraction breaks down and you still need a clean way to work with a real supplier:

- Object storage
- Email delivery

The package is organized around three layers:

- `mountaineer_cloud.providers`: provider-specific configuration, authentication, and dependency injection
- `mountaineer_cloud.providers_common`: shared provider-side runtime code
- `mountaineer_cloud.primitives`: the user-facing primitives you actually use in application code

## Installation

Install the package as usual. If you want additional dependencies to mock (some) providers locally, also install the `mocks` extra:

```bash
uv add "mountaineer-cloud"
uv add --dev "mountaineer-cloud[mocks]"
```

## Primitives

The user-facing primitives currently live under `mountaineer_cloud.primitives`.

Today those include:

- `CloudEmailField`
- `CloudFile`
- `CloudFileField`
- `CloudMixin`
- `EmailBody`
- `EmailMessage`
- `EmailRecipient`

The storage primitives are the primary implemented surface today. The core convention is:

1. A provider package establishes how to connect to the real supplier.
2. That provider exposes an authenticated `*Core` object.
3. A primitive accepts that core object and uses it to perform work.

For storage, that means your field annotation carries the provider core type:

```python
from fastapi import Depends
from iceaxe import Field, TableBase

from mountaineer_cloud import CloudMixin
from mountaineer_cloud.primitives import CloudFile, CloudFileField
from mountaineer_cloud.providers.aws import AWSCore, AWSDependencies


class Asset(CloudMixin, TableBase):
    id: int = Field(primary_key=True)
    file_url: CloudFile[AWSCore] | None = CloudFileField(
        bucket="my-bucket",
        prefix="assets",
    )


async def upload_asset(
    asset: Asset,
    aws: AWSCore = Depends(AWSDependencies.get_aws_core),
):
    await asset.file_url.put_content(aws, b"hello world")
    contents = await asset.file_url.get_content(aws)
    return contents
```

If you later move that same model or endpoint to another provider, the primitive API stays the same. Thanks to the magic of typehinting, we'll proactively flag errors if you're trying to use a backend provider that doesn't support the functionality you expect. The main thing that changes is the core type:

- `CloudFile[AWSCore]`
- `CloudFile[CloudflareCore]`
- `CloudFile[DigitalOceanCore]`

Email follows the same convention, except the primitive is a regular embedded Pydantic model instead of a string-backed pointer:

```python
from fastapi import Depends
from iceaxe import Field, TableBase

from mountaineer_cloud import CloudMixin
from mountaineer_cloud.primitives import (
    CloudEmailField,
    EmailBody,
    EmailMessage,
    EmailRecipient,
)
from mountaineer_cloud.providers.resend import ResendCore, ResendDependencies


class Notification(CloudMixin, TableBase):
    id: int = Field(primary_key=True)
    email: EmailMessage[ResendCore] | None = CloudEmailField()


async def send_notification(
    notification: Notification,
    resend: ResendCore = Depends(ResendDependencies.get_resend_core),
):
    notification.email = EmailMessage[ResendCore](
        sender=EmailRecipient(
            email="noreply@example.com",
            display_name="Example App",
        ),
        recipient=EmailRecipient(email="user@example.com"),
        subject="Welcome",
        body=EmailBody(
            text="Welcome to Example App",
            html="<p>Welcome to Example App</p>",
        ),
    )

    return await notification.email.send(resend)
```

## Providers

Each provider module establishes the concrete connection details for an underlying supplier.

Every provider package follows the same pattern:

- `*Config`: the settings model you inherit into your downstream app config
- `*Core`: the authenticated runtime object that combines provider config with an authenticated session
- `*Dependencies`: dependency-injected helpers like `get_*_core`, useful when using the DI syntax of FastAPI and Mountaineer

This is the center of the convention.

Your endpoint should depend on the provider core, not on a raw client or session. Then your primitive should accept that core and perform the actual work. This keeps supplier-specific connection logic inside the provider package and keeps the primitive surface stable.

## AWS

Import `AWSConfig` into your downstream application config and inherit from it:

```python
from mountaineer_cloud.providers.aws import AWSConfig


class AppConfig(AWSConfig):
    APP_NAME: str = "my-app"
```

This adds the AWS settings required by the provider:

- `AWS_ACCESS_KEY`
- `AWS_SECRET_KEY`
- `AWS_REGION_NAME`
- `AWS_ROLE_ARN`
- `AWS_ROLE_SESSION_NAME`

Then inject `AWSCore` where you need to talk to AWS-backed primitives:

```python
from fastapi import Depends

from mountaineer_cloud.providers.aws import AWSCore, AWSDependencies


async def endpoint(
    aws: AWSCore = Depends(AWSDependencies.get_aws_core),
):
    ...
```

`AWSCore` now supports both S3-backed storage and SES-backed email sending.

## Cloudflare

Import `CloudflareConfig` into your downstream application config and inherit from it:

```python
from mountaineer_cloud.providers.cloudflare import CloudflareConfig


class AppConfig(CloudflareConfig):
    APP_NAME: str = "my-app"
```

This adds the Cloudflare R2 settings required by the provider:

- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_ACCOUNT_ID`

Then inject `CloudflareCore` where you need to talk to Cloudflare-backed primitives:

```python
from fastapi import Depends

from mountaineer_cloud.providers.cloudflare import (
    CloudflareCore,
    CloudflareDependencies,
)


async def endpoint(
    cloudflare: CloudflareCore = Depends(CloudflareDependencies.get_cloudflare_core),
):
    ...
```

## DigitalOcean

Import `DigitalOceanConfig` into your downstream application config and inherit from it:

```python
from mountaineer_cloud.providers.digitalocean import DigitalOceanConfig


class AppConfig(DigitalOceanConfig):
    APP_NAME: str = "my-app"
```

This adds the Spaces settings required by the provider:

- `SPACES_ACCESS_KEY_ID`
- `SPACES_SECRET_ACCESS_KEY`
- `SPACES_REGION`

Then inject `DigitalOceanCore` where you need to talk to DigitalOcean-backed primitives:

```python
from fastapi import Depends

from mountaineer_cloud.providers.digitalocean import (
    DigitalOceanCore,
    DigitalOceanDependencies,
)


async def endpoint(
    digitalocean: DigitalOceanCore = Depends(
        DigitalOceanDependencies.get_digitalocean_core
    ),
):
    ...
```

## Resend

Import `ResendConfig` into your downstream application config and inherit from it:

```python
from mountaineer_cloud.providers.resend import ResendConfig


class AppConfig(ResendConfig):
    APP_NAME: str = "my-app"
```

This adds the Resend settings required by the provider:

- `RESEND_API_KEY`
- `RESEND_BASE_URL`
- `RESEND_TIMEOUT_SECONDS`

Then inject `ResendCore` where you need to talk to Resend-backed email primitives:

```python
from fastapi import Depends

from mountaineer_cloud.providers.resend import ResendCore, ResendDependencies


async def endpoint(
    resend: ResendCore = Depends(ResendDependencies.get_resend_core),
):
    ...
```

## Storage Notes

`CloudFile` is intentionally a string-backed type so it still stores cleanly in ORMs like Iceaxe, while also carrying the methods needed to read and write remote content.

`CloudFileField(...)` defines the storage configuration for the pointer itself:

- `bucket`
- `prefix`
- `suffix`
- `compression`
- `storage_backend`

`CloudMixin` is the model-side glue that binds those field definitions back onto runtime values. It exists because `CloudFileField(...)` is instantiated in global scope, before the field has access to the resolved model type hint or the eventual model instance via `self`.

## Email Notes

`EmailMessage` is stored as a JSON-backed embedded value in Iceaxe. `CloudEmailField(...)` marks the field as JSON, attaches the runtime binding metadata, and ensures bound values can call `await email.send(core)` against any provider core that implements `email_send(...)`.
