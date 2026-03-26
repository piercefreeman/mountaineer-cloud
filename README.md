# mountaineer-cloud

Shared cloud primitives for Python webservices, in particular ones built off of [Mountaineer](https://github.com/piercefreeman/mountaineer) or [FastAPI](https://github.com/fastapi/fastapi).

Most webapps themselves are cloud-agnostic and can be distributed within any Linux environment. This works pretty well when your whole application is stateless and just talks to a database, but falls down once you actually start needing infrastructure that spawns across your whole cluster. This package exists for the places where that abstraction breaks down and you still need a clean way to work with a real supplier:

- Object storage
- Email delivery

## Installation

Install the package as usual. If you want additional dependencies to mock (some) providers locally, also install the `mocks` extra:

```bash
uv add "mountaineer-cloud"
uv add --dev "mountaineer-cloud[mocks]"
```

### Storage Primitive

The core convention for our storage primitives is:

1. A provider package establishes how to connect to the real supplier (S3, R2, etc).
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
) -> bytes:
    await asset.file_url.put_content(aws, b"hello world")
    contents = await asset.file_url.get_content(aws)
    return contents


async def get_asset_contents(
    asset: Asset,
    aws: AWSCore = Depends(AWSDependencies.get_aws_core),
) -> bytes:
    return await asset.file_url.get_content(aws)
```

If you later move that same model or endpoint to another provider, the primitive API stays the same. Thanks to the magic of typehinting, we'll proactively flag errors if you're trying to use a backend provider that doesn't support the functionality you expect. The main thing that changes is the core type:

- `CloudFile[AWSCore]`
- `CloudFile[CloudflareCore]`
- `CloudFile[DigitalOceanCore]`

CloudFile has support for writing and reading in one fell-swoop, in addition to streaming reads that are more efficient for large files stored remotely.

### Email Primitive

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

| Provider | Storage | Email | Config / Core / Dependencies |
| --- | --- | --- | --- |
| AWS | Yes, via S3 | Yes, via SES | `AWSConfig`, `AWSCore`, `AWSDependencies` |
| Cloudflare | Yes, via R2 | No | `CloudflareConfig`, `CloudflareCore`, `CloudflareDependencies` |
| DigitalOcean | Yes, via Spaces | No | `DigitalOceanConfig`, `DigitalOceanCore`, `DigitalOceanDependencies` |
| Resend | No | Yes | `ResendConfig`, `ResendCore`, `ResendDependencies` |

Environment variables are intentionally omitted here because each provider's settings can evolve over time. The source of truth is the corresponding `*Config` model in the provider package.

The usage pattern stays the same across providers:

```python
from fastapi import Depends

from mountaineer_cloud.providers.aws import AWSCore, AWSDependencies


async def endpoint(
    aws: AWSCore = Depends(AWSDependencies.get_aws_core),
) -> None:
    ...
```
