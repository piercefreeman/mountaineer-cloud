# Email

## Setup

Emails are modeled as an embedded Iceaxe field:

```python
from iceaxe import Field, TableBase

from mountaineer_cloud import CloudMixin
from mountaineer_cloud.primitives import (
    CloudEmailField,
    EmailBody,
    EmailMessage,
    EmailRecipient,
)
from mountaineer_cloud.providers.resend import ResendCore


class Notification(CloudMixin, TableBase):
    id: int = Field(primary_key=True)
    email: EmailMessage[ResendCore] | None = CloudEmailField()
```

Each `EmailMessage[...]` includes:

- `sender`
- `recipient`
- `subject`
- `body`

`body` supports either `text`, `html`, or both.

Send through the provider-specific core with:

```python
await notification.email.send(core)
```

## Resend

Required settings:

- `RESEND_API_KEY`
- `RESEND_BASE_URL`
- `RESEND_TIMEOUT_SECONDS`

### AWS SES

Required settings:

- `AWS_ACCESS_KEY`
- `AWS_SECRET_KEY`
- `AWS_REGION_NAME`
- `AWS_ROLE_ARN`
- `AWS_ROLE_SESSION_NAME`

Once you've authenticated your domain, you have permission to send emails to a limited amount of sandbox email accounts. These are only intended to be used for testing since you'll need to add them one by one in the console. When you're ready to start sending emails to your production users, you'll need to request production access.

![ses_prod.png]()
