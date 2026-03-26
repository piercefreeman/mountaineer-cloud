from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from mountaineer_cloud.providers_common.email import (
    EmailBody,
    EmailProviderCore,
    EmailRecipient,
)
from mountaineer_cloud.providers_common.s3_compat import (
    S3CompatibleStorageCore,
    S3SessionManager,
)

from .config import AWSConfig

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from types_aiobotocore_ses.client import SESClient
    from types_aiobotocore_ses.type_defs import SendEmailResponseTypeDef

_session_manager = S3SessionManager[AWSConfig](url_scheme="s3")


@dataclass
class AWSCore(
    S3CompatibleStorageCore[AWSConfig],
    EmailProviderCore[AWSConfig],
):
    s3_session_manager = _session_manager

    @asynccontextmanager
    async def get_email_client(self) -> "AsyncGenerator[SESClient, None]":
        async with self.session.client(
            "ses",
            region_name=self.config.AWS_REGION_NAME,
        ) as client:
            yield client

    async def email_send(
        self,
        *,
        sender: EmailRecipient,
        recipient: EmailRecipient,
        subject: str,
        body: EmailBody,
    ) -> str:
        message_body: dict[str, dict[str, str]] = {}
        if body.text is not None:
            message_body["Text"] = {
                "Data": body.text,
                "Charset": "UTF-8",
            }
        if body.html is not None:
            message_body["Html"] = {
                "Data": body.html,
                "Charset": "UTF-8",
            }

        async with self.get_email_client() as ses:
            response: SendEmailResponseTypeDef = await ses.send_email(
                Source=sender.formatted,
                Destination={"ToAddresses": [recipient.email]},
                Message={
                    "Subject": {
                        "Data": subject,
                        "Charset": "UTF-8",
                    },
                    "Body": message_body,
                },
            )

        return str(response["MessageId"])
