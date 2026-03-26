from dataclasses import dataclass
from typing import Any

import httpx

from mountaineer_cloud.providers_common.email import (
    EmailBody,
    EmailProviderCore,
    EmailRecipient,
)

from .config import ResendConfig


@dataclass
class ResendCore(EmailProviderCore[ResendConfig]):
    session: httpx.AsyncClient

    async def email_send(
        self,
        *,
        sender: EmailRecipient,
        recipient: EmailRecipient,
        subject: str,
        body: EmailBody,
    ) -> str:
        payload: dict[str, Any] = {
            "from": sender.formatted,
            "to": [recipient.formatted],
            "subject": subject,
        }

        if body.html is not None:
            payload["html"] = body.html
        if body.text is not None:
            payload["text"] = body.text

        response = await self.session.post("/emails", json=payload)
        response.raise_for_status()

        response_json = response.json()
        if "id" not in response_json:
            raise ValueError("Resend response did not include an email id.")

        return str(response_json["id"])
