from abc import ABC, abstractmethod
from email.utils import formataddr
from typing import Generic, TypeVar

from pydantic import BaseModel, model_validator

from mountaineer_cloud.providers.base import ProviderCore

TConfig = TypeVar("TConfig")


class EmailRecipient(BaseModel):
    email: str
    display_name: str | None = None

    @property
    def formatted(self) -> str:
        if self.display_name:
            return formataddr((self.display_name, self.email))
        return self.email


class EmailBody(BaseModel):
    text: str | None = None
    html: str | None = None

    @model_validator(mode="after")
    def validate_body(self):
        if self.text is None and self.html is None:
            raise ValueError("EmailBody requires at least one of `text` or `html`.")
        return self


class EmailProviderCore(ProviderCore[TConfig], Generic[TConfig], ABC):
    @abstractmethod
    async def email_send(
        self,
        *,
        sender: EmailRecipient,
        recipient: EmailRecipient,
        subject: str,
        body: EmailBody,
    ) -> str:
        """
        Deliver an email and return the provider-specific message identifier.
        """
