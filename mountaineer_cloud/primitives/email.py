from dataclasses import dataclass, field


@dataclass(frozen=True)
class EmailRecipient:
    email: str
    display_name: str | None = None


@dataclass(frozen=True)
class EmailMessage:
    sender: EmailRecipient
    to: list[EmailRecipient]
    subject: str
    text: str | None = None
    html: str | None = None
    cc: list[EmailRecipient] = field(default_factory=list)
    bcc: list[EmailRecipient] = field(default_factory=list)
