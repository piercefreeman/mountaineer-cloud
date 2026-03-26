import json
from typing import Any, cast

import httpx
import pytest
from iceaxe import Field as IceaxeField, TableBase
from iceaxe.schemas.db_memory_serializer import DatabaseHandler
from iceaxe.schemas.db_stubs import DBColumn
from iceaxe.sql_types import ColumnType

from mountaineer_cloud.mixin import CloudMixin
from mountaineer_cloud.primitives import (
    CloudEmailField,
    EmailBody,
    EmailMessage,
    EmailRecipient,
)
from mountaineer_cloud.providers.resend import ResendConfig, ResendCore


class ExampleResendOutboundEmail(CloudMixin, TableBase):
    id: int = IceaxeField(primary_key=True)
    email: EmailMessage[ResendCore] | None = cast(
        Any,
        CloudEmailField(),
    )


def test_cloudemail_iceaxe_column_type():
    columns = [
        node
        for node, _ in DatabaseHandler().convert([ExampleResendOutboundEmail])
        if isinstance(node, DBColumn)
    ]
    email_column = next(column for column in columns if column.column_name == "email")

    assert email_column.column_type == ColumnType.JSON
    assert email_column.nullable is True


@pytest.mark.asyncio
async def test_resend_email_send():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert str(request.url) == "https://api.resend.com/emails"
        assert request.headers["Authorization"] == "Bearer re_test_key"
        assert "User-Agent" in request.headers

        payload = json.loads(request.content.decode())
        assert payload == {
            "from": "Mountaineer <sender@example.com>",
            "to": ["Recipient <recipient@example.com>"],
            "subject": "Resend subject",
            "text": "Plain text body",
            "html": "<p>HTML body</p>",
        }

        return httpx.Response(
            status_code=200,
            json={"id": "re_email_123"},
        )

    core = ResendCore(
        config=ResendConfig(RESEND_API_KEY="re_test_key"),
        session=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://api.resend.com",
            headers={
                "Authorization": "Bearer re_test_key",
                "User-Agent": "mountaineer-cloud",
            },
        ),
    )

    record = ExampleResendOutboundEmail(
        id=1,
        email=cast(
            Any,
            {
                "sender": {
                    "email": "sender@example.com",
                    "display_name": "Mountaineer",
                },
                "recipient": {
                    "email": "recipient@example.com",
                    "display_name": "Recipient",
                },
                "subject": "Resend subject",
                "body": {
                    "text": "Plain text body",
                    "html": "<p>HTML body</p>",
                },
            },
        ),
    )
    assert record.email is not None

    message_id = await record.email.send(core)

    assert message_id == "re_email_123"

    await core.aclose()


@pytest.mark.asyncio
async def test_resend_core_email_send_supports_text_only():
    captured_payload = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_payload
        captured_payload = json.loads(request.content.decode())
        return httpx.Response(status_code=200, json={"id": "re_email_456"})

    core = ResendCore(
        config=ResendConfig(RESEND_API_KEY="re_test_key"),
        session=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://api.resend.com",
            headers={
                "Authorization": "Bearer re_test_key",
                "User-Agent": "mountaineer-cloud",
            },
        ),
    )

    message_id = await core.email_send(
        sender=EmailRecipient(email="sender@example.com"),
        recipient=EmailRecipient(email="recipient@example.com"),
        subject="Text only",
        body=EmailBody(text="Only text"),
    )

    assert message_id == "re_email_456"
    assert captured_payload == {
        "from": "sender@example.com",
        "to": ["recipient@example.com"],
        "subject": "Text only",
        "text": "Only text",
    }

    await core.aclose()
