from typing import Any, cast

import pytest
import pytest_asyncio
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
from mountaineer_cloud.primitives.base import get_cloud_field_definition
from mountaineer_cloud.providers.aws import AWSCore
from mountaineer_cloud.providers.aws.dependencies import build_aws_core
from mountaineer_cloud.test_utilities import MockAWS
from mountaineer_cloud.test_utilities.fixtures import get_mock_aws


class ExampleAWSOutboundEmail(CloudMixin, TableBase):
    id: int = IceaxeField(primary_key=True)
    email: EmailMessage[AWSCore] | None = cast(
        Any,
        CloudEmailField(),
    )


@pytest_asyncio.fixture
async def mock_aws():
    async with get_mock_aws() as mock_aws:
        yield mock_aws


@pytest_asyncio.fixture
async def aws_core(
    mock_aws: MockAWS,
    mock_app_config,
):
    del mock_aws
    return await build_aws_core(mock_app_config)


def test_cloudemail_iceaxe_column_type():
    columns = [
        node
        for node, _ in DatabaseHandler().convert([ExampleAWSOutboundEmail])
        if isinstance(node, DBColumn)
    ]
    email_column = next(column for column in columns if column.column_name == "email")

    assert email_column.column_type == ColumnType.JSON
    assert email_column.nullable is True


def test_cloudemail_field_definition_is_runtime_only():
    field = ExampleAWSOutboundEmail.model_fields["email"]
    definition = get_cloud_field_definition(field)

    assert field.json_schema_extra is None
    assert definition is not None

    email = ExampleAWSOutboundEmail(
        id=1,
        email={
            "sender": {"email": "sender@example.com"},
            "recipient": {"email": "recipient@example.com"},
            "subject": "Hello",
            "body": {"text": "Plain text"},
        },
    ).email
    assert email is not None
    assert email._cloud_definition is not None
    assert email._cloud_definition == definition
    assert email._cloud_field_name == "email"

    serialized = field.to_db_value(email)
    assert '"subject": "Hello"' in serialized
    assert '"recipient"' in serialized


def test_emailmessage_legacy_shape_is_supported():
    email = EmailMessage[AWSCore].model_validate(
        {
            "sender": {"email": "sender@example.com"},
            "to": [{"email": "recipient@example.com"}],
            "subject": "Legacy",
            "text": "Plain text",
        }
    )

    assert email.recipient.email == "recipient@example.com"
    assert email.body.text == "Plain text"


@pytest.mark.asyncio
async def test_ses_email_send(
    mock_aws: MockAWS,
    aws_core: AWSCore,
):
    await mock_aws.mock_ses.verify_email_identity(EmailAddress="sender@example.com")
    await mock_aws.mock_ses.verify_email_identity(
        EmailAddress="recipient@example.com"
    )

    message_id = await aws_core.email_send(
        sender=EmailRecipient(
            email="sender@example.com",
            display_name="Mountaineer",
        ),
        recipient=EmailRecipient(email="recipient@example.com"),
        subject="SES subject",
        body=EmailBody(
            text="Plain text body",
            html="<p>HTML body</p>",
        ),
    )

    assert message_id


@pytest.mark.asyncio
async def test_emailmessage_send_with_bound_iceaxe_field(
    mock_aws: MockAWS,
    aws_core: AWSCore,
):
    await mock_aws.mock_ses.verify_email_identity(EmailAddress="sender@example.com")
    await mock_aws.mock_ses.verify_email_identity(
        EmailAddress="recipient@example.com"
    )

    record = ExampleAWSOutboundEmail(
        id=1,
        email={
            "sender": {
                "email": "sender@example.com",
                "display_name": "Mountaineer",
            },
            "recipient": {"email": "recipient@example.com"},
            "subject": "Bound email",
            "body": {
                "text": "Bound body",
            },
        },
    )
    assert record.email is not None

    message_id = await record.email.send(aws_core)

    assert message_id
