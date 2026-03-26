"""
Tests for the moto-based AWS mock implementation.
"""

import json

import pytest
from types_aiobotocore_lambda.type_defs import InvocationResponseTypeDef
from types_aiobotocore_s3.type_defs import (
    GetObjectOutputTypeDef,
    ListObjectsV2OutputTypeDef,
)
from types_aiobotocore_ses.type_defs import SendEmailResponseTypeDef

from mountaineer_cloud.logging import LOGGER
from mountaineer_cloud.test_utilities.fixtures import get_mock_aws


@pytest.mark.asyncio
async def test_async_mock_aws():
    """Test the async interface for mock AWS services."""
    async with get_mock_aws() as mock_aws:
        # Test S3
        await mock_aws.mock_s3.create_bucket(Bucket="test-bucket")
        await mock_aws.mock_s3.put_object(
            Bucket="test-bucket", Key="test.txt", Body="Hello World"
        )
        s3_response: GetObjectOutputTypeDef = await mock_aws.mock_s3.get_object(
            Bucket="test-bucket", Key="test.txt"
        )
        body = s3_response["Body"]
        content = await body.read()
        assert content == b"Hello World"

        # Verify email addresses for SES
        await mock_aws.mock_ses.verify_email_identity(EmailAddress="test@example.com")
        await mock_aws.mock_ses.verify_email_identity(
            EmailAddress="recipient@example.com"
        )

        # Test SES
        response_ses: SendEmailResponseTypeDef = await mock_aws.mock_ses.send_email(
            Source="test@example.com",
            Destination={"ToAddresses": ["recipient@example.com"]},
            Message={
                "Subject": {"Data": "Test Subject"},
                "Body": {"Text": {"Data": "Test Body"}},
            },
        )
        assert "MessageId" in response_ses


@pytest.mark.asyncio
async def test_s3_operations():
    """Test comprehensive S3 operations."""
    async with get_mock_aws() as mock_aws:
        # Create bucket
        await mock_aws.mock_s3.create_bucket(Bucket="test-bucket")

        # Put objects
        test_files = {
            "folder1/file1.txt": "content1",
            "folder1/file2.txt": "content2",
            "folder2/file3.txt": "content3",
            "file4.txt": "content4",
        }
        for key, content in test_files.items():
            await mock_aws.mock_s3.put_object(
                Bucket="test-bucket", Key=key, Body=content
            )

        # List objects
        response: ListObjectsV2OutputTypeDef = await mock_aws.mock_s3.list_objects_v2(
            Bucket="test-bucket"
        )
        assert response["KeyCount"] == 4
        contents = response.get("Contents", [])
        assert len(contents) == 4
        keys = {item.get("Key") for item in contents}
        assert keys == set(test_files.keys())

        # List with prefix
        response = await mock_aws.mock_s3.list_objects_v2(
            Bucket="test-bucket", Prefix="folder1/"
        )
        assert response["KeyCount"] == 2
        contents = response.get("Contents", [])
        keys = {item.get("Key") for item in contents}
        assert keys == {"folder1/file1.txt", "folder1/file2.txt"}

        # Delete objects
        await mock_aws.mock_s3.delete_objects(
            Bucket="test-bucket",
            Delete={
                "Objects": [{"Key": "folder1/file1.txt"}, {"Key": "folder1/file2.txt"}]
            },
        )

        # Verify deletion
        response = await mock_aws.mock_s3.list_objects_v2(Bucket="test-bucket")
        assert response["KeyCount"] == 2
        contents = response.get("Contents", [])
        keys = {item.get("Key") for item in contents}
        assert keys == {"folder2/file3.txt", "file4.txt"}


@pytest.mark.asyncio
async def test_ses_operations():
    """Test SES operations."""
    async with get_mock_aws() as mock_aws:
        # Verify email addresses first
        await mock_aws.mock_ses.verify_email_identity(EmailAddress="sender@example.com")
        await mock_aws.mock_ses.verify_email_identity(
            EmailAddress="recipient@example.com"
        )
        await mock_aws.mock_ses.verify_email_identity(EmailAddress="cc@example.com")
        await mock_aws.mock_ses.verify_email_identity(EmailAddress="bcc@example.com")

        # Send email
        response_ses: SendEmailResponseTypeDef = await mock_aws.mock_ses.send_email(
            Source="sender@example.com",
            Destination={
                "ToAddresses": ["recipient@example.com"],
                "CcAddresses": ["cc@example.com"],
                "BccAddresses": ["bcc@example.com"],
            },
            Message={
                "Subject": {"Data": "Test Email", "Charset": "UTF-8"},
                "Body": {
                    "Text": {"Data": "This is a test email", "Charset": "UTF-8"},
                    "Html": {"Data": "<p>This is a test email</p>", "Charset": "UTF-8"},
                },
            },
        )
        assert "MessageId" in response_ses


@pytest.mark.asyncio
async def test_mock_lambda_response():
    """Test the shortcut mock_lambda_response functionality."""
    async with get_mock_aws() as mock_aws:
        # Set up a mock response for the Lambda function
        mock_response = {"result": "success", "data": {"key": "value"}}
        await mock_aws.mock_lambda_response(
            function_name="test-function",
            response_payload=mock_response,
        )

        # Invoke the function and verify we get our mocked response
        lambda_response: InvocationResponseTypeDef = await mock_aws.mock_lambda.invoke(
            FunctionName="test-function",
            Payload=json.dumps({"input": "data"}).encode(),
        )

        assert lambda_response["StatusCode"] == 200

        response_payload = await lambda_response["Payload"].read()
        LOGGER.info(f"Response payload: {response_payload}")

        try:
            response_payload = json.loads(response_payload)
        except Exception as e:
            raise Exception(f"Failed to parse payload: {response_payload}") from e

        assert response_payload == mock_response
