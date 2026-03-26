import io
from itertools import product
from unittest.mock import patch

import aioboto3
import pytest
import pytest_asyncio
from pydantic import ValidationError

from mountaineer_cloud.aws import AWSDependencies, CompressionType, StorageBackendType
from mountaineer_cloud.aws.config import AWSConfig
from mountaineer_cloud.aws.s3 import (
    S3Metadata,
    S3PointerMixin,
)
from mountaineer_cloud.test_utilities import MockAWS
from mountaineer_cloud.test_utilities.fixtures import get_mock_aws


@pytest.fixture
def mock_app_config():
    return AWSConfig(
        AWS_ACCESS_KEY="test_key",
        AWS_SECRET_KEY="test_secret",
        AWS_REGION_NAME="test_region",
        AWS_ROLE_ARN="test_role_arn",
        AWS_ROLE_SESSION_NAME="test_session_name",
    )


@pytest_asyncio.fixture
async def mock_aws():
    async with get_mock_aws(
        whitelisted_buckets=[
            "mountaineer-test",
        ]
    ) as mock_aws:
        yield mock_aws


# bug: Global definition of our pointer mixin, versus being scoped to
# each test class that needs it.
# This is a workaround for in-function Pydantic definition causing a
# deferral trace and not validating with the pydantic mypy plugin
# Since we need to dynamically define the metadata, we can't use one
# global definition here. Instead the main class has to be defined
# globally and test functions have to override the metadata to avoid the issue
class ExampleAWSPointer(S3PointerMixin):
    pass


@pytest_asyncio.fixture
async def aws_session(
    mock_aws,
    mock_app_config,
):
    """Get AWS session using the mock STS client."""
    return await AWSDependencies.get_aws_session(mock_app_config)


@pytest.mark.parametrize(
    "compression_type,backend_type,data_size",
    list(
        product(
            [CompressionType.RAW, CompressionType.BROTLI, CompressionType.GZIP],
            [StorageBackendType.DISK, StorageBackendType.MEMORY],
            [0, 1024, 50 * 1024],
        )
    ),
)
def test_compression_decompression(
    compression_type: CompressionType,
    backend_type: StorageBackendType,
    data_size: int,
):
    original_data = b"x" * data_size

    ExampleAWSPointer.s3_object_metadata = S3Metadata(
        key_bucket="mountaineer-test",
        key_prefix="test-prefix",
        pointer_compression=compression_type,
        pointer_storage_backend=backend_type,
    )

    stub_obj = ExampleAWSPointer()

    with io.BytesIO(original_data) as file:
        with stub_obj.wrap_compressed_file(file) as compressed_file:
            compressed_data = compressed_file.read()

            with io.BytesIO(compressed_data) as compressed_io:
                with stub_obj.unwrap_compressed_file(
                    compressed_io
                ) as decompressed_file:
                    decompressed_data = decompressed_file.read()

    assert original_data == decompressed_data


@pytest.mark.parametrize("invalid_compression_type", ["INVALID_TYPE", None, 123])
def test_invalid_compression_type(
    invalid_compression_type,
    mock_aws,
):
    with pytest.raises(ValidationError):
        S3Metadata(
            key_bucket="mountaineer-test",
            key_prefix="test-prefix",
            pointer_compression=invalid_compression_type,
            pointer_storage_backend=StorageBackendType.DISK,
        )


@pytest.mark.asyncio
async def test_s3_upload(
    mock_aws: MockAWS, aws_session: aioboto3.Session, mock_app_config: AWSConfig
):
    """
    Ensure that we can properly upload and download data to S3. This test does not
    need to be fully parameterized with the different encoding and storage options
    because these are tested separately.

    """

    ExampleAWSPointer.s3_object_metadata = S3Metadata(
        key_bucket="mountaineer-test",
        key_prefix="test-prefix",
        pointer_compression=CompressionType.RAW,
        pointer_storage_backend=StorageBackendType.MEMORY,
    )

    stub_obj = ExampleAWSPointer()

    with patch("mountaineer_cloud.aws.s3.uuid4", return_value="test-uuid"):
        await stub_obj.put_content_into_pointer(
            payload=io.BytesIO(b"test data"),
            session=aws_session,
            config=mock_app_config,
        )

    # We have retained the passed-in properties and added our s3 path
    # to the appropriate attribute
    assert stub_obj.s3_object_path == "s3://mountaineer-test/test-prefix/test-uuid"

    # Make sure we have actually written this path to S3
    obj = await mock_aws.mock_s3.get_object(
        Bucket="mountaineer-test",
        Key="test-prefix/test-uuid",
    )
    assert await obj["Body"].read() == b"test data"


@pytest.mark.asyncio
async def test_s3_download(
    mock_aws: MockAWS, aws_session: aioboto3.Session, mock_app_config: AWSConfig
):
    ExampleAWSPointer.s3_object_metadata = S3Metadata(
        key_bucket="mountaineer-test",
        key_prefix="test-prefix",
        pointer_compression=CompressionType.RAW,
        pointer_storage_backend=StorageBackendType.MEMORY,
    )

    # Manually upload a file to S3 and create the database object
    await mock_aws.mock_s3.put_object(
        Bucket="mountaineer-test",
        Key="test-prefix/test-uuid",
        Body=b"test data",
    )

    stub_obj = ExampleAWSPointer(
        s3_object_path="s3://mountaineer-test/test-prefix/test-uuid"
    )

    # Make sure we can download the file
    async with stub_obj.get_contents_from_pointer(
        session=aws_session, config=mock_app_config
    ) as file:
        assert file.read() == b"test data"


@pytest.mark.asyncio
async def test_explicit_s3_key(
    mock_aws: MockAWS, aws_session: aioboto3.Session, mock_app_config: AWSConfig
):
    ExampleAWSPointer.s3_object_metadata = S3Metadata(
        key_bucket="mountaineer-test",
        key_prefix="test-prefix",
        pointer_compression=CompressionType.RAW,
        pointer_storage_backend=StorageBackendType.MEMORY,
    )

    stub_obj = ExampleAWSPointer()

    # Manually upload a file to S3 and create the database object
    await stub_obj.put_content_into_pointer(
        payload=io.BytesIO(b"test data"),
        explicit_s3_path="s3://mountaineer-test/test-prefix/test-uuid",
        session=aws_session,
        config=mock_app_config,
    )

    # Get the file from where we expect in S3
    obj = await mock_aws.mock_s3.get_object(
        Bucket="mountaineer-test",
        Key="test-prefix/test-uuid",
    )
    assert await obj["Body"].read() == b"test data"
