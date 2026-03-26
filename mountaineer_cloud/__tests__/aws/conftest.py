import pytest

from mountaineer import ConfigBase
from mountaineer.config import unregister_config

from mountaineer_cloud.providers.aws import AWSConfig


@pytest.fixture
def mock_app_config():
    # Unset the current config if it's already been set
    unregister_config()

    class ExampleAWSConfig(AWSConfig, ConfigBase):
        pass

    return ExampleAWSConfig(
        AWS_ACCESS_KEY="mock_access_key",
        AWS_SECRET_KEY="mock_secret_key",
        AWS_REGION_NAME="us-east-1",
        AWS_ROLE_ARN="arn:aws:iam::123456789012:role/mock-role",
        AWS_ROLE_SESSION_NAME="mock_session_name",
    )
