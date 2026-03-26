from pydantic_settings import BaseSettings


class AWSConfig(BaseSettings):
    AWS_ACCESS_KEY: str
    AWS_SECRET_KEY: str
    AWS_REGION_NAME: str
    AWS_ROLE_ARN: str
    AWS_ROLE_SESSION_NAME: str
