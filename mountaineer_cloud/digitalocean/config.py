from pydantic_settings import BaseSettings


class DigitalOceanConfig(BaseSettings):
    SPACES_ACCESS_KEY_ID: str
    SPACES_SECRET_ACCESS_KEY: str
    SPACES_REGION: str
