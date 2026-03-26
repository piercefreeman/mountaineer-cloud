from pydantic_settings import BaseSettings


class CloudflareConfig(BaseSettings):
    R2_ACCESS_KEY_ID: str
    R2_SECRET_ACCESS_KEY: str
    R2_ACCOUNT_ID: str
