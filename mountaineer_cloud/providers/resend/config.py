from pydantic_settings import BaseSettings


class ResendConfig(BaseSettings):
    RESEND_API_KEY: str
    RESEND_BASE_URL: str = "https://api.resend.com"
    RESEND_TIMEOUT_SECONDS: float = 30.0
