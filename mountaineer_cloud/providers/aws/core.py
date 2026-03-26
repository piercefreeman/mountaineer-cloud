from dataclasses import dataclass

from mountaineer_cloud.providers_common.s3_compat import (
    S3SessionManager,
    StorageProviderCore,
)

from .config import AWSConfig

_session_manager = S3SessionManager[AWSConfig](url_scheme="s3")


@dataclass
class AWSCore(StorageProviderCore[AWSConfig]):
    s3_session_manager = _session_manager
