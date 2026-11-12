"""
Typed errors raised by storage providers.

Where a failure has a native Python equivalent we raise that type directly, so
callers can handle remote objects the same way they would local files:

- Missing objects raise `FileNotFoundError`.
- Denied access raises `PermissionError`.

Failures without a native equivalent raise subclasses of botocore's
`ClientError`, so existing `except ClientError` handlers keep catching them
while new code can match on the specific condition.

In every case the original `ClientError` from the provider is preserved as the
`__cause__` of the raised exception.
"""

import errno
import os
from typing import Any

from botocore.exceptions import ClientError

# Error codes are matched before HTTP status codes, since some conditions share
# a status (S3 returns 404 for both a missing key and a missing bucket).
NOT_FOUND_ERROR_CODES = frozenset({"NoSuchKey", "NotFound", "404"})
PERMISSION_ERROR_CODES = frozenset(
    {"AccessDenied", "AllAccessDisabled", "Forbidden", "403"}
)
BUCKET_NOT_FOUND_ERROR_CODES = frozenset({"NoSuchBucket"})
RATE_LIMIT_ERROR_CODES = frozenset(
    {
        "SlowDown",
        "Throttling",
        "ThrottlingException",
        "RequestThrottled",
        "TooManyRequests",
        "TooManyRequestsException",
    }
)


class StorageClientError(ClientError):
    """
    Base class for storage failures that have no native Python equivalent.
    """

    def __init__(
        self,
        error_response: Any,
        operation_name: str,
        *,
        path: str | None = None,
    ):
        super().__init__(error_response, operation_name)
        self.path = path


class StorageBucketNotFoundError(StorageClientError):
    """
    The bucket referenced by the storage path does not exist.
    """


class StorageRateLimitError(StorageClientError):
    """
    The provider throttled the request. Retrying after a backoff may succeed.
    """


def translate_storage_error(exc: ClientError, *, path: str) -> Exception | None:
    """
    Map a provider `ClientError` onto a native or storage-specific exception.

    Returns `None` when the error has no more specific type, in which case the
    caller should re-raise the original `ClientError`.
    """
    error = exc.response.get("Error", {})
    code = str(error.get("Code", ""))
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if code in BUCKET_NOT_FOUND_ERROR_CODES:
        return StorageBucketNotFoundError(exc.response, exc.operation_name, path=path)
    if code in RATE_LIMIT_ERROR_CODES:
        return StorageRateLimitError(exc.response, exc.operation_name, path=path)
    if code in NOT_FOUND_ERROR_CODES:
        return FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
    if code in PERMISSION_ERROR_CODES:
        return PermissionError(errno.EACCES, os.strerror(errno.EACCES), path)

    if status == 404:
        return FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
    if status == 403:
        return PermissionError(errno.EACCES, os.strerror(errno.EACCES), path)
    if status == 429:
        return StorageRateLimitError(exc.response, exc.operation_name, path=path)

    return None
