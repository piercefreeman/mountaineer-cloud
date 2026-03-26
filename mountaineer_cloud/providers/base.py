from dataclasses import dataclass
from typing import Generic, TypeVar

import aioboto3

TConfig = TypeVar("TConfig")


@dataclass
class ProviderCore(Generic[TConfig]):
    config: TConfig
    session: aioboto3.Session

    async def aclose(self):
        """
        Hook for future provider-specific cleanup.
        aioboto3 sessions themselves do not currently need explicit teardown.
        """
