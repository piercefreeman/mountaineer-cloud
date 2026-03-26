from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

TConfig = TypeVar("TConfig")


@dataclass
class ProviderCore(Generic[TConfig]):
    config: TConfig
    session: Any

    async def aclose(self):
        """
        Hook for future provider-specific cleanup.

        aioboto3 sessions themselves do not currently need explicit teardown, but
        other providers may wrap sessions that do.
        """
        session_aclose = getattr(self.session, "aclose", None)
        if callable(session_aclose):
            await session_aclose()


TProviderCore = TypeVar("TProviderCore", bound=ProviderCore[Any])


async def provider_core_dependency(
    *,
    build_core: Callable[[], Awaitable[TProviderCore]],
) -> AsyncGenerator[TProviderCore, None]:
    core = await build_core()
    try:
        yield core
    finally:
        await core.aclose()
