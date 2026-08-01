from pathlib import Path

from fastapi.testclient import TestClient

from mountaineer import AppController, ConfigBase, Depends
from mountaineer.config import unregister_config

from mountaineer_cloud.providers.resend import (
    ResendConfig,
    ResendCore,
    ResendDependencies,
)


class AppConfig(ConfigBase, ResendConfig):
    pass


def test_resend_component_boots_with_mountaineer(tmp_path: Path) -> None:
    unregister_config()
    component = AppController(
        view_root=tmp_path,
        config=AppConfig(
            RESEND_API_KEY="re_test_key",
            RESEND_BASE_URL="https://api.resend.test",
        ),
    )

    @component.app.get("/cloud")
    async def cloud(
        resend: ResendCore = Depends(ResendDependencies.get_resend_core),
    ) -> dict[str, str]:
        return {"base_url": str(resend.session.base_url)}

    try:
        with TestClient(component.app) as client:
            response = client.get("/cloud")
    finally:
        unregister_config()

    assert response.status_code == 200
    assert response.json() == {"base_url": "https://api.resend.test"}
