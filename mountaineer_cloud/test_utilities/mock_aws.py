"""
Core moto server functionality and AWS session management.
"""

import asyncio
import shutil
import signal
import socket
import subprocess
from contextlib import asynccontextmanager
from dataclasses import dataclass
from json import dumps as json_dumps
from subprocess import Popen
from typing import Any, AsyncIterator
from unittest.mock import AsyncMock

import aioboto3
import aiohttp

try:
    from types_aiobotocore_iam.client import IAMClient
    from types_aiobotocore_lambda.client import LambdaClient
    from types_aiobotocore_s3.client import S3Client
    from types_aiobotocore_ses.client import SESClient
    from types_aiobotocore_sts.client import STSClient
    from typing_extensions import TypeAlias
except ImportError as e:
    raise ImportError(
        "Could not find typehints. You need to install our `mocks` extra to use AWSMock:\n"
        "uv add --dev 'mountaineer-cloud[mocks]'"
    ) from e

from mountaineer_cloud.logging import LOGGER

# Type definitions
ServiceName: TypeAlias = str
ServiceURL: TypeAlias = str

MOCK_REGION = "us-east-1"


class MockSessionOverride(aioboto3.Session):
    def __init__(self, *args, **kwargs):
        moto_url = kwargs.pop("moto_url", None)
        super().__init__(*args, **kwargs)
        self.moto_url = moto_url

    def client(self, service_name, *args, **kwargs):
        # Prepare a fake STS client that simulates assume_role. moto doesn't fully support
        # assuming roles and scoping permissions, so we'd rather just use the main session's
        # mocked auth keys.
        if service_name == "sts":
            return AsyncMock()

        kwargs = self._bootstrap_args(kwargs)
        return super().client(service_name, *args, **kwargs)

    def resource(self, service_name, *args, **kwargs):
        if service_name == "sts":
            return AsyncMock()

        kwargs = self._bootstrap_args(kwargs)
        return super().resource(service_name, *args, **kwargs)

    def _bootstrap_args(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        """
        Bootstrap the arguments for the session.
        """
        if "endpoint_url" not in kwargs:
            kwargs["endpoint_url"] = self.moto_url
            kwargs["region_name"] = MOCK_REGION
            kwargs["aws_access_key_id"] = "test"
            kwargs["aws_secret_access_key"] = "test"
            kwargs["aws_session_token"] = "test"
        return kwargs


@dataclass
class MockAWS:
    """
    Convenience wrapper for accessing AWS service clients.

    """

    session: aioboto3.Session
    mock_s3: S3Client
    mock_ses: SESClient
    mock_lambda: LambdaClient
    mock_iam: IAMClient
    mock_sts: STSClient

    @classmethod
    @asynccontextmanager
    async def create(cls, service_url: str) -> AsyncIterator["MockAWS"]:
        """
        Create a new MockAWS instance with initialized clients.
        """
        session = MockSessionOverride(moto_url=service_url)

        async with (
            session.client("s3") as s3,
            session.client("ses") as ses,
            session.client("lambda") as lambda_client,
            session.client("iam") as iam,
            session.client("sts") as sts,
        ):
            yield cls(
                session=session,
                mock_s3=s3,
                mock_ses=ses,
                mock_lambda=lambda_client,
                mock_iam=iam,
                mock_sts=sts,
            )

    async def mock_lambda_response(
        self,
        function_name: str,
        response_payload: dict,
    ):
        """
        Running twice will override the previous mock.

        """
        # Construct the endpoint URL using the currently running moto server URL.
        endpoint = f"{self.session.moto_url}/moto-api/static/lambda-simple/response"  # type: ignore

        # Prepare the expected results as a queue (here, a single response).
        expected_results = {
            "results": [json_dumps(response_payload)],
            "region": MOCK_REGION,
        }

        async with aiohttp.ClientSession() as client:
            response = await client.post(endpoint, json=expected_results)
            assert response.status == 201


class MotoServerManager:
    """
    Manages the lifecycle of moto server processes and their endpoints.
    """

    def __init__(self) -> None:
        self.processes: list[Popen[Any]] = []
        self._proxy_bypass = {
            "http": None,
            "https": None,
        }

    async def start_service(self, host: str = "localhost") -> str:
        """
        Start a moto server for all services (moto 5.0+).
        Returns the URL of the started service.
        """
        port = self.get_free_port()
        self.url = f"http://{host}:{port}"

        moto_svr_path = shutil.which("moto_server")
        if not moto_svr_path:
            raise ValueError(
                "Could not find moto_server in PATH. Make sure moto[server] is installed in the virtualenv."
            )

        args = [moto_svr_path, "-H", host, "-p", str(port)]

        LOGGER.info(f"Starting moto server: {args}")
        process = subprocess.Popen(
            args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        self.processes.append(process)

        # Wait for server to start
        await self._wait_for_server(process)
        return self.url

    async def configure_service(
        self,
        batch_use_docker: bool = True,
        lambda_use_docker: bool = True,
        stepfunctions_execute_state_machine: bool = True,
    ):
        options = {
            "batch": {"use_docker": batch_use_docker},
            "lambda": {"use_docker": lambda_use_docker},
            "stepfunctions": {
                "execute_state_machine": stepfunctions_execute_state_machine
            },
        }
        async with aiohttp.ClientSession() as client:
            response = await client.post(f"{self.url}/moto-api/config", json=options)
            assert response.status == 201

    def stop_all(self) -> None:
        """
        Stop all running moto server processes.
        """
        for process in self.processes:
            self.stop_process(process)
        self.processes.clear()

    @staticmethod
    def get_free_port() -> int:
        """
        Find and return a free TCP port number.
        """
        with socket.socket() as sock:
            sock.bind(("", 0))
            _, port = sock.getsockname()
            return int(port)

    @staticmethod
    def stop_process(process: Popen[Any]) -> None:
        """
        Safely stop a moto server process.
        """
        try:
            process.send_signal(signal.SIGTERM)
            process.communicate(timeout=20)
        except subprocess.TimeoutExpired as te:
            process.kill()
            outs, errors = process.communicate(timeout=20)
            msg = f"Process {process.pid} failed to terminate cleanly: {outs} {errors}"
            raise RuntimeError(msg) from te

    async def _wait_for_server(
        self,
        process: Popen[Any],
        max_retries: int = 10,
        timeout: float = 5.0,
    ) -> None:
        """
        Wait for the moto server to become available.
        """
        async with aiohttp.ClientSession() as session:
            for _ in range(max_retries):
                if process.poll() is not None:
                    stdout, stderr = process.communicate()
                    LOGGER.error(f"moto_server stdout: {stdout!s}")
                    LOGGER.error(f"moto_server stderr: {stderr!s}")
                    raise RuntimeError("moto_server failed to start")

                try:
                    async with session.get(
                        self.url, timeout=aiohttp.ClientTimeout(total=timeout)
                    ):
                        break
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    await asyncio.sleep(0.5)
            else:
                MotoServerManager.stop_process(process)
                raise RuntimeError("Timeout waiting for moto service")

        LOGGER.info(f"Connected to moto server at {self.url}")
