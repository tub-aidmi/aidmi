import os
from pathlib import Path

if "DOCKER_HOST" not in os.environ:
    _sock = Path(f"/run/user/{os.getuid()}/podman/podman.sock")
    if _sock.exists():
        os.environ["DOCKER_HOST"] = f"unix://{_sock}"

import pytest
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


@pytest.fixture
def staging_db_url(postgres_container):
    return postgres_container.get_connection_url(driver=None)
