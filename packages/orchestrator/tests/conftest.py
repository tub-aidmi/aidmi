"""Shared pytest fixtures for orchestrator tests.

Reuses SP1's Podman-socket detection trick so testcontainers works on a dev
host without Docker but with rootless Podman.
"""

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


_DOCKER_FIXTURES = {"postgres_container", "staging_db_url"}


def pytest_collection_modifyitems(items):
    for item in items:
        if _DOCKER_FIXTURES & set(item.fixturenames):
            item.add_marker(pytest.mark.requires_docker)
