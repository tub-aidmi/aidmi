"""Backwards-compatible entry point for the fixture generator."""

from aidmi_orchestrator.scripts.fixtures_gen.build import (  # noqa: F401
    FIXTURES_DIR,
    build_fixture,
    main,
)

if __name__ == "__main__":
    main()
