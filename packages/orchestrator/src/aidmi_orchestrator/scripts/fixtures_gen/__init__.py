"""Fixture generator internals.

The seed and the single `Faker` instance live here rather than in `build`, so that
every submodule can read them without importing `build` (which imports them all).
There must be exactly one `Faker` instance, and it must only ever be re-seeded by
`dataset.reset`.
"""

from __future__ import annotations

from faker import Faker

SEED = 42
fake = Faker("de_DE")
