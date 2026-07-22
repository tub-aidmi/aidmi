"""ensemble_vote: N candidates per table, judge picks the winner."""

from __future__ import annotations

import asyncio

import pytest
from aidmi_orchestrator.domain import ModelSpec
from aidmi_orchestrator.strategy.ensemble_vote.strategy import (
    EnsembleVote,
    EnsembleVoteConfig,
    pick_candidate,
)
from aidmi_orchestrator.strategy.structured_common import TableMapping
from pydantic import ValidationError
from pydantic_ai.models.test import TestModel

from .test_structured_common import MAPPING_ARGS, fake_api


def _mapping(sql: str) -> TableMapping:
    return TableMapping(target_table="users", dbt_sql=sql, column_notes=[])


def test_pick_candidate_clamps_out_of_range_index() -> None:
    candidates = [_mapping("a"), _mapping("b")]
    assert pick_candidate(candidates, 1).dbt_sql == "b"
    assert pick_candidate(candidates, 99).dbt_sql == "b"
    assert pick_candidate(candidates, -3).dbt_sql == "a"


def test_generate_judges_and_writes_winner(tmp_path) -> None:
    writer = TestModel(custom_output_args=MAPPING_ARGS)
    judge = TestModel(
        custom_output_args={"chosen_index": 0, "justification": "cleanest cast"}
    )
    roles: list[str] = []

    def make_llm(spec, role):
        roles.append(role)
        return judge if role == "judge" else writer

    api = fake_api(tmp_path, make_llm=make_llm)
    strategy = EnsembleVote(
        EnsembleVoteConfig(
            writer_model=ModelSpec(provider="litellm", model_name="m"),
            n_candidates=3,
        )
    )
    result = asyncio.run(strategy.generate(api))
    assert result.self_reported_status == "complete"
    assert set(roles) == {"writer", "judge"}
    assert (tmp_path / "models" / "users.sql").exists()
    assert "cleanest cast" in result.manifest.tables[0].reasoning


def test_n_candidates_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        EnsembleVoteConfig(
            writer_model=ModelSpec(provider="litellm", model_name="m"),
            n_candidates=0,
        )
