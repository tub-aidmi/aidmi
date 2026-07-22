from aidmi_orchestrator.strategy.base import register_strategy
from aidmi_orchestrator.strategy.ensemble_vote.strategy import (
    EnsembleVote,
    EnsembleVoteConfig,
)

register_strategy("ensemble_vote", EnsembleVote, EnsembleVoteConfig)
