from __future__ import annotations
import statistics
from collections import defaultdict
from typing import Callable, Hashable
from aidmi_orchestrator.report.data import RunRecord


def group_mean(records, key, metric):
    acc = defaultdict(list)
    for r in records:
        v = metric(r)
        if v is not None:
            acc[key(r)].append(v)
    return {k: sum(v) / len(v) for k, v in acc.items() if v}


def group_mean_zero(records, key, metric):
    """Group mean counting a missing (None) metric as 0.

    Unlike group_mean, a run that produced nothing evaluable is not dropped; it
    contributes 0 to the mean. This is the honest denominator for coverage
    metrics (recall, f1) where None means "recovered nothing", so dropping it
    would inflate the average by hiding failed runs.
    """
    acc = defaultdict(list)
    for r in records:
        v = metric(r)
        acc[key(r)].append(v if v is not None else 0.0)
    return {k: sum(v) / len(v) for k, v in acc.items() if v}


def pass_rate(records, key, predicate):
    hit = defaultdict(int)
    tot = defaultdict(int)
    for r in records:
        tot[key(r)] += 1
        if predicate(r):
            hit[key(r)] += 1
    return {k: hit[k] / tot[k] for k in tot}


def rep_values(records, key, metric):
    acc = defaultdict(list)
    for r in records:
        v = metric(r)
        if v is not None:
            acc[key(r)].append(v)
    return dict(acc)


def materialization_rate(records, key):
    """Mean fraction of target tables materialized per run (a run that produced
    nothing counts as 0). Not a pass-rate: partial materialization contributes
    its actual fraction rather than a binary success."""
    return group_mean_zero(records, key, lambda r: r.tables_materialized)


def summary_stats(values):
    """(mean, median, population sd) over values; None if empty. sd is 0 for a
    single value. Population (not sample) sd matches _fmt_mean_sd elsewhere."""
    if not values:
        return None
    mean = sum(values) / len(values)
    median = statistics.median(values)
    sd = statistics.pstdev(values) if len(values) > 1 else 0.0
    return mean, median, sd
