from __future__ import annotations
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

def pass_rate(records, key, predicate):
    hit = defaultdict(int); tot = defaultdict(int)
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
    return pass_rate(records, key, lambda r: r.materialized)
