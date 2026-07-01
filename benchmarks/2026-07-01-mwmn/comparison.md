# Mistral LiteLLM validation (ISE)

Same grid as [`2026-07-01-x535`](../2026-07-01-x535) (3 strategies × 2 v2 fixtures, hardening config) but with **nvidia/mistral-medium-3.5-128b** via ISE LiteLLM (`localhost:4000` SSH tunnel).

## Compare against

- Gemini hardening validation: [`2026-07-01-x535`](../2026-07-01-x535) — dbt_success **2/6**
- Pre-hardening baseline: [`2026-07-01-z848`](../2026-07-01-z848) — dbt_success **0/4**

## SSH tunnel

```bash
ssh -N -L 4000:elise.ise.tu-berlin.de:4000 jump@ssh-gateway.ise.tu-berlin.de -p 20122
```

Requires `LITELLM_API_KEY` in `.env`.

## x535 reference (gemini-2.5-flash)

| Fixture | Strategy | dbt_success | gt_recall | gt_tables_materialized |
|---|---|---|---|---|
| wrong_field_names_v2 | write_tools_freeform | 1 | 1.0 | 1.0 |
| wrong_field_names_v2 | plan_write_critique | 1 | 1.0 | 1.0 |
| wrong_field_names_v2 | write_then_critique | 0 | — | — |
| missing_relations_v2 | write_tools_freeform | 0 | — | — |
| missing_relations_v2 | write_then_critique | 0 | 0.0 | 0.0 |
| missing_relations_v2 | plan_write_critique | 0 | 0.70 | 0.6 |

## Mistral results

See [report/summary.csv](report/summary.csv) after sweep completes.
