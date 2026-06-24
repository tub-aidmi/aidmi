DBT_PROJECT_GUIDELINES = """\
## dbt project rules

- Use `{{ config(materialized='table') }}` at the top of each model.
- Use `{{ source('<source_slug>', '<table>') }}` where `<source_slug>` is the dbt source slug from context (shown in the prompt).
- Declare sources in `sources.yml` with `schema: "<source_schema>"` as a YAML string — the physical Postgres schema where raw source tables live (from context). Do not point sources at `{{ target.schema }}`; that is the per-run output schema.
- Map each target table from source tables only. Do NOT use `{{ ref('OtherModel') }}` or join to other target models during initial writes — sibling models may not exist yet. Resolve relationships via source keys and joins.
- One model file per target table under `models/`.
"""
