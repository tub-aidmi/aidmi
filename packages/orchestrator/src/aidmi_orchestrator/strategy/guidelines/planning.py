PLANNING_GUIDELINES = """\
## Planning rules

- Produce a global mapping plan covering EVERY target table: which source tables feed it, join keys, and per-column transform hints.
- The overview must capture cross-table decisions: shared keys, consistent enum/date/amount logic, and foreign-key resolution strategy.
- Note per-table pitfalls (casts, enums, normalisation) without writing SQL.
- Be precise about column names; downstream writers follow the plan literally.
- Plan only — no SQL and no dbt Jinja in the plan output.
"""
