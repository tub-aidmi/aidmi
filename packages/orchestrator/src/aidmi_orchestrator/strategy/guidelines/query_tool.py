QUERY_TOOL_GUIDELINES = """\
## query_postgres tool rules

- `query_postgres(sql)` runs plain PostgreSQL against staging — NOT dbt. Never pass `{{ source(...) }}` or other Jinja to query_postgres.
- Use quoted schema and table names, e.g. `SELECT * FROM "my_source_schema"."my_table" LIMIT 10`.
- When introspecting output tables, quote mixed-case schema, table, and column names exactly as materialized.
"""
