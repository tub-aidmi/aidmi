TRANSFORMATION_GUIDELINES = """\
## Transformation rules

- Output column names and types must match the target spec exactly. Use double-quoted aliases for mixed-case names (see PostgreSQL rules).
- For enum-typed target columns, map source values into the declared enum domain. Normalize case and spelling. Apply a consistent fallback policy (NULL vs a sensible default) across all tables.
- Dates: accept multiple text formats in source data; output consistent ISO `YYYY-MM-DD` text where applicable. Prefer NULL over sentinel dates when source is missing or unparseable. Never cast raw date strings directly — parse with `TO_DATE`/`TO_CHAR` or regex guards first (e.g. `DD.MM.YYYY`, `YYYYMMDD`, `MM/DD/YYYY`).
- Amounts and currencies: strip currency symbols and text prefixes before casting. Handle locale-specific separators explicitly (e.g. European `1.234,56` vs `1234.56`). For dot+comma European values, remove thousand-separator dots then swap comma to decimal point before `::DOUBLE PRECISION`.
- Cross-table keys: use the same key transform everywhere a foreign key appears. Inspect sample rows or use query_postgres to detect prefix or format mismatches between related source columns — do not assume join columns match literally.
- NOT NULL target columns: satisfy constraints with meaningful defaults, not empty strings unless that is intentional.
- Use INITCAP / LOWER / TRIM for normalisation where the target spec or source data quality suggests it.
"""
