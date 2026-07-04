TRANSFORMATION_GUIDELINES = """\
## Transformation rules

- Output column names and types must match the target spec exactly. Use double-quoted aliases for mixed-case names (see PostgreSQL rules).
- Model file body must be executable SQL only — no wrapper functions, markdown fences, or prose outside SQL comments.
- Populate every `Legacy_*__c` column from the source natural key (e.g. customer number, contact id). These legacy columns are used to verify row-level correctness.
- `AccountId` and `Account__c` must reference the Salesforce-style Account `Id`, not raw source customer numbers. Join or map source keys when formats differ.
- For enum-typed target columns, map source values into the declared enum domain. Normalize case and spelling. Apply a consistent fallback policy (NULL vs a sensible default) across all tables.
- Dates: accept multiple text formats in source data; output consistent ISO `YYYY-MM-DD` text where applicable. Prefer NULL over sentinel dates when source is missing or unparseable. Never cast raw date strings directly — parse with `TO_DATE`/`TO_CHAR` or regex guards first (e.g. `DD.MM.YYYY`, `YYYYMMDD`, `MM/DD/YYYY`).
- Amounts and currencies: strip currency symbols and text prefixes before casting. Handle locale-specific separators explicitly (e.g. European `1.234,56` vs `1234.56`). For dot+comma European values, remove thousand-separator dots then swap comma to decimal point before `::DOUBLE PRECISION`.
- Cross-table keys: use the same key transform everywhere a foreign key appears. Inspect sample rows or use query_postgres to detect prefix or format mismatches between related source columns — do not assume join columns match literally.
- NOT NULL target columns: satisfy constraints with meaningful defaults, not empty strings unless that is intentional.
- Use INITCAP / LOWER / TRIM for normalisation where the target spec or source data quality suggests it.
"""
