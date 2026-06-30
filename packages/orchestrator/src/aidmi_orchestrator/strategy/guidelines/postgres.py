POSTGRES_SQL_GUIDELINES = """\
## PostgreSQL SQL rules

- Use only PostgreSQL syntax. Do NOT use TRY_CAST, SAFE_CAST, ISNULL, NVL, or invented functions/macros.
- Model output must be executable SQL only — no markdown fences, prose, filenames, or non-ASCII commentary outside string literals.
- Quoted identifiers: PostgreSQL folds unquoted identifiers to lowercase. For mixed-case target column names, always alias with double quotes, e.g. `expr AS "ColumnName"` or `expr AS "Custom_Field__c"`.
- CAST syntax: `CAST(expr AS type)` requires the `AS type` clause. Inner CASE branches that already cast to the target type do not need a redundant outer CAST without `AS type`.
- To coerce text to integer when digits-only, use: `CASE WHEN col ~ '^\\d+$' THEN col::INTEGER ELSE NULL END`
- Use regex (`~`) and REGEXP_REPLACE for text cleanup before casting to numeric types.
"""
