{{ config(materialized='table') }}

-- This model needs a proper source definition in sources.yml for orders data.
-- For now, it's a placeholder to prevent a 'NoneType' error due to an empty file.
SELECT
    1 AS "dummy_column"
