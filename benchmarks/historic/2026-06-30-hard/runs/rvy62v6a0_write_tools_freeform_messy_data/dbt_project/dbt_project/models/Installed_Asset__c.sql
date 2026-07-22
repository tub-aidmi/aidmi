{% set source_name %}{{ 'fixture_messy_data_src' }}{% endset %}
{% set table_name %}{{ 'Installed_Asset__c' }}{% endset %}

{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE("Name", 'Unknown') AS "Name", -- Target is NOT NULL
    "Serial_Number__c" AS "Serial_Number__c",
    COALESCE(
        CASE
            WHEN NULLIF(TRIM(REPLACE(COALESCE("Warranty_End_Date__c", ''), 'N/A', '')), '') ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(NULLIF(TRIM(REPLACE(COALESCE("Warranty_End_Date__c", ''), 'N/A', '')), ''), 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN NULLIF(TRIM(REPLACE(COALESCE("Warranty_End_Date__c", ''), 'N/A', '')), '') ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(NULLIF(TRIM(REPLACE(COALESCE("Warranty_End_Date__c", ''), 'N/A', '')), ''), 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN NULLIF(TRIM(REPLACE(COALESCE("Warranty_End_Date__c", ''), 'N/A', '')), '') ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(NULLIF(TRIM(REPLACE(COALESCE("Warranty_End_Date__c", ''), 'N/A', '')), ''), 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN NULLIF(TRIM(REPLACE(COALESCE("Warranty_End_Date__c", ''), 'N/A', '')), '') ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(NULLIF(TRIM(REPLACE(COALESCE("Warranty_End_Date__c", ''), 'N/A', '')), ''), 'DD.MM.YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        NULL -- Default to NULL if unparseable, as target is not NOT NULL
    ) AS "Warranty_End_Date__c",
    "Account__c" AS "Account__c",
    "Project__c" AS "Project__c",
    NULL AS "Legacy_Asset_ID__c", -- Not in source, default to NULL
    NULL AS "CreatedDate", -- Not in source, default to NULL
    NULL AS "LastModifiedDate", -- Not in source, default to NULL
    0 AS "IsDeleted" -- Not in source, default to 0 (false)
FROM {{ source(source_name, table_name) }}
