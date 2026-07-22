{% set source_name %}{{ 'fixture_messy_data_src' }}{% endset %}
{% set table_name %}{{ 'Project__c' }}{% endset %}

{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE("Name", 'Unknown') AS "Name", -- Target is NOT NULL
    CASE
        WHEN TRIM(LOWER(COALESCE("Project_Status__c", ''))) IN ('active', 'aktiv', 'in bearbeitung') THEN 'Active'
        WHEN TRIM(LOWER(COALESCE("Project_Status__c", ''))) IN ('pending') THEN 'In Planning'
        WHEN TRIM(LOWER(COALESCE("Project_Status__c", ''))) IN ('inaktiv', 'inactive') THEN 'On Hold' -- Mapping 'Inactive' to 'On Hold'
        ELSE NULL
    END AS "Project_Status__c",
    COALESCE(
        CASE
            WHEN NULLIF(TRIM(REPLACE(COALESCE("Go_Live_Date__c", ''), 'N/A', '')), '') ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(NULLIF(TRIM(REPLACE(COALESCE("Go_Live_Date__c", ''), 'N/A', '')), ''), 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN NULLIF(TRIM(REPLACE(COALESCE("Go_Live_Date__c", ''), 'N/A', '')), '') ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(NULLIF(TRIM(REPLACE(COALESCE("Go_Live_Date__c", ''), 'N/A', '')), ''), 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN NULLIF(TRIM(REPLACE(COALESCE("Go_Live_Date__c", ''), 'N/A', '')), '') ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(NULLIF(TRIM(REPLACE(COALESCE("Go_Live_Date__c", ''), 'N/A', '')), ''), 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN NULLIF(TRIM(REPLACE(COALESCE("Go_Live_Date__c", ''), 'N/A', '')), '') ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(NULLIF(TRIM(REPLACE(COALESCE("Go_Live_Date__c", ''), 'N/A', '')), ''), 'DD.MM.YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        NULL -- Default to NULL if unparseable or N/A, as target is not NOT NULL
    ) AS "Go_Live_Date__c",
    "Account__c" AS "Account__c",
    "Opportunity__c" AS "Opportunity__c",
    NULL AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source(source_name, table_name) }}
