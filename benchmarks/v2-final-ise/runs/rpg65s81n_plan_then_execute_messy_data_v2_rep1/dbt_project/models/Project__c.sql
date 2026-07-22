{{ config(materialized='table') }}

SELECT
    UPPER(TRIM(REGEXP_REPLACE(p.id, '^[^a-zA-Z0-9]+', '', 'g'))) AS "Id",
    INITCAP(TRIM(COALESCE(p.name, 'Unnamed Project'))) AS "Name",
    CASE 
        WHEN LOWER(TRIM(p.project_status__c)) IN ('active', 'completed', 'in planning', 'on hold', 'cancelled') THEN INITCAP(TRIM(p.project_status__c))
        ELSE 'In Planning'
    END AS "Project_Status__c",
    -- Parse go_live_date__c with guards against sentinel dates like 0000-00-00
    CASE 
        WHEN TRIM(p.go_live_date__c) = '' THEN NULL
        WHEN LOWER(TRIM(p.go_live_date__c)) IN ('null', 'none', '-', '', 'unknown') THEN NULL
        WHEN TRIM(p.go_live_date__c) ~ '^(\d{2}\.\d{2}\.\d{4}|\d{8}|\d{4}-\d{2}-\d{2})$' 
             AND TRIM(p.go_live_date__c) NOT LIKE '00%'
        THEN CASE 
            WHEN TRIM(p.go_live_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(p.go_live_date__c), 'DD.MM.YYYY')::TEXT
            WHEN TRIM(p.go_live_date__c) ~ '^\d{8}$' THEN TO_DATE(TRIM(p.go_live_date__c), 'YYYYMMDD')::TEXT
            ELSE CAST(CAST(TRIM(p.go_live_date__c) AS DATE) AS TEXT)
        END
        ELSE NULL 
    END AS "Go_Live_Date__c",
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }} p
LEFT JOIN (
    SELECT 
        id,
        UPPER(TRIM(REGEXP_REPLACE(id, '^[^a-zA-Z0-9]+', '', 'g'))) AS clean_key
    FROM {{ source('fixture_messy_data_v2_src', 'account') }}
) a
    ON UPPER(TRIM(REGEXP_REPLACE(p.account__c, '^[^a-zA-Z0-9]+', '', 'g'))) = a.clean_key
LEFT JOIN (
    SELECT 
        id,
        UPPER(TRIM(REGEXP_REPLACE(id, '^[^a-zA-Z0-9]+', '', 'g'))) AS clean_key
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
) o
    ON UPPER(TRIM(REGEXP_REPLACE(p.opportunity__c, '^[^a-zA-Z0-9]+', '', 'g'))) = o.clean_key