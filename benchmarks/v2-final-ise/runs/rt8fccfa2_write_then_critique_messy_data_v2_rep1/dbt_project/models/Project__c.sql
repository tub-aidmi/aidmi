{{ config(materialized='table') }}
SELECT
    p."id" AS "Id",
    COALESCE(NULLIF(TRIM(p."name"), ''), 'Unnamed Project') AS "Name",
    CASE
        WHEN TRIM(LOWER(p."project_status__c")) IN ('active', 'completed', 'in planning', 'on hold', 'cancelled')
        THEN INITCAP(TRIM(LOWER(p."project_status__c")))
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p."go_live_date__c" ~ '^\d{4}-\d{2}-\d{2}$' THEN p."go_live_date__c"
        WHEN p."go_live_date__c" ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p."go_live_date__c", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p."go_live_date__c" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p."go_live_date__c", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p."go_live_date__c" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p."go_live_date__c", 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    p."account__c" AS "Account__c",
    p."opportunity__c" AS "Opportunity__c",
    p."id" AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }} p