{{ config(materialized='table') }}

SELECT
    CAST(p."id" AS text) AS "Id",
    INITCAP(p."name") AS "Name",
    CASE
        WHEN LOWER(TRIM(p."status")) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(p."status")) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(p."status")) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(p."status")) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(p."status")) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p."go_live" ~ '^\d{4}-\d{2}-\d{2}$' THEN p."go_live"
        ELSE CAST(TO_DATE(p."go_live", 'YYYY-MM-DD') AS text)
    END AS "Go_Live_Date__c",
    -- Account__c: transform ACC-XXXX to A0XXXXXXXX format via account lookup
    CASE
        WHEN a."id" IS NOT NULL THEN INITCAP('A' || SUBSTRING(a."id" FROM '\d+')::text)
        ELSE NULL
    END AS "Account__c",
    -- Opportunity__c: transform OPP-XXXXX to 006XXXXXXXX format
    CASE
        WHEN p."opportunity_ref" ~ '^OPP-\d+$' THEN '006' || SUBSTRING(p."opportunity_ref" FROM '\d+')
        ELSE NULL
    END AS "Opportunity__c",
    CAST(p."id" AS text) AS "Legacy_Project_ID__c",
    CAST('2024-01-01' AS text) AS "CreatedDate",
    CAST('2024-01-01' AS text) AS "LastModifiedDate",
    CASE WHEN p."id" IS NOT NULL THEN 0 ELSE 1 END AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON SUBSTRING(p."client_id" FROM '\d+') = SUBSTRING(a."id" FROM '\d+')
