{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(NULLIF(TRIM(p.name), ''), 'Untitled Project') AS "Name",
    CASE
        WHEN UPPER(TRIM(p.status)) IN ('ACTIVE') THEN 'Active'
        WHEN UPPER(TRIM(p.status)) IN ('COMPLETED') THEN 'Completed'
        WHEN UPPER(TRIM(p.status)) IN ('IN PLANNING', 'PLANNING') THEN 'In Planning'
        WHEN UPPER(TRIM(p.status)) IN ('ON HOLD', 'ON_HOLD') THEN 'On Hold'
        WHEN UPPER(TRIM(p.status)) IN ('CANCELLED', 'CANCELED') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    COALESCE(
        (SELECT a.id FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a WHERE a.id = p.client_id LIMIT 1),
        (SELECT a.id FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a WHERE a.id = (SELECT c.account_ref FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c WHERE c.id = p.client_id LIMIT 1) LIMIT 1)
    ) AS "Account__c",
    COALESCE(
        (SELECT op.id FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} op WHERE op.id = p.opportunity_ref LIMIT 1)
    ) AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p