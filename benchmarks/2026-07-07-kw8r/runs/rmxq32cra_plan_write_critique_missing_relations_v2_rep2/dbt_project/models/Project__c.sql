{{ config(materialized='table') }}

SELECT 
    TRIM(p.id) AS "Id",
    INITCAP(TRIM(COALESCE(p.name, ''))) AS "Name",
    CASE 
        WHEN UPPER(TRIM(p.status)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(p.status)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(p.status)) IN ('IN PLANNING', 'PLANNING') THEN 'In Planning'
        WHEN UPPER(TRIM(p.status)) IN ('ON HOLD', 'HOLD') THEN 'On Hold'
        WHEN UPPER(TRIM(p.status)) IN ('CANCELLED', 'CANCELED') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live IS NULL OR TRIM(p.go_live) = '' THEN NULL
        WHEN TRIM(p.go_live) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(p.go_live), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(p.go_live) ~ '^\d{8}$' THEN TO_DATE(TRIM(p.go_live), 'YYYYMMDD')::TEXT
        WHEN TRIM(p.go_live) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(p.go_live), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(p.go_live) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(p.go_live), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(UPPER(a.id)) AS "Account__c",
    TRIM(UPPER(o.id)) AS "Opportunity__c",
    TRIM(p.id) AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON TRIM(UPPER(p.client_id)) = TRIM(UPPER(a.id))
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o 
    ON TRIM(UPPER(p.opportunity_ref)) = TRIM(UPPER(o.id))