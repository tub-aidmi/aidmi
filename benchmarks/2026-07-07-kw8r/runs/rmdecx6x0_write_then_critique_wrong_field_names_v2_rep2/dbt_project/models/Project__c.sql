{{ config(materialized='table') }}

SELECT
    CAST(p.proj_id AS TEXT) AS "Id",
    COALESCE(NULLIF(TRIM(p.name), ''), 'Unnamed Project') AS "Name",
    CASE LOWER(TRIM(p.status))
        WHEN 'active' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live IS NOT NULL AND p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(p.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN p.go_live IS NOT NULL AND p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live IS NOT NULL AND p.go_live ~ '^\d{8}$' THEN SUBSTR(p.go_live, 1, 4) || '-' || SUBSTR(p.go_live, 5, 2) || '-' || SUBSTR(p.go_live, 7, 2)
        ELSE NULL
    END AS "Go_Live_Date__c",
    CASE WHEN k.kunden_nr IS NOT NULL THEN '001' || SUBSTRING(MD5(k.kunden_nr), 1, 15) ELSE NULL END AS "Account__c",
    CAST(p.opp AS TEXT) AS "Opportunity__c",
    CAST(p.proj_id AS TEXT) AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON p.kd = k.kunden_nr