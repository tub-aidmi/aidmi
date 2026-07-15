{{ config(materialized='table') }}

SELECT 
    '001' || RIGHT('000000' || REGEXP_REPLACE(p.proj_id, '[^0-9]', '', 'g'), 6) AS "Id",
    p.name AS "Name",
    CASE 
        WHEN TRIM(LOWER(p.status)) = 'active' THEN 'Active'
        WHEN TRIM(LOWER(p.status)) = 'completed' THEN 'Completed'
        WHEN TRIM(LOWER(p.status)) = 'in planning' THEN 'In Planning'
        WHEN TRIM(LOWER(p.status)) = 'on hold' THEN 'On Hold'
        WHEN TRIM(LOWER(p.status)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live IS NOT NULL AND p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        WHEN p.go_live IS NOT NULL AND p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live IS NOT NULL AND p.go_live ~ '^\d{8}$' THEN SUBSTR(p.go_live, 1, 4) || '-' || SUBSTR(p.go_live, 5, 2) || '-' || SUBSTR(p.go_live, 7, 2)
        ELSE NULL
    END AS "Go_Live_Date__c",
    '001' || RIGHT('000000' || REGEXP_REPLACE(p.kd, '[^0-9]', '', 'g'), 6) AS "Account__c",
    CASE 
        WHEN p.opp IS NOT NULL THEN '001' || RIGHT('000000' || REGEXP_REPLACE(p.opp, '[^0-9]', '', 'g'), 6)
        ELSE NULL
    END AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
