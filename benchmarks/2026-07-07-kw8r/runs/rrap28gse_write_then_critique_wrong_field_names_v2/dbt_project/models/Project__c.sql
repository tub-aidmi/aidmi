{{ config(materialized='table') }}

SELECT 
    proj_id AS "Id",
    COALESCE(name, 'Unnamed Project') AS "Name",
    CASE LOWER(TRIM(status))
        WHEN 'active' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live IS NULL THEN NULL
        WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(go_live, 'YYYY-MM-DD')::TEXT
        WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live, 'DD.MM.YYYY')::TEXT
        WHEN go_live ~ '^\d{8}$' THEN TO_DATE(go_live, 'YYYYMMDD')::TEXT
        WHEN go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(go_live, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    '001' || REGEXP_REPLACE(kunden.kunden_nr, '\D', '', 'g') AS "Account__c",
    opp AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} proj
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} kunden 
    ON proj.kd = kunden.kunden_nr