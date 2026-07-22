{{ config(materialized='table') }}

SELECT 
    UPPER(TRIM(p.proj_id)) AS "Id",
    COALESCE(INITCAP(TRIM(p.name)), 'Unknown Project') AS "Name",
    CASE UPPER(TRIM(p.status))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'IN PLANNING' THEN 'In Planning'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(p.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')::TEXT
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    UPPER(TRIM(k.kunden_nr)) AS "Account__c",
    UPPER(TRIM(c.chance_id)) AS "Opportunity__c",
    UPPER(TRIM(p.proj_id)) AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON UPPER(TRIM(p.kd)) = UPPER(TRIM(k.kunden_nr))
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c 
    ON UPPER(TRIM(p.opp)) = UPPER(TRIM(c.chance_id))