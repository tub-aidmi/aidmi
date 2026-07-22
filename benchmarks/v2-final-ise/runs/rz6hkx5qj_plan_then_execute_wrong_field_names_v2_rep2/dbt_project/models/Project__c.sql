{{ config(materialized='table') }}

SELECT
    'a7Proj_' || TRIM(p.proj_id) AS "Id",
    INITCAP(TRIM(p.name)) AS "Name",
    CASE 
        WHEN LOWER(TRIM(p.status)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(p.status)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live IS NOT NULL AND TRIM(p.go_live) != '' THEN
            COALESCE(
                TO_CHAR(TO_DATE(TRIM(p.go_live), 'YYYY-MM-DD'), 'YYYY-MM-DD'),
                TO_CHAR(TO_DATE(TRIM(p.go_live), 'DD.MM.YYYY'), 'YYYY-MM-DD')
             )
        ELSE NULL
    END AS "Go_Live_Date__c",
    CASE WHEN k.kunden_nr IS NOT NULL THEN 'a01' || REGEXP_REPLACE(TRIM(k.kunden_nr), '[^0-9]', '') ELSE NULL END AS "Account__c",
    CASE WHEN c.chance_id IS NOT NULL THEN 'a06' || REGEXP_REPLACE(TRIM(c.chance_id), '[^0-9]', '') ELSE NULL END AS "Opportunity__c",
    TRIM(p.proj_id) AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(k.kunden_nr) = TRIM(p.kd)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c 
    ON TRIM(c.chance_id) = TRIM(p.opp)