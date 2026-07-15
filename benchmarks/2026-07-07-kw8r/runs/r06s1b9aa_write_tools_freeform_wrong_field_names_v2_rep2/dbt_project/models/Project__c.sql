{{ config(materialized='table') }}

SELECT
    p.proj_id AS "Id",
    TRIM(p.name) AS "Name",
    CASE 
        WHEN UPPER(TRIM(p.status)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(p.status)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(p.status)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(p.status)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(p.status)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        ELSE NULL
    END AS "Go_Live_Date__c",
    p.kd AS "Account__c",
    p.opp AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON p.kd = k.kunden_nr
