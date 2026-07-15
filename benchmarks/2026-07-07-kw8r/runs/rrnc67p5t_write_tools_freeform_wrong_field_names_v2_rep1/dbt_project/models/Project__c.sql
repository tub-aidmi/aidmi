{{ config(materialized='table') }}

SELECT
    CAST('a00' || p.proj_id AS TEXT) AS "Id",
    p.name AS "Name",
    CASE 
        WHEN UPPER(TRIM(p.status)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(p.status)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(p.status)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(p.status)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(p.status)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    p.go_live AS "Go_Live_Date__c",
    CAST('001' || k.kunden_nr AS TEXT) AS "Account__c",
    CAST('006' || c.chance_id AS TEXT) AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON p.kd = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c 
    ON p.opp = c.chance_id
