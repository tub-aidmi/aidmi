{{ config(materialized='table') }}

SELECT
    '01t' || MD5(p.proj_id) AS "Id",
    TRIM(p.name) AS "Name",
    CASE 
        WHEN UPPER(p.status) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(p.status) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(p.status) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(p.status) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(p.status) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        ELSE NULL
    END AS "Go_Live_Date__c",
    '001' || MD5(k.kunden_nr) AS "Account__c",
    '006' || MD5(c.chance_id) AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON p.kd = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c ON p.opp = c.chance_id
