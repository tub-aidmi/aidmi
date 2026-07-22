{{ config(materialized='table') }}
SELECT 
    proj.proj_id AS "Id",
    proj.name AS "Name",
    CASE 
        WHEN INITCAP(TRIM(proj.status)) IN ('Active', 'Completed', 'In Planning', 'On Hold', 'Cancelled') 
        THEN INITCAP(TRIM(proj.status))
        ELSE NULL 
    END AS "Project_Status__c",
    CASE 
        WHEN proj.go_live ~ '^\d{4}-\d{2}-\d{2}$' 
        THEN proj.go_live 
        ELSE NULL 
    END AS "Go_Live_Date__c",
    MD5(kunden.kunden_nr) || '000000000000' AS "Account__c",
    MD5(chancen.chance_id) || '000000000000' AS "Opportunity__c",
    proj.proj_id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} proj
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} kunden ON proj.kd = kunden.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} chancen ON proj.opp = chancen.chance_id