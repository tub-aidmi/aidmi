{{ config(materialized='table') }}

SELECT
    CAST(p.id AS TEXT) AS "Id",
    CAST(COALESCE(TRIM(p.name), 'Unknown') AS TEXT) AS "Name",
    CASE 
        WHEN LOWER(TRIM(p.status)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(p.status)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    al.id AS "Account__c",
    ol.id AS "Opportunity__c",
    CAST(p.id AS TEXT) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} al
    ON LOWER(TRIM(p.client_id)) = LOWER(TRIM(al.id))
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} ol
    ON LOWER(TRIM(p.opportunity_ref)) = LOWER(TRIM(ol.id))