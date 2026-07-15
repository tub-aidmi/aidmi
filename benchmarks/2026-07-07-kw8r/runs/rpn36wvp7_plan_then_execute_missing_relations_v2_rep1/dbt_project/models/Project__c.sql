{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(p.name, 'Unknown Project') AS "Name",
    CASE LOWER(TRIM(p.status))
        WHEN 'active' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN TO_DATE(p.go_live, 'YYYY-MM-DD')::TEXT
        WHEN p.go_live ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{4}$' THEN TO_DATE(p.go_live, 'DD.MM.YYYY')::TEXT
        ELSE NULL 
    END AS "Go_Live_Date__c",
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON TRIM(p.client_id) = TRIM(a.id)
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o 
    ON TRIM(p.opportunity_ref) = TRIM(o.id)