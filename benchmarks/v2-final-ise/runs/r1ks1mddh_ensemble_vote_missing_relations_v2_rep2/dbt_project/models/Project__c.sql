{{ config(materialized='table') }}

SELECT
    'a0G' || LPAD(REGEXP_REPLACE(p.id, '[^0-9]', '', 'g'), 12, '0') AS "Id",
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
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TO_CHAR(TO_DATE(p.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    CASE
        WHEN a.id IS NOT NULL
            THEN '001' || LPAD(REGEXP_REPLACE(a.id, '[^0-9]', '', 'g'), 12, '0')
        ELSE NULL
    END AS "Account__c",
    CASE
        WHEN o.id IS NOT NULL
            THEN '006' || LPAD(REGEXP_REPLACE(o.id, '[^0-9]', '', 'g'), 12, '0')
        ELSE NULL
    END AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON a.id = p.client_id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
    ON o.id = p.opportunity_ref