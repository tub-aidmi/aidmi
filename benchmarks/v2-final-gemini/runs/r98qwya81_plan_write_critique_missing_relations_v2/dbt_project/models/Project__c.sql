{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(TRIM(p.name), 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(TRIM(p.status)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(p.status)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) = 'cancelled' THEN 'Cancelled'
        ELSE 'In Planning'
    END AS "Project_Status__c",
    TO_CHAR(
        COALESCE(
            CASE WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(p.go_live, 'YYYY-MM-DD') ELSE NULL END,
            CASE WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(p.go_live, 'DD.MM.YYYY') ELSE NULL END,
            CASE WHEN p.go_live ~ '^\d{8}$' THEN TO_DATE(p.go_live, 'YYYYMMDD') ELSE NULL END,
            CASE WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(p.go_live, 'MM/DD/YYYY') ELSE NULL END
        ),
        'YYYY-MM-DD'
    ) AS "Go_Live_Date__c",
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON p.client_id = a.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
    ON p.opportunity_ref = o.id
