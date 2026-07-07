{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(TRIM(p.name), 'Unnamed Project') AS "Name",
    CASE UPPER(TRIM(p.status))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'IN PLANNING' THEN 'In Planning'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'CANCELLED' THEN 'Cancelled'
        ELSE 'In Planning'
    END AS "Project_Status__c",
    TO_CHAR(
        COALESCE(
            CASE WHEN TRIM(p.go_live) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(p.go_live), 'YYYY-MM-DD') ELSE NULL END,
            CASE WHEN TRIM(p.go_live) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(p.go_live), 'DD.MM.YYYY') ELSE NULL END,
            CASE WHEN TRIM(p.go_live) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(p.go_live), 'MM/DD/YYYY') ELSE NULL END
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
