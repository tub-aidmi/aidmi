{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(NULLIF(p.name, ''), 'Unknown') AS "Name",
    CASE
        WHEN LOWER(TRIM(p.status)) IN ('active') THEN 'Active'
        WHEN LOWER(TRIM(p.status)) IN ('completed', 'done') THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) IN ('in planning', 'planning') THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) IN ('on hold', 'hold') THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) IN ('cancelled', 'canceled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live IS NOT NULL AND TRIM(p.go_live) != '' THEN
            TO_CHAR(
                TO_DATE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(p.go_live, '([0-9]{2})[/.]([0-9]{2})[/.]([0-9]{4})', '\3-\2-\1'),
                        '([0-9]{4})([0-9]{2})([0-9]{2})',
                        '\1-\2-\3'
                    ),
                    'YYYY-MM-DD'
                ),
                'YYYY-MM-DD'
            )
        ELSE NULL
    END AS "Go_Live_Date__c",
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON p.client_id = a.id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
    ON p.opportunity_ref = o.id
