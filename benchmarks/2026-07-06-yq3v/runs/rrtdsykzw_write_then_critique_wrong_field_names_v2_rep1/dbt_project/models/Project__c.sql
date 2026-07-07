{{
    config(materialized='table')
}}

SELECT
    p.proj_id AS "Id",
    COALESCE(p.name, 'Unknown Project') AS "Name",
    CASE
        WHEN TRIM(LOWER(p.status)) = 'active' THEN 'Active'
        WHEN TRIM(LOWER(p.status)) = 'completed' THEN 'Completed'
        WHEN TRIM(LOWER(p.status)) = 'in planning' THEN 'In Planning'
        WHEN TRIM(LOWER(p.status)) = 'on hold' THEN 'On Hold'
        WHEN TRIM(LOWER(p.status)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live -- YYYY-MM-DD
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    a."Id" AS "Account__c",
    o."Id" AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
LEFT JOIN
    {{ ref('Account') }} AS a
    ON p.kd = a."Legacy_Customer_ID__c"
LEFT JOIN
    {{ ref('Opportunity') }} AS o
    ON p.opp = o."Legacy_Opportunity_ID__c"