{{ config(materialized='table') }}

SELECT
    MD5(TRIM(p.proj_id)) AS "Id",
    COALESCE(TRIM(p.name), 'Unknown Project') AS "Name",
    CASE TRIM(LOWER(p.status))
        WHEN 'active' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    TO_CHAR(
        CASE
            WHEN TRIM(p.go_live) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(p.go_live), 'YYYY-MM-DD')
            WHEN TRIM(p.go_live) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(p.go_live), 'DD.MM.YYYY')
            WHEN TRIM(p.go_live) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(p.go_live), 'MM/DD/YYYY')
            WHEN TRIM(p.go_live) ~ '^\d{8}$' THEN TO_DATE(TRIM(p.go_live), 'YYYYMMDD')
            ELSE NULL
        END,
        'YYYY-MM-DD'
    ) AS "Go_Live_Date__c",
    MD5(TRIM(k.kunden_nr)) AS "Account__c",
    MD5(TRIM(c.chance_id)) AS "Opportunity__c",
    TRIM(p.proj_id) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON TRIM(p.kd) = TRIM(k.kunden_nr)
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
    ON TRIM(p.opp) = TRIM(c.chance_id)