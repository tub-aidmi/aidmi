{{ config(materialized='table') }}

SELECT
    MD5(p.proj_id) AS "Id",
    COALESCE(TRIM(p.name), 'Unknown Project') AS "Name",
    COALESCE(
        CASE UPPER(TRIM(p.status))
            WHEN 'ACTIVE' THEN 'Active'
            WHEN 'COMPLETED' THEN 'Completed'
            WHEN 'IN PLANNING' THEN 'In Planning'
            WHEN 'ON HOLD' THEN 'On Hold'
            WHEN 'CANCELLED' THEN 'Cancelled'
            ELSE 'In Planning'
        END,
        'In Planning'
    ) AS "Project_Status__c",
    TO_CHAR(
        CASE
            WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live::DATE
            WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(p.go_live, 'DD.MM.YYYY')
            WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(p.go_live, 'MM/DD/YYYY')
            ELSE NULL
        END,
        'YYYY-MM-DD'
    ) AS "Go_Live_Date__c",
    MD5(k.kunden_nr) AS "Account__c",
    MD5(c.chance_id) AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON p.kd = k.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
    ON p.opp = c.chance_id
