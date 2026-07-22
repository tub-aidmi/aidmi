{{ config(materialized='table') }}
SELECT
    MD5(p.proj_id) AS "Id",
    TRIM(p.name) AS "Name",
    CASE
        WHEN TRIM(p.status) IN ('Active', 'Completed', 'In Planning', 'On Hold', 'Cancelled') THEN TRIM(p.status)
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(k.kunden_nr) AS "Account__c",
    MD5(c.chance_id) AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    '1970-01-01' AS "CreatedDate",
    '1970-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON p.kd = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c ON p.opp = c.chance_id