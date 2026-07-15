{{ config(materialized='table') }}

SELECT
    'a00' || LPAD(CAST(SUBSTRING(p.proj_id FROM 6) AS INTEGER), 12, '0') AS "Id",
    p.name AS "Name",
    CASE
        WHEN UPPER(TRIM(p.status)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(p.status)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(p.status)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(p.status)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(p.status)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(p.go_live, 'YYYY-MM-DD')::text
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(p.go_live, 'DD.MM.YYYY')::text
        WHEN p.go_live ~ '^\d{4}/\d{2}/\d{2}$' THEN TO_DATE(p.go_live, 'YYYY/MM/DD')::text
        WHEN p.go_live ~ '^\d{8}$' THEN TO_DATE(p.go_live, 'YYYYMMDD')::text
        ELSE NULL
    END AS "Go_Live_Date__c",
    '001' || SUBSTRING(MD5(k.kunden_nr::text) FROM 1 FOR 14)::text AS "Account__c",
    '001' || SUBSTRING(MD5(c.chance_id::text) FROM 1 FOR 14)::text AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(p.kd) = TRIM(k.kunden_nr)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
    ON TRIM(p.opp) = TRIM(c.chance_id)