{{ config(materialized='table') }}

SELECT
    'proj_' || p.proj_id AS "Id",
    p.name AS "Name",
    CASE
        WHEN LOWER(TRIM(p.status)) IN ('active') THEN 'Active'
        WHEN LOWER(TRIM(p.status)) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) IN ('geplant', 'planning') THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) IN ('pause', 'on hold') THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) IN ('storniert', 'cancelled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(p.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    'acc_' || k.kunden_nr AS "Account__c",
    'opp_' || c.chance_id AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON p.kd = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c ON p.opp = c.chance_id