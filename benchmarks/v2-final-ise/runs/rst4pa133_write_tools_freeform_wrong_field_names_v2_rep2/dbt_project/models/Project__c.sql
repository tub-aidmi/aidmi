{{ config(materialized='table') }}

SELECT
    p.proj_id AS "Id",
    TRIM(p.name) AS "Name",
    CASE 
        WHEN UPPER(TRIM(p.status)) IN ('AKTIV', 'ACTIVE') THEN 'Active'
        WHEN UPPER(TRIM(p.status)) IN ('ABGESCHLOSSEN', 'COMPLETED') THEN 'Completed'
        WHEN UPPER(TRIM(p.status)) IN ('IN PLANUNG', 'IN PLANNING') THEN 'In Planning'
        WHEN UPPER(TRIM(p.status)) IN ('IN BEARBEITUNG', 'ON HOLD') THEN 'On Hold'
        WHEN UPPER(TRIM(p.status)) IN ('STORNIERT', 'CANCELLED') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    k.kunden_nr AS "Account__c",
    c.chance_id AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON p.kd = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c ON p.opp = c.chance_id
