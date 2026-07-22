{{ config(materialized='table') }}

SELECT 
    CAST(proj_id AS TEXT) AS "Id",
    INITCAP(TRIM(name)) AS "Name",
    CASE
        WHEN LOWER(TRIM(status)) = 'aktiv' THEN 'Active'
        WHEN LOWER(TRIM(status)) IN ('abgeschlossen', 'fertig', 'beendet') THEN 'Completed'
        WHEN LOWER(TRIM(status)) IN ('in planung', 'planung') THEN 'In Planning'
        WHEN LOWER(TRIM(status)) IN ('pausiert', 'gesperrt', 'gehalten') THEN 'On Hold'
        WHEN LOWER(TRIM(status)) IN ('storniert', 'abgebrochen', 'cancelled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live, 'DD.MM.YYYY')::TEXT
        WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live
        ELSE NULL
    END AS "Go_Live_Date__c",
    CONCAT('A-', TRIM(kd)) AS "Account__c",
    CONCAT('O-', TRIM(opp)) AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
WHERE TRIM(proj_id) != ''