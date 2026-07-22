{{ config(materialized='table') }}

SELECT
    TRIM(p.proj_id) AS "Id",
    INITCAP(TRIM(p.name)) AS "Name",
    CASE
        WHEN LOWER(TRIM(p.status)) IN ('aktiv', 'active') THEN 'Active'
        WHEN LOWER(TRIM(p.status)) IN ('abgeschlossen', 'fertig', 'completed') THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) IN ('in planung', 'in planning') THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) IN ('pausiert', 'angehalten', 'on hold') THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) IN ('storniert', 'abgebrochen', 'cancelled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    TO_DATE(NULLIF(TRIM(p.go_live), ''), 'YYYY-MM-DD')::TEXT AS "Go_Live_Date__c",
    TRIM(k.kunden_nr) AS "Account__c",
    TRIM(c.chance_id) AS "Opportunity__c",
    TRIM(p.proj_id) AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(p.kd) = TRIM(k.kunden_nr)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
    ON TRIM(p.opp) = TRIM(c.chance_id)