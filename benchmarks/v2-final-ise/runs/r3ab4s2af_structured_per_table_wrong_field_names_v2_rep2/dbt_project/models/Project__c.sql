{{ config(materialized='table') }}

WITH proj_data AS (
    SELECT 
        p.proj_id,
        p.name,
        p.status,
        p.go_live,
        p.kd,
        p.opp,
        k.kunden_nr AS account_kunden_nr,
        c.chance_id AS opportunity_chance_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
        ON p.kd = k.kunden_nr
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c 
        ON p.opp = c.chance_id
)

SELECT 
    proj_id AS "Id",
    name AS "Name",
    CASE 
        WHEN LOWER(TRIM(status)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(status)) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(TRIM(status)) IN ('in planning', 'in planung') THEN 'In Planning'
        WHEN LOWER(TRIM(status)) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(status)) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live
        WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    account_kunden_nr AS "Account__c",
    opportunity_chance_id AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    '2023-01-01' AS "CreatedDate",
    '2023-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM proj_data