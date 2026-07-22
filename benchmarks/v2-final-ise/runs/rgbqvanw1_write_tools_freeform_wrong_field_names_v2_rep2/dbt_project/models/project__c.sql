{{ config(materialized='table') }}

SELECT
    CONCAT('500', LEFT(MD5(proj_id), 17)) AS "Id",
    INITCAP(TRIM(name)) AS "Name",
    CASE
        WHEN UPPER(TRIM(status)) IN ('AKTIV', 'ACTIVE') THEN 'Active'
        WHEN UPPER(TRIM(status)) IN ('ABGESCHLOSSEN', 'COMPLETED', 'FERTIG') THEN 'Completed'
        WHEN UPPER(TRIM(status)) IN ('IN PLANUNG', 'IN PLANNING', 'PLANUNG') THEN 'In Planning'
        WHEN UPPER(TRIM(status)) IN ('PAUSIERT', 'ON HOLD', 'GEPARKT') THEN 'On Hold'
        WHEN UPPER(TRIM(status)) IN ('GESTRICHT', 'CANCELLED', 'ABGELEHNT') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live, 'DD.MM.YYYY')::TEXT
        WHEN go_live ~ '^\d{8}$' THEN TO_DATE(go_live, 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    CONCAT('001', LEFT(MD5(kd), 17)) AS "Account__c",
    CONCAT('006', LEFT(MD5(opp), 17)) AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
