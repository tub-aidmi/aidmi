{{ config(materialized='table') }}

SELECT
    p.projekt_kennung AS "Id",
    COALESCE(p.projektname, 'Unnamed Project ' || p.projekt_kennung) AS "Name",
    CASE
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('COMPLETED', 'ABGESCHLOSSEN') THEN 'Completed'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('IN PLANNING', 'IN PLANUNG') THEN 'In Planning'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('ON HOLD', 'PAUSIERT') THEN 'On Hold'
        WHEN UPPER(TRIM(p.projektstatus)) IN ('CANCELLED', 'STORNIERT') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    SHA256(p.kunden_kennung::bytea)::text AS "Account__c",
    SHA256(p.opp_kennung_ref::bytea)::text AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p
