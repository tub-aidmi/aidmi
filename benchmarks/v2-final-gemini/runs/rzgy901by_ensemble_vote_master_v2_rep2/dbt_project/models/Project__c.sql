{{ config(materialized='table') }}

SELECT
    mp.projekt_kennung AS "Id",
    COALESCE(mp.projektname, mp.projekt_kennung) AS "Name",
    CASE
        WHEN TRIM(LOWER(mp.projektstatus)) IN ('active', 'in progress') THEN 'Active'
        WHEN TRIM(LOWER(mp.projektstatus)) IN ('completed', 'finished') THEN 'Completed'
        WHEN TRIM(LOWER(mp.projektstatus)) IN ('planned', 'pending', 'in planning') THEN 'In Planning'
        WHEN TRIM(LOWER(mp.projektstatus)) = 'on hold' THEN 'On Hold'
        WHEN TRIM(LOWER(mp.projektstatus)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN mp.go_live_datum = '0000-00-00' THEN NULL -- Handle specific problematic date value
        WHEN mp.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(mp.go_live_datum AS DATE), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mp.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(mp.kunden_kennung) AS "Account__c",
    MD5(mp.opp_kennung_ref) AS "Opportunity__c",
    mp.projekt_kennung AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp
WHERE mp.projekt_kennung IS NOT NULL
