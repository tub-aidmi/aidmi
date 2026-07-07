{{ config(materialized='table') }}

SELECT
    proj.projekt_kennung AS "Id",
    COALESCE(TRIM(proj.projektname), 'Unknown Project') AS "Name",
    CASE
        WHEN INITCAP(TRIM(proj.projektstatus)) = 'Active' THEN 'Active'
        WHEN INITCAP(TRIM(proj.projektstatus)) = 'Completed' THEN 'Completed'
        WHEN INITCAP(TRIM(proj.projektstatus)) = 'In Planning' THEN 'In Planning'
        WHEN INITCAP(TRIM(proj.projektstatus)) = 'On Hold' THEN 'On Hold'
        WHEN INITCAP(TRIM(proj.projektstatus)) = 'Cancelled' THEN 'Cancelled'
        ELSE 'In Planning' -- Default for NOT NULL enum
    END AS "Project_Status__c",
    CASE
        WHEN proj.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    cust.kundennummer AS "Account__c",
    opp.opp_kennung AS "Opportunity__c",
    proj.projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS proj
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS cust
    ON proj.kunden_kennung = cust.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opp
    ON proj.opp_kennung_ref = opp.opp_kennung