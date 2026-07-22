{{ config(materialized='table') }}

SELECT
    projekt_kennung AS "Id",
    COALESCE(TRIM(projektname), 'Unknown Project') AS "Name",
    CASE
        WHEN UPPER(TRIM(projektstatus)) IN ('ACTIVE', 'AKTIV', 'IN BEARBEITUNG') THEN 'Active'
        WHEN UPPER(TRIM(projektstatus)) IN ('INACTIVE', 'INAKTIV') THEN 'On Hold'
        WHEN UPPER(TRIM(projektstatus)) = 'PENDING' THEN 'In Planning'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_datum -- YYYY-MM-DD
        WHEN go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
        WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- M/D/YYYY or MM/DD/YYYY
        WHEN go_live_datum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(kunden_kennung) AS "Account__c",
    TRIM(opp_kennung_ref) AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    '2023-01-01' AS "CreatedDate",
    '2023-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_projekte') }}
