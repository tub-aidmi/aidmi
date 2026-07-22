-- models/Project__c.sql

{{ config(materialized='table') }}

SELECT
    projekt_kennung AS "Id",
    COALESCE(projektname, projekt_kennung) AS "Name",
    CASE
        WHEN LOWER(projektstatus) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(projektstatus) IN ('in bearbeitung', 'pending') THEN 'In Planning'
        WHEN LOWER(projektstatus) IN ('inactive', 'inaktiv') THEN 'Cancelled' -- Assuming inactive means cancelled
        ELSE NULL
    END AS "Project_Status__c",
    COALESCE(
        TO_CHAR(CAST(go_live_datum AS DATE), 'YYYY-MM-DD'), -- YYYY-MM-DD
        TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD'), -- DD.MM.YYYY
        TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD'), -- M/D/YYYY
        TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
    ) AS "Go_Live_Date__c",
    kunden_kennung AS "Account__c",
    opp_kennung_ref AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_projekte') }}
