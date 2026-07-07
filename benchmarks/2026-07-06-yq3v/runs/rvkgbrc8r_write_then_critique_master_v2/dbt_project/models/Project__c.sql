-- depends_on: {{ source('fixture_master_v2_src', 'master_projekte') }}

{{ config(materialized='table') }}

SELECT
    projekt_kennung AS "Id",
    COALESCE(INITCAP(TRIM(projektname)), 'Untitled Project') AS "Name",
    CASE
        WHEN TRIM(projektstatus) ILIKE 'Active' THEN 'Active'
        WHEN TRIM(projektstatus) ILIKE 'Completed' THEN 'Completed'
        WHEN TRIM(projektstatus) ILIKE 'Complete' THEN 'Completed'
        WHEN TRIM(projektstatus) ILIKE 'In Planning' THEN 'In Planning'
        WHEN TRIM(projektstatus) ILIKE 'Planning' THEN 'In Planning'
        WHEN TRIM(projektstatus) ILIKE 'On Hold' THEN 'On Hold'
        WHEN TRIM(projektstatus) ILIKE 'Hold' THEN 'On Hold'
        WHEN TRIM(projektstatus) ILIKE 'Cancelled' THEN 'Cancelled'
        WHEN TRIM(projektstatus) ILIKE 'Canceled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_datum
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    kunden_kennung AS "Account__c",
    opp_kennung_ref AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }}