{{ config(materialized='table') }}

SELECT
    MD5(TRIM(projekt_kennung)) AS "Id",
    COALESCE(TRIM(projektname), 'Unknown Project ' || TRIM(projekt_kennung)) AS "Name",
    CASE
        WHEN LOWER(TRIM(projektstatus)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(projektstatus)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(projektstatus)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(projektstatus)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(projektstatus)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(go_live_datum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_datum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(go_live_datum) ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_datum), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(go_live_datum) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_datum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(TRIM(kunden_kennung)) AS "Account__c",
    MD5(TRIM(opp_kennung_ref)) AS "Opportunity__c",
    TRIM(projekt_kennung) AS "Legacy_Project_ID__c",
    to_char(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "CreatedDate",
    to_char(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }}
