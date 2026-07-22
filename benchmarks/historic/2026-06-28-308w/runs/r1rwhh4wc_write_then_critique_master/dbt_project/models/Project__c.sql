
{{ config(materialized='table') }}

WITH project_data AS (
    SELECT
        projekt_kennung,
        projektname,
        projektstatus,
        go_live_datum,
        kunden_kennung,
        opp_kennung_ref,
        CASE
            WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(go_live_datum, 'YYYY-MM-DD')
            WHEN go_live_datum ~ '^\d{8}$' THEN TO_DATE(go_live_datum, 'YYYYMMDD')
            WHEN go_live_datum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(go_live_datum, 'DD.MM.YYYY')
            WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(go_live_datum, 'MM/DD/YYYY')
            ELSE NULL
        END AS parsed_go_live_date
    FROM {{ source('fixture_master_src', 'master_projekte') }}
)
SELECT
    projekt_kennung AS "Id",
    COALESCE(projektname, 'N/A') AS "Name",
    CASE
        WHEN LOWER(projektstatus) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(projektstatus) IN ('in bearbeitung', 'pending') THEN 'In Planning'
        WHEN LOWER(projektstatus) IN ('inactive', 'inaktiv') THEN 'On Hold'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN parsed_go_live_date IS NOT NULL
        THEN TO_CHAR(parsed_go_live_date, 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    kunden_kennung AS "Account__c",
    opp_kennung_ref AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    project_data
