{{ config(materialized='table') }}

SELECT
    'PROJ_' || projekt_kennung AS "Id",
    projektname AS "Name",
    CASE 
        WHEN UPPER(projektstatus) IN ('AKTIV', 'ACTIVE') THEN 'Active'
        WHEN UPPER(projektstatus) IN ('ABGESCHLOSSEN', 'COMPLETED') THEN 'Completed'
        WHEN UPPER(projektstatus) IN ('IN PLANUNG', 'IN PLANNING') THEN 'In Planning'
        WHEN UPPER(projektstatus) IN ('IN WARTESTELLUNG', 'ON HOLD') THEN 'On Hold'
        WHEN UPPER(projektstatus) IN ('STORNIERT', 'CANCELLED') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live_datum ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    'ACC_' || kunden_kennung AS "Account__c",
    CASE 
        WHEN opp_kennung_ref IS NOT NULL THEN 'OPP_' || opp_kennung_ref
        ELSE NULL
    END AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
