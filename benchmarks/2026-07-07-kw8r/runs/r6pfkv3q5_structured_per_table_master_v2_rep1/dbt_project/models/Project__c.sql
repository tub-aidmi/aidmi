{{ config(materialized='table') }}

WITH normalized_dates AS (
    SELECT
        projekt_kennung,
        CASE
            WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_datum
            WHEN go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END AS parsed_go_live_date
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
)

SELECT
    mp.projekt_kennung AS "Id",
    mp.projektname AS "Name",
    CASE
        WHEN mp.projektstatus IN ('Active', 'active', 'Aktiv') THEN 'Active'
        WHEN mp.projektstatus IN ('Completed', 'completed', 'Abgeschlossen') THEN 'Completed'
        WHEN mp.projektstatus IN ('In Planning', 'In Planung', 'Planung') THEN 'In Planning'
        WHEN mp.projektstatus IN ('On Hold', 'on hold', 'Pausiert') THEN 'On Hold'
        WHEN mp.projektstatus IN ('Cancelled', 'cancelled', 'Storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    nd.parsed_go_live_date AS "Go_Live_Date__c",
    mk.kundennummer AS "Account__c",
    mo.opp_kennung AS "Opportunity__c",
    mp.projekt_kennung AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_projekte') }} mp
LEFT JOIN normalized_dates nd ON mp.projekt_kennung = nd.projekt_kennung
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk ON mp.kunden_kennung = mk.kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} mo ON mp.opp_kennung_ref = mo.opp_kennung