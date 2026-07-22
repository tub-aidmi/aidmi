{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        p.projekt_kennung,
        p.projektname,
        p.projektstatus,
        p.go_live_datum,
        p.kunden_kennung,
        p.opp_kennung_ref,
        a.kundennummer AS account_kundennummer,
        o.opp_kennung AS opportunity_opp_kennung
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} a
        ON p.kunden_kennung = a.kundennummer
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} o
        ON p.opp_kennung_ref = o.opp_kennung
),

normalized AS (
    SELECT
        projekt_kennung,
        INITCAP(TRIM(projektname)) AS name,
        TRIM(projektstatus) AS status,
        TRIM(go_live_datum) AS go_live_date,
        account_kundennummer,
        opportunity_opp_kennung
    FROM source_data
),

parsed_date AS (
    SELECT
        projekt_kennung,
        name,
        status,
        CASE
            WHEN go_live_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN go_live_date
            WHEN go_live_date ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{4}$' THEN
                TO_CHAR(TO_DATE(go_live_date, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN go_live_date ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                TO_CHAR(TO_DATE(go_live_date, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN go_live_date ~ '^[0-9]{8}$' THEN
                TO_CHAR(TO_DATE(go_live_date, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END AS go_live_date_iso,
        account_kundennummer,
        opportunity_opp_kennung
    FROM normalized
),

status_mapped AS (
    SELECT
        projekt_kennung,
        name,
        go_live_date_iso,
        account_kundennummer,
        opportunity_opp_kennung,
        CASE
            WHEN LOWER(status) IN ('aktiv', 'active') THEN 'Active'
            WHEN LOWER(status) IN ('abgeschlossen', 'completed') THEN 'Completed'
            WHEN LOWER(status) IN ('in planung', 'in planning') THEN 'In Planning'
            WHEN LOWER(status) IN ('in wartestellung', 'on hold') THEN 'On Hold'
            WHEN LOWER(status) IN ('storniert', 'cancelled') THEN 'Cancelled'
            ELSE NULL
        END AS project_status__c
    FROM parsed_date
)

SELECT
    MD5(projekt_kennung || '_PROJECT') AS "Id",
    name AS "Name",
    project_status__c AS "Project_Status__c",
    go_live_date_iso AS "Go_Live_Date__c",
    CASE
        WHEN account_kundennummer IS NOT NULL
        THEN MD5(account_kundennummer || '_ACCOUNT')
        ELSE NULL
    END AS "Account__c",
    CASE
        WHEN opportunity_opp_kennung IS NOT NULL
        THEN MD5(opportunity_opp_kennung || '_OPPORTUNITY')
        ELSE NULL
    END AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM status_mapped
