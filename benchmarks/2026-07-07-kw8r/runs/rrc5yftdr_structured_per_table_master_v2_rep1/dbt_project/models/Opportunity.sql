{{ config(materialized='table') }}

SELECT
    opp.opp_kennung AS "Id",
    INITCAP(TRIM(opp.titel)) AS "Name",
    CASE
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('prospect', 'prospecting', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('quali', 'qualifikation', 'qualification', 'in prüfung') THEN 'Qualification'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('gewonnen', 'won', 'closed won', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(opp.vertriebsphase)) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN opp.zieldatum IS NULL THEN NULL
        WHEN opp.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(opp.zieldatum AS TEXT)
        WHEN opp.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN opp.auftragswert IS NULL OR TRIM(opp.auftragswert) = 'None' THEN NULL
        ELSE CAST(
            CASE
                WHEN REGEXP_REPLACE(opp.auftragswert, '[^0-9.,\-+]', '') ~ '\..*,' THEN 
                    REPLACE(REPLACE(REGEXP_REPLACE(opp.auftragswert, '[^0-9.,\-+]', ''), '.', ''), ',', '.')
                WHEN REGEXP_REPLACE(opp.auftragswert, '[^0-9.,\-+]', '') ~ '\,[0-9]+$' THEN
                    REPLACE(REGEXP_REPLACE(opp.auftragswert, '[^0-9.,\-+]', ''), ',', '.')
                ELSE REGEXP_REPLACE(opp.auftragswert, '[^0-9.,\-+]', '')
            END
        AS DOUBLE PRECISION)
    END AS "Amount",
    CASE
        WHEN LOWER(TRIM(opp.waehrungscode)) IN ('chf') THEN 'CHF'
        WHEN LOWER(TRIM(opp.waehrungscode)) IN ('eur', 'euro', '€') THEN 'EUR'
        WHEN LOWER(TRIM(opp.waehrungscode)) IN ('gbp', '£') THEN 'GBP'
        WHEN LOWER(TRIM(opp.waehrungscode)) IN ('usd', 'dollar', '$') THEN 'USD'
        ELSE UPPER(TRIM(opp.waehrungscode))
    END AS "CurrencyIsoCode",
    cust.kundennummer AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} opp
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} cust
    ON REPLACE(opp.kunden_ref, 'KD-', 'CUST-') = cust.kundennummer