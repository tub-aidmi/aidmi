{{ config(materialized='table') }}

WITH opportunity_data AS (
    SELECT 
        mo.opp_kennung,
        mo.titel,
        mo.vertriebsphase,
        mo.zieldatum,
        mo.auftragswert,
        mo.waehrungscode,
        mo.kunden_ref,
        mk.kundennummer AS account_kundennummer
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk 
        ON REPLACE(mo.kunden_ref, 'KD-', 'CUST-') = mk.kundennummer
)

SELECT 
    opp_kennung AS "Id",
    titel AS "Name",
    CASE 
        WHEN LOWER(TRIM(vertriebsphase)) IN ('prospecting', 'prospect', 'prospekt') THEN 'Prospecting'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('in kontakt') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('in prüfung') THEN 'Value Proposition'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('closed won', 'gewonnen', 'won', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE 
        WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN zieldatum
        WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN auftragswert IS NULL OR TRIM(auftragswert) = 'None' THEN NULL
        ELSE 
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(auftragswert, '[^0-9\-.]', '', 'g'),
                        '\.', '', 'g'
                    ),
                    ',', '.', 'g'
                ) AS DOUBLE PRECISION
            )
    END AS "Amount",
    CASE 
        WHEN LOWER(TRIM(waehrungscode)) IN ('eur', 'euro', '€') THEN 'EUR'
        WHEN LOWER(TRIM(waehrungscode)) IN ('usd', 'dollar', '$') THEN 'USD'
        WHEN LOWER(TRIM(waehrungscode)) IN ('chf') THEN 'CHF'
        WHEN LOWER(TRIM(waehrungscode)) IN ('gbp', '£') THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    account_kundennummer AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunity_data