{{ config(materialized='table') }}

SELECT
    '001' || REGEXP_REPLACE(opp_kennung, '^OPP[-M]*', '') AS "Id",
    COALESCE(INITCAP(TRIM(titel)), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(vertriebsphase)) IN ('gewonnen', 'won', 'closed won', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('verloren', 'lost', 'closed lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('prospecting', 'prospect', 'PROSPECTING') THEN 'Prospecting'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('in kontakt', 'in prüfung') THEN 'Needs Analysis'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN TRIM(zieldatum) IS NULL OR TRIM(zieldatum) = '' OR UPPER(TRIM(zieldatum)) IN ('N/A', 'NULL') THEN NULL
        WHEN TRIM(zieldatum) ~ '^0000-00-00$' THEN NULL
        WHEN TRIM(zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(zieldatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(zieldatum)
        WHEN TRIM(zieldatum) ~ '^\d{8}$' THEN TO_DATE(TRIM(zieldatum), 'YYYYMMDD')::TEXT
        WHEN TRIM(zieldatum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(zieldatum), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(COALESCE(auftragswert, '')) = '' OR UPPER(TRIM(COALESCE(auftragswert, ''))) IN ('NONE', 'NULL', 'N/A') THEN NULL
        WHEN UPPER(TRIM(COALESCE(auftragswert, ''))) = '0' OR TRIM(COALESCE(auftragswert, '')) = '0' THEN 0.0
        ELSE CAST(
            REGEXP_REPLACE(
                REPLACE(
                    SPLIT_PART(
                        REGEXP_REPLACE(
                            TRIM(REGEXP_REPLACE(auftragswert, '[€$]|eur|usd|chf|gbp|dollar\s', '', 'gi')),
                            '\.[0-9]{3},[0-9]{2}$'
                        ),
                        ',', 1
                    ) || '.' || SPLIT_PART(
                        REGEXP_REPLACE(TRIM(REGEXP_REPLACE(auftragswert, '[€$]|eur|usd|chf|gbp|dollar\s', '', 'gi')), '\.[0-9]{3},[0-9]{2}$'),
                        ',', 2
                    ),
                '.', ''
            ) AS DOUBLE PRECISION)
    END AS "Amount",
    CASE
        WHEN LOWER(TRIM(waehrungscode)) IN ('eur', '€') THEN 'EUR'
        WHEN LOWER(TRIM(waehrungscode)) IN ('usd', '$', 'dollar') THEN 'USD'
        WHEN LOWER(TRIM(waehrungscode)) IN ('gbp', '£') THEN 'GBP'
        WHEN LOWER(TRIM(waehrungscode)) IN ('chf', 'fr.', 'ch') THEN 'CHF'
        ELSE NULL
    END AS "CurrencyIsoCode",
    CASE
        WHEN kunden_ref IS NOT NULL AND kunden_ref ~ '^KD[-M]*' THEN
            '001' || REGEXP_REPLACE(kunden_ref, '^KD[-M]*', '')
        ELSE NULL
    END AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_src', 'master_opportunities') }}