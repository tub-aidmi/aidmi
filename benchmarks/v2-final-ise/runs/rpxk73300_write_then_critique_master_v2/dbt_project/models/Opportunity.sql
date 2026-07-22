{{ config(materialized='table') }}

SELECT
    opp_kennung AS "Id",
    titel AS "Name",
    CASE
        WHEN LOWER(TRIM(vertriebsphase)) IN ('prospecting', 'in kontakt', 'prospect') THEN 'Prospecting'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('needs analysis', 'in prüfung') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('value proposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('id. decision makers', 'decision maker') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('proposal/price quote', 'proposal', 'price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('closed won', 'abgeschlossen (gewonnen)', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('closed lost', 'abgeschlossen (verloren)', 'lost', 'verloren') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN zieldatum
        WHEN zieldatum ~ '^\d{8}$' THEN
            SUBSTR(zieldatum, 1, 4) || '-' || SUBSTR(zieldatum, 5, 2) || '-' || SUBSTR(zieldatum, 7, 2)
        WHEN zieldatum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN
            TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY')::TEXT
        WHEN zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
            TO_DATE(TRIM(zieldatum), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(UPPER(auftragswert)) = 'NONE' OR auftragswert IS NULL THEN NULL
        ELSE
            CAST(
                CASE 
                    WHEN REGEXP_REPLACE(TRIM(auftragswert), '[A-Za-z€$£]+\s*', '') ~ '\.' AND 
                         REGEXP_REPLACE(TRIM(auftragswert), '[A-Za-z€$£]+\s*', '') ~ ',' THEN
                        REPLACE(
                            REGEXP_REPLACE(REGEXP_REPLACE(TRIM(auftragswert), '[A-Za-z€$£]+\s*', ''), '\.', '', 'g'),
                            ',', '.'
                        )::DOUBLE PRECISION
                    ELSE
                        REGEXP_REPLACE(REGEXP_REPLACE(TRIM(auftragswert), '[A-Za-z€$£]+\s*', ''), '\.', '', 'g')::DOUBLE PRECISION
                END
            AS DOUBLE PRECISION)
    END AS "Amount",
    CASE
        WHEN LOWER(TRIM(waehrungscode)) IN ('usd', 'dollar', '$') THEN 'USD'
        WHEN LOWER(TRIM(waehrungscode)) IN ('eur', 'euro', '€') THEN 'EUR'
        WHEN LOWER(TRIM(waehrungscode)) IN ('gbp', '£') THEN 'GBP'
        WHEN LOWER(TRIM(waehrungscode)) IN ('chf') THEN 'CHF'
        ELSE UPPER(TRIM(waehrungscode))
    END AS "CurrencyIsoCode",
    CASE 
        WHEN kunden_ref ~ '^KD-M\d+' THEN '001' || LPAD(SUBSTRING(kunden_ref FROM 'M(\d+)')::INTEGER::TEXT, 8, '0')
        ELSE NULL
    END AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}