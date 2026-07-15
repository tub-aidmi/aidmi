{{ config(materialized='table') }}

SELECT
    opp_kennung AS "Id",
    titel AS "Name",
    CASE 
        WHEN UPPER(TRIM(vertriebsphase)) IN ('PROSPECTING', 'PROSPECT', 'IN KONTAKT', 'PROSPEKT') THEN 'Prospecting'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('QUALIFICATION', 'QUALI', 'QUALIFIKATION', 'IN PRÜFUNG') THEN 'Qualification'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('NEEDS ANALYSIS', 'BEDARFSANALYSE') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('VALUE PROPOSITION', 'WERTPROPOSITION', 'WERTEPROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('ID. DECISION MAKERS', 'ENTSCHEIDUNGSFINDEUR', 'IDENTIFY DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('PERCEPTION ANALYSIS', 'WAHRNEHMUNGSANALYSE') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('PROPOSAL/PRICE QUOTE', 'ANGEBOT PREISZITAT', 'PROPOSAL PRICE QUOTE', 'ANGEBOT') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('NEGOTIATION/REVIEW', 'VERHANDLUNG PRÜFUNG', 'NEGOTIATION REVIEW', 'VERHANDLUNG') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('CLOSED WON', 'GEWONNEN', 'ABGESCHLOSSEN (GEWONNEN)', 'WIN') THEN 'Closed Won'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('CLOSED LOST', 'VERLOREN', 'ABGESCHLOSSEN (VERLOREN)', 'LOST', 'LOSE') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN zieldatum IS NULL OR TRIM(zieldatum) = '' THEN NULL
        -- YYYY-MM-DD format
        WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN zieldatum
        -- DD.MM.YYYY format (European dot separator)
        WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(zieldatum, 'DD.MM.YYYY')::TEXT
        -- MM/DD/YYYY format (US slash separator)
        WHEN zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(zieldatum, 'MM/DD/YYYY')::TEXT
        -- YYYYMMDD format (8-digit compact)
        WHEN zieldatum ~ '^\d{8}$' THEN
            CASE
                WHEN SUBSTRING(zieldatum FROM 5 FOR 2)::INTEGER BETWEEN 1 AND 12
                 AND SUBSTRING(zieldatum FROM 7 FOR 2)::INTEGER BETWEEN 1 AND 31
                    THEN SUBSTRING(zieldatum FROM 1 FOR 4) || '-' || 
                         LPAD(SUBSTRING(zieldatum FROM 5 FOR 2), 2, '0') || '-' || 
                         LPAD(SUBSTRING(zieldatum FROM 7 FOR 2), 2, '0')
                ELSE NULL
            END
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN auftragswert IS NULL OR TRIM(auftragswert) = '' THEN NULL
        WHEN UPPER(TRIM(auftragswert)) IN ('NONE', 'N/A', '-', '') THEN NULL
        -- European format: remove currency prefix/suffix, dots are thousand-sep, comma is decimal
        -- Example: "EUR 144893.69" → strip "EUR ", then check
        WHEN REGEXP_REPLACE(auftragswert, '[^\d.,\-\+]', '') ~ '^\d{1,3}(\.\d{3})*,\d+$' THEN
            CAST(REGEXP_REPLACE(REGEXP_REPLACE(auftragswert, '[^\d.,\-\+]', ''), '\.', '') AS DOUBLE PRECISION)
        -- Standard decimal format (possibly with currency prefix like "EUR ")
        WHEN REGEXP_REPLACE(auftragswert, '[^\d.,\-\+]', '') ~ '^\d+\.\d+$' THEN
            CAST(REGEXP_REPLACE(auftragswert, '[^\d.,\-\+]', '') AS DOUBLE PRECISION)
        -- Integer or negative without decimal point
        WHEN auftragswert ~ '^-?\d+$' THEN CAST(auftragswert AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM(waehrungscode)) IN ('USD', 'US DOLLAR', 'EURO') AND waehrungscode = 'Euro' THEN 'EUR'
        WHEN UPPER(TRIM(waehrungscode)) IN ('USD', '$', 'US DOLLAR', 'DOLLAR') THEN 'USD'
        WHEN UPPER(TRIM(waehrungscode)) IN ('EUR', '€', 'EURO') THEN 'EUR'
        WHEN UPPER(TRIM(waehrungscode)) IN ('GBP', '£', 'BRITISH POUND') THEN 'GBP'
        WHEN UPPER(TRIM(waehrungscode)) IN ('CHF', 'SWISS FRANC') THEN 'CHF'
        ELSE UPPER(TRIM(waehrungscode))
    END AS "CurrencyIsoCode",
    REPLACE(kunden_ref, 'KD-', 'CUST-') AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c"::TEXT,
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }};