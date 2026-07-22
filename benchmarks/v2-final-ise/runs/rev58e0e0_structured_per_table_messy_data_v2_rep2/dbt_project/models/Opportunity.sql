{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(stagename)) IN ('prospecting', 'prospect', 'prospecting', 'prospect') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualification', 'quali', 'qualifikation', 'in prüfung') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) IN ('needs analysis', 'in kontakt') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('value proposition', 'value prop') THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) IN ('id. decision makers', 'identify decision makers', 'decision makers', 'id.decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) IN ('perception analysis', 'perception') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'proposal', 'price quote', 'vorschlag') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'negotiation', 'review', 'verhandlung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)', 'lose') THEN 'Closed Lost'
        ELSE 'Needs Analysis'
    END AS "StageName",
    CASE
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(closedate AS DATE)
        WHEN closedate ~ '^\d{8}$' THEN TO_DATE(closedate, 'YYYYMMDD')
        WHEN closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')
        WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
            CASE
                WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' AND LENGTH(SPLIT_PART(closedate, '/', 1)) = 1 AND LENGTH(SPLIT_PART(closedate, '/', 2)) > 2 THEN TO_DATE(closedate, 'M/DD/YYYY')
                ELSE TO_DATE(closedate, 'MM/D/YYYY')
            END
        ELSE NULL
    END::TEXT AS "CloseDate",
    CASE
        WHEN CAST(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    TRIM(REGEXP_REPLACE(amount, '[^\d.,\-]', '', 'g')),
                    '^\.','0.'),
                    ',\.$', ',0') AS DOUBLE PRECISION
        ) IS NULL AND amount IS NOT NULL THEN NULL
        ELSE
            CASE
                -- European format: dots as thousands separators and comma as decimal (e.g. "60.702,05")
                WHEN CAST(amount AS TEXT) ~ '\.[0-9]{3}[^0-9]*,' OR (CAST(amount AS TEXT) ~ '\.' AND CAST(amount AS TEXT) ~ ',')
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(TRIM(amount), '[^\d,\-.]', '', 'g'), '\.', '')::DOUBLE PRECISION / 10
                -- Plain number with possible currency prefix already stripped above, or negative sign handling
                WHEN amount IS NOT NULL AND amount ~ '^-?\d+$' THEN CAST(
                    REGEXP_REPLACE(TRIM(REGEXP_REPLACE(amount, '[^\d.,\-]', '', 'g')), '[\.,]', '', 'g') AS DOUBLE PRECISION)
                WHEN amount IS NOT NULL AND amount ~ '\.[0-9]+$' THEN CAST(
                    TRIM(REGEXP_REPLACE(amount, '[^\d.,\-]', '', 'g')) AS DOUBLE PRECISION)
                WHEN amount IS NOT NULL AND amount ~ ',[0-9]{1,2}$' THEN CAST(
                    TRIM(REGEXP_REPLACE(amount, '[^\d.,\-]', '', 'g')) AS DOUBLE PRECISION)
                ELSE CAST(
                    REGEXP_REPLACE(TRIM(REGEXP_REPLACE(amount, '[^\d.,\-]', '', 'g')), '[\.,]', '.', 'g') AS DOUBLE PRECISION)
            END
    END AS "Amount",
    CASE LOWER(TRIM(COALESCE(currencyisocode, '')))
        WHEN '$' THEN 'USD'
        WHEN 'usd' THEN 'USD'
        WHEN 'euro' THEN 'EUR'
        WHEN 'eur' THEN 'EUR'
        WHEN '€' THEN 'EUR'
        WHEN 'gbp' THEN 'GBP'
        WHEN '£' THEN 'GBP'
        WHEN 'chf' THEN 'CHF'
        WHEN 'dollar' THEN 'USD'
        ELSE COALESCE(UPPER(TRIM(currencyisocode)), NULL)
    END AS "CurrencyIsoCode",
    accountid AS "AccountId",
    CAST(id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}