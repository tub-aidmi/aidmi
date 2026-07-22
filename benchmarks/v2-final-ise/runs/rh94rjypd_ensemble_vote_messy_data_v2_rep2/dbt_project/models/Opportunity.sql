{{ config(materialized='table') }}

WITH cleaned_amount AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        amount,
        currencyisocode,
        accountid,
        -- Clean amount: strip all non-numeric except dot, comma, minus
        CASE 
            WHEN TRIM(COALESCE(amount, '')) = '' OR LOWER(TRIM(COALESCE(amount, ''))) IN ('none', 'n/a', '-', '') THEN NULL
            ELSE
                CASE
                    -- First pass: remove all text characters, keep only digits, dot, comma, minus
                    WHEN COALESCE(amount, '') ~ '[A-Za-z]' THEN
                        REGEXP_REPLACE(TRIM(COALESCE(amount, '')), '[^0-9.,\-]', '')
                    ELSE TRIM(COALESCE(amount, ''))
                END
        END AS raw_cleaned_amount
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
)

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(name)) AS "Name",
    CASE 
        WHEN LOWER(TRIM(stagename)) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(stagename)) IN ('prospecting', 'prospect', 'prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualification', 'qualifikation', 'in prüfung', 'quali') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) = 'in kontakt' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('value proposition', 'wertpropose', 'wertanalyse') THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) IN ('id. decision makers', 'identifizierte entscheidungsträger', 'decision maker') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) IN ('perception analysis', 'wahrnehmungsanalyse', 'market analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'angebot/preisanfrage', 'vorschlag') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'verhandlung/prüfung', 'verhandlung') THEN 'Negotiation/Review'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN TRIM(closedate) = '' OR closedate IS NULL THEN NULL
        WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate
        WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CAST(
        CASE 
            WHEN raw_cleaned_amount IS NULL OR raw_cleaned_amount = '' THEN NULL
            -- Case with both dot and comma: European format (dot=thousands, comma=decimal)
            WHEN raw_cleaned_amount ~ '\.' AND raw_cleaned_amount ~ ',' THEN
                REGEXP_REPLACE(raw_cleaned_amount, '[\.,]', 
                    CASE WHEN SUBSTRING(raw_cleaned_amount FROM '.*,(.*)') ~ '^\d+\.\d*$' THEN '' ELSE '.' END,
                    CASE WHEN raw_cleaned_amount ~ '[0-9]+\.[0-9]+,[0-9]+$' AND LENGTH(REGEXP_REPLACE(raw_cleaned_amount, '[0-9.]+,', '')) = 2 
                         THEN ',' 
                         ELSE '.' END
                )::DOUBLE PRECISION
            -- Only comma present: treat as decimal (European)
            WHEN raw_cleaned_amount ~ ',' THEN
                REGEXP_REPLACE(raw_cleaned_amount, ',', '.')::DOUBLE PRECISION
            -- Only dot or no special chars: standard format
            ELSE CAST(REGEXP_REPLACE(raw_cleaned_amount, '[^\d\-.]', '') AS DOUBLE PRECISION)
        END
    AS DOUBLE PRECISION) AS "Amount",
    CASE UPPER(TRIM(COALESCE(currencyisocode, '')))
        WHEN 'USD' THEN 'USD'
        WHEN 'EUR' THEN 'EUR'
        WHEN 'CHF' THEN 'CHF'
        WHEN 'GBP' THEN 'GBP'
        WHEN 'DOLLAR' THEN 'USD'
        WHEN 'EURO' THEN 'EUR'
        WHEN '$' THEN 'USD'
        WHEN '£' THEN 'GBP'
        WHEN '€' THEN 'EUR'
        ELSE UPPER(TRIM(COALESCE(currencyisocode, '')))
    END AS "CurrencyIsoCode",
    -- Map accountid to Account.Id by joining with Account table to get proper Salesforce-style Id
    CAST(a.id AS TEXT) AS "AccountId",
    CAST(id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM cleaned_amount ca
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a 
    ON a.erp_number__c = LEFT(ca.accountid, 13) -- Map CUST-XXXX to ERP numbers
WHERE ca.id IS NOT NULL