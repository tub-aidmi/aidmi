{{ config(materialized='table') }}

WITH parsed_amount AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        amount,
        currencyisocode,
        accountid,
        CASE
            WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
            WHEN amount ~ '^[+-]?[0-9]{1,3}(\.[0-9]{3})+,[0-9]+$' THEN
                CAST(REPLACE(REPLACE(TRIM(amount), '.', ''), ',', '.') AS DOUBLE PRECISION)
            WHEN amount ~ '^[+-]?[0-9]+(,[0-9]+)?$' THEN
                CAST(REPLACE(TRIM(amount), ',', '.') AS DOUBLE PRECISION)
            ELSE
                CASE
                    WHEN REGEXP_REPLACE(TRIM(amount), '[^\d.,+\-]', '', 'g') IN ('', '+', '-', '+-', '-+')
                        THEN NULL
                    ELSE CAST(
                        REPLACE(
                            REGEXP_REPLACE(TRIM(amount), '[^\d.,+\-]', '', 'g'),
                            ',', '.'
                        ) AS DOUBLE PRECISION)
                END
        END AS amount_cleaned
    FROM "fixture_messy_data_v2_src"."opportunity"
)

SELECT
    id AS "Id",
    COALESCE(TRIM(name), '') AS "Name",
    CASE LOWER(TRIM(COALESCE(stagename, '')))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'prospect' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'quali' THEN 'Qualification'
        WHEN 'qualifikation' THEN 'Qualification'
        WHEN 'in prüfung' THEN 'Needs Analysis'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'won' THEN 'Closed Won'
        WHEN 'abgeschlossen (gewonnen)' THEN 'Closed Won'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        WHEN 'lost' THEN 'Closed Lost'
        WHEN 'abgeschlossen (verloren)' THEN 'Closed Lost'
        WHEN 'verloren' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN closedate IS NULL OR TRIM(closedate) = '' THEN NULL
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate
        WHEN closedate ~ '^\d{8}$' THEN
            SUBSTR(closedate, 1, 4) || '-' || SUBSTR(closedate, 5, 2) || '-' || SUBSTR(closedate, 7, 2)
        WHEN closedate ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN
            LPAD(SPLIT_PART(closedate, '/', 3), 4, '0') || '-' ||
            LPAD(SPLIT_PART(closedate, '/', 1), 2, '0') || '-' ||
            LPAD(SPLIT_PART(closedate, '/', 2), 2, '0')
        WHEN closedate ~ '^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$' THEN
            SUBSTR(closedate, 7, 4) || '-' ||
            LPAD(SPLIT_PART(closedate, '.', 2), 2, '0') || '-' ||
            LPAD(SPLIT_PART(closedate, '.', 1), 2, '0')
        ELSE NULL
    END AS "CloseDate",
    amount_cleaned AS "Amount",
    CASE UPPER(TRIM(COALESCE(currencyisocode, '')))
        WHEN 'USD' THEN 'USD'
        WHEN 'EUR' THEN 'EUR'
        WHEN 'CHF' THEN 'CHF'
        WHEN 'GBP' THEN 'GBP'
        WHEN 'EURO' THEN 'EUR'
        WHEN 'DOLLAR' THEN 'USD'
        WHEN '$' THEN 'USD'
        WHEN '€' THEN 'EUR'
        WHEN '£' THEN 'GBP'
        ELSE UPPER(TRIM(COALESCE(currencyisocode, '')))
    END AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM parsed_amount
