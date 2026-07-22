{{ config(materialized='table') }}

WITH opportunity_raw AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        amount,
        currencyisocode,
        accountid
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
)

SELECT
    id AS "Id",
    name AS "Name",
    CASE LOWER(TRIM(stagename))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'prospect' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'quali' THEN 'Qualification'
        WHEN 'qualifikation' THEN 'Qualification'
        WHEN 'in prüfung' THEN 'Needs Analysis'
        WHEN 'in kontakt' THEN 'Qualification'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'won' THEN 'Closed Won'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'abgeschlossen (gewonnen)' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        WHEN 'lost' THEN 'Closed Lost'
        WHEN 'verloren' THEN 'Closed Lost'
        WHEN 'abgeschlossen (verloren)' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(closedate, 'YYYY-MM-DD')::TEXT
        WHEN closedate ~ '^\d{8}$' THEN TO_DATE(closedate, 'YYYYMMDD')::TEXT
        WHEN closedate ~ '^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')::TEXT
        WHEN closedate ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(amount) IS NULL OR TRIM(amount) = '' OR TRIM(LOWER(TRIM(amount))) = 'none' THEN NULL
        WHEN REGEXP_REPLACE(TRIM(amount), '^\w+\s+', '') ~ '\d{1,3}\.\d{3},\d+' THEN
            CAST(REPLACE(REPLACE(REGEXP_REPLACE(TRIM(amount), '^\w+\s+', ''), '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN REGEXP_REPLACE(TRIM(amount), '^\w+\s+', '') ~ '^-?\d+\.?\d*$' THEN
            CAST(REGEXP_REPLACE(TRIM(amount), '^\w+\s+', '') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE LOWER(TRIM(currencyisocode))
        WHEN 'usd' THEN 'USD'
        WHEN 'eur' THEN 'EUR'
        WHEN 'gbp' THEN 'GBP'
        WHEN 'chf' THEN 'CHF'
        WHEN '$' THEN 'USD'
        WHEN '€' THEN 'EUR'
        WHEN '£' THEN 'GBP'
        WHEN 'dollar' THEN 'USD'
        WHEN 'euro' THEN 'EUR'
        ELSE UPPER(TRIM(currencyisocode))
    END AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunity_raw