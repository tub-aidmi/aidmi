{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN TRIM(LOWER(stagename)) IN ('prospecting') THEN 'Prospecting'
        WHEN TRIM(LOWER(stagename)) IN ('qualification') THEN 'Qualification'
        WHEN TRIM(LOWER(stagename)) IN ('needs analysis') THEN 'Needs Analysis'
        WHEN TRIM(LOWER(stagename)) IN ('value proposition') THEN 'Value Proposition'
        WHEN TRIM(LOWER(stagename)) IN ('id. decision makers', 'identification decision makers') THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(stagename)) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN TRIM(LOWER(stagename)) IN ('proposal/price quote') THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(stagename)) IN ('negotiation/review') THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(stagename)) IN ('closed won') THEN 'Closed Won'
        WHEN TRIM(LOWER(stagename)) IN ('closed lost') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to 'Prospecting' for unmapped/NULL values
    END AS "StageName",
    COALESCE(
        (CASE WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD') END),
        (CASE WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD') END),
        (CASE WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD') END),
        (CASE WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD') END),
        '1900-01-01'
    ) AS "CloseDate",
    CASE
        WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
        -- Regex to validate amount format before processing
        WHEN amount ~ '^[+-]?\$?\s*\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?\s*€?$' THEN
            CAST(REPLACE(REPLACE(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(amount, '[€$]', '', 'g'), '\s', '', 'g')), '.', ''), ',', '.') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }}
