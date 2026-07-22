{{ config(materialized='table') }}

WITH source_data AS (
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
    -- Id (text NOT NULL)
    sd.id AS "Id",

    -- Name (text NOT NULL)
    COALESCE(TRIM(sd.name), 'Unknown Opportunity') AS "Name",

    -- StageName (text NOT NULL in enum)
    CASE
        WHEN LOWER(TRIM(sd.stagename)) IN ('prospecting', 'new lead') THEN 'Prospecting'
        WHEN LOWER(TRIM(sd.stagename)) IN ('qualification', 'qualifying') THEN 'Qualification'
        WHEN LOWER(TRIM(sd.stagename)) IN ('needs analysis', 'analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(sd.stagename)) IN ('value proposition', 'proposal prepared') THEN 'Value Proposition'
        WHEN LOWER(TRIM(sd.stagename)) IN ('id. decision makers', 'identify decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(sd.stagename)) IN ('perception analysis', 'perception') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(sd.stagename)) IN ('proposal/price quote', 'quote sent') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(sd.stagename)) IN ('negotiation/review', 'negotiating') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(sd.stagename)) IN ('closed won', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(sd.stagename)) IN ('closed lost', 'lost', 'cancelled') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",

    -- CloseDate (text NOT NULL - YYYY-MM-DD)
    COALESCE(
        TO_CHAR(TO_DATE(sd.closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(sd.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(sd.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL
    ) AS "CloseDate",

    -- Amount (double precision)
    CASE
        WHEN sd.amount IS NULL OR TRIM(sd.amount) = '' THEN NULL
        ELSE
            CAST(REGEXP_REPLACE(
                -- If European format (has comma, potentially with dots as thousands separators)
                CASE
                    WHEN TRIM(sd.amount) ~ '^.*,\d{1,2}$' THEN REGEXP_REPLACE(TRIM(sd.amount), '[.]', '', 'g')
                    ELSE TRIM(sd.amount)
                END,
                ','
                ,'.'
            ) AS DOUBLE PRECISION)
    END AS "Amount",

    -- CurrencyIsoCode (text)
    sd.currencyisocode AS "CurrencyIsoCode",

    -- AccountId (text)
    sd.accountid AS "AccountId",

    -- Legacy_Opportunity_ID__c (text)
    sd.id AS "Legacy_Opportunity_ID__c",

    -- CreatedDate (text) - Not in source, set to NULL
    CAST(NULL AS TEXT) AS "CreatedDate",

    -- LastModifiedDate (text) - Not in source, set to NULL
    CAST(NULL AS TEXT) AS "LastModifiedDate",

    -- IsDeleted (integer) - Not in source, set to 0
    0 AS "IsDeleted"
FROM
    source_data sd
