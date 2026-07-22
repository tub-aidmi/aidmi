{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Opportunity') AS "Name",
    COALESCE(
        CASE LOWER(stagename)
            WHEN 'prospecting' THEN 'Prospecting'
            WHEN 'qualification' THEN 'Qualification'
            WHEN 'needs analysis' THEN 'Needs Analysis'
            WHEN 'value proposition' THEN 'Value Proposition'
            WHEN 'id. decision makers' THEN 'Id. Decision Makers'
            WHEN 'perception analysis' THEN 'Perception Analysis'
            WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
            WHEN 'negotiation/review' THEN 'Negotiation/Review'
            WHEN 'closed won' THEN 'Closed Won'
            WHEN 'closed lost' THEN 'Closed Lost'
            ELSE 'Prospecting' -- Default for NOT NULL
        END,
    'Prospecting') AS "StageName",
    COALESCE(
        CASE
            WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate -- YYYY-MM-DD
            WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- MM/DD/YYYY
            WHEN closedate ~ '^\d{2}.\d{2}.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
            WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
            ELSE NULL
        END,
    '1900-01-01') AS "CloseDate", -- Default for NOT NULL
    CASE
        WHEN TRIM(REGEXP_REPLACE(amount, '[^0-9,.]', '', 'g')) ~ '^\d+(\.\d{3})*,\d+$' THEN -- European format (1.234,56)
            REPLACE(REPLACE(TRIM(REGEXP_REPLACE(amount, '[^0-9,.]', '', 'g')), '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN TRIM(REGEXP_REPLACE(amount, '[^0-9,.]', '', 'g')) ~ '^\d+(,\d{3})*\.\d+$' THEN -- US format (1,234.56)
            REPLACE(TRIM(REGEXP_REPLACE(amount, '[^0-9,.]', '', 'g')), ',', '')::DOUBLE PRECISION
        WHEN TRIM(REGEXP_REPLACE(amount, '[^0-9,.]', '', 'g')) ~ '^\d+,\d+$' THEN -- European format (1234,56) without thousands sep
             REPLACE(TRIM(REGEXP_REPLACE(amount, '[^0-9,.]', '', 'g')), ',', '.')::DOUBLE PRECISION
        WHEN TRIM(REGEXP_REPLACE(amount, '[^0-9.]', '', 'g')) ~ '^\d+\.?\d*$' THEN -- US format (1234.56) or (1234) without thousands sep
            TRIM(REGEXP_REPLACE(amount, '[^0-9.]', '', 'g'))::DOUBLE PRECISION
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
