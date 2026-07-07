{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(stagename) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(stagename) = 'qualification' THEN 'Qualification'
        WHEN LOWER(stagename) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(stagename) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(stagename) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(stagename) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(stagename) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(stagename) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(stagename) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(stagename) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target
    END AS "StageName",
    COALESCE(
        TO_CHAR(CASE
            WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(closedate, 'YYYY-MM-DD')
            WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY')
            WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')
            WHEN closedate ~ '^\d{8}$' THEN TO_DATE(closedate, 'YYYYMMDD')
            ELSE NULL
        END, 'YYYY-MM-DD'),
        '1900-01-01' -- Default for NOT NULL target
    ) AS "CloseDate",
    CASE
        WHEN amount IS NULL THEN NULL
        ELSE
            NULLIF(
                (SELECT
                    CASE
                        WHEN _cleaned_amount ~ '^-?\d+\.\d+,\d+$' THEN -- European with dots as thousands, comma as decimal (e.g. 1.234,56)
                            REPLACE(REPLACE(_cleaned_amount, '.', ''), ',', '.')
                        WHEN _cleaned_amount ~ '^-?\d+,\d+\.\d+$' THEN -- American with commas as thousands, dot as decimal (e.g. 1,234.56)
                            REPLACE(_cleaned_amount, ',', '')
                        WHEN _cleaned_amount ~ '^-?\d+,\d+$' THEN -- Only comma present, assume it's a decimal separator (e.g. 123,45)
                            REPLACE(_cleaned_amount, ',', '.')
                        ELSE -- Simple number or already in standard decimal format (e.g. 123.45 or 12345)
                            _cleaned_amount
                    END
                FROM (SELECT REGEXP_REPLACE(amount, '[^0-9.,-]+', '', 'g') AS _cleaned_amount) AS sub)
            , '')::DOUBLE PRECISION
    END AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c", -- Using source id as legacy ID
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
