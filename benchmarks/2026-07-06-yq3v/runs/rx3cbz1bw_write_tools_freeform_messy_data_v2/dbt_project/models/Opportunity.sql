-- models/Opportunity.sql

{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Opportunity') AS "Name",
    COALESCE(
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
            ELSE NULL
        END,
        'Prospecting' -- Default to a valid enum value for NOT NULL column
    ) AS "StageName",
    COALESCE(
        CASE
            WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01' -- Default date for NOT NULL column if parsing fails
    ) AS "CloseDate",
    NULLIF(
        CAST(
            (SELECT
                CASE
                    WHEN _cleaned_val ~ '\.' AND _cleaned_val ~ ',' AND POSITION(',' IN _cleaned_val) > POSITION(REVERSE('.') IN REVERSE(_cleaned_val)) THEN
                        REPLACE(REPLACE(_cleaned_val, '.', '', 'g'), ',', '.') -- European: 1.234,56 -> 1234.56
                    WHEN _cleaned_val ~ ',' AND NOT _cleaned_val ~ '\.' THEN
                        REPLACE(_cleaned_val, ',', '.') -- European: 1234,56 -> 1234.56
                    ELSE
                        REPLACE(_cleaned_val, ',', '', 'g') -- US/Standard: 1,234.56 -> 1234.56 or 1234.56 -> 1234.56
                END
             FROM (SELECT REGEXP_REPLACE(LOWER(COALESCE(amount, '')), '[^0-9\.,]+', '', 'g') AS _cleaned_val) AS sub_clean
            )
        AS DOUBLE PRECISION
    ), '' -- Nullify if the resulting string is empty after cleaning, indicating a non-numeric value
    ) AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c", -- Using source ID as legacy ID
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }}
