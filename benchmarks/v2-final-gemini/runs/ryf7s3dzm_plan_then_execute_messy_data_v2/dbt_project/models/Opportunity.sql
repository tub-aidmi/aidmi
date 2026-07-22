-- dbt model for Opportunity

{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(TRIM(o.name), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(o.stagename)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(o.stagename)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(o.stagename)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(o.stagename)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(o.stagename)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(o.stagename)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(o.stagename)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(o.stagename)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(o.stagename)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(o.stagename)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default value for NOT NULL constraint
    END AS "StageName",
    COALESCE(
        CASE
            WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(o.closedate::DATE, 'YYYY-MM-DD')
            WHEN o.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN o.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN o.closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            ELSE '1900-01-01' -- Default value for NOT NULL constraint if unparseable
        END,
        '1900-01-01' -- Default value if closedate is NULL
    ) AS "CloseDate",
    CASE
        WHEN TRIM(o.amount) IS NULL OR TRIM(o.amount) = 'None' THEN NULL
        ELSE
            CAST(REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(LOWER(TRIM(o.amount)), '^eur |^£|^usd |^chf ', ''),
                    E'(\\d+)\\.(\\d{3}),(\\d{2})', E'\\1\\2.\\3'  -- Handle X.XXX,YY (European format with dots as thousands)
                ),
                ',', '.' -- Replace comma with dot for other European-like formats (X,YY) or remaining commas
            ) AS DOUBLE PRECISION)
    END AS "Amount",
    o.currencyisocode AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS o