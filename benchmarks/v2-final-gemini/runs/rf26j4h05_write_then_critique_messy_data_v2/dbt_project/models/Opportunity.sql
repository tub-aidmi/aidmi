{{ config(materialized='table') }}

SELECT
    s.id AS "Id",
    COALESCE(s.name, 'Unknown Opportunity') AS "Name",
    COALESCE(
        CASE
            WHEN LOWER(s.stagename) IN ('prospecting', 'new business') THEN 'Prospecting'
            WHEN LOWER(s.stagename) = 'qualification' THEN 'Qualification'
            WHEN LOWER(s.stagename) = 'needs analysis' THEN 'Needs Analysis'
            WHEN LOWER(s.stagename) = 'value proposition' THEN 'Value Proposition'
            WHEN LOWER(s.stagename) = 'id. decision makers' THEN 'Id. Decision Makers'
            WHEN LOWER(s.stagename) = 'perception analysis' THEN 'Perception Analysis'
            WHEN LOWER(s.stagename) IN ('proposal/price quote', 'proposal submitted') THEN 'Proposal/Price Quote'
            WHEN LOWER(s.stagename) = 'negotiation/review' THEN 'Negotiation/Review'
            WHEN LOWER(s.stagename) = 'closed won' THEN 'Closed Won'
            WHEN LOWER(s.stagename) = 'closed lost' THEN 'Closed Lost'
            ELSE NULL
        END,
        'Prospecting'
    ) AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN s.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(s.closedate, 'YYYY-MM-DD')
                WHEN s.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(s.closedate, 'MM/DD/YYYY')
                WHEN s.closedate ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE(s.closedate, 'DD-MM-YYYY')
                WHEN s.closedate ~ '^\d{8}$' THEN TO_DATE(s.closedate, 'YYYYMMDD')
                ELSE NULL
            END,
            'YYYY-MM-DD'
        ),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "CloseDate",
    CASE
        WHEN REGEXP_REPLACE(s.amount, '[^0-9\.,]+', '', 'g') ~ '^[0-9]+([\.][0-9]+)?$' -- check for standard decimal format
        THEN CAST(REGEXP_REPLACE(s.amount, '[^0-9\.]+', '', 'g') AS DOUBLE PRECISION)
        WHEN REGEXP_REPLACE(s.amount, '[^0-9\.,]+', '', 'g') ~ '^[0-9]+([,][0-9]+)?$' -- check for comma decimal format
        THEN CAST(REPLACE(REGEXP_REPLACE(s.amount, '[^0-9,]+', '', 'g'), ',', '.') AS DOUBLE PRECISION)
        WHEN REGEXP_REPLACE(s.amount, '[^0-9\.,]+', '', 'g') ~ '^[0-9]{1,3}(\.[0-9]{3})*([,][0-9]+)?$' -- check for european format 1.234,56
        THEN CAST(REPLACE(REPLACE(REGEXP_REPLACE(s.amount, '[^0-9\.,]+', '', 'g'), '.', ''), ',', '.') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    s.currencyisocode AS "CurrencyIsoCode",
    s.accountid AS "AccountId",
    s.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS s