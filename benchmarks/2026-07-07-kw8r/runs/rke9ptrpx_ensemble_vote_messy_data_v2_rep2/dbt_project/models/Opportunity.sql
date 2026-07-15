{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(NULLIF(TRIM(o.name), ''), 'Untitled Opportunity') AS "Name",
    CASE
        WHEN TRIM(LOWER(o.stagename)) IN ('prospecting') THEN 'Prospecting'
        WHEN TRIM(LOWER(o.stagename)) IN ('qualification') THEN 'Qualification'
        WHEN TRIM(LOWER(o.stagename)) IN ('needs analysis', 'needs_analysis') THEN 'Needs Analysis'
        WHEN TRIM(LOWER(o.stagename)) IN ('value proposition', 'value_proposition') THEN 'Value Proposition'
        WHEN TRIM(LOWER(o.stagename)) IN ('id. decision makers', 'id_decision_makers', 'identify decision makers') THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(o.stagename)) IN ('perception analysis', 'perception_analysis') THEN 'Perception Analysis'
        WHEN TRIM(LOWER(o.stagename)) IN ('proposal/price quote', 'proposal_price_quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(o.stagename)) IN ('negotiation/review', 'negotiation_review', 'negotiation') THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(o.stagename)) IN ('closed won', 'closed_won') THEN 'Closed Won'
        WHEN TRIM(LOWER(o.stagename)) IN ('closed lost', 'closed_lost') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN o.closedate
        WHEN o.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN o.amount ~ '^[0-9]+(\.[0-9]+)?$' THEN o.amount::DOUBLE PRECISION
        WHEN o.amount ~ '^[0-9]+,[0-9]+$' THEN REPLACE(o.amount, ',', '.')::DOUBLE PRECISION
        WHEN o.amount ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN REPLACE(REPLACE(o.amount, '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN o.amount ~ '^[^0-9]*([0-9]+(\.[0-9]+)?)[^0-9]*$' THEN
            CASE
                WHEN REGEXP_REPLACE(o.amount, '[^0-9.,]', '', 'g') ~ '^\d+,\d+$' THEN REPLACE(REGEXP_REPLACE(o.amount, '[^0-9.,]', '', 'g'), ',', '.')::DOUBLE PRECISION
                WHEN REGEXP_REPLACE(o.amount, '[^0-9.,]', '', 'g') ~ '^\d+\.\d+,\d+$' THEN REPLACE(REPLACE(REGEXP_REPLACE(o.amount, '[^0-9.,]', '', 'g'), '.', ''), ',', '.')::DOUBLE PRECISION
                ELSE NULL
            END
        ELSE NULL
    END AS "Amount",
    NULLIF(TRIM(o.currencyisocode), '') AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o