{{ config(materialized='table') }}
SELECT
    src_opportunity.id AS "Id",
    TRIM(src_opportunity.name) AS "Name",
    CASE
        WHEN TRIM(LOWER(src_opportunity.stagename)) = 'prospecting' THEN 'Prospecting'
        WHEN TRIM(LOWER(src_opportunity.stagename)) = 'qualification' THEN 'Qualification'
        WHEN TRIM(LOWER(src_opportunity.stagename)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN TRIM(LOWER(src_opportunity.stagename)) = 'value proposition' THEN 'Value Proposition'
        WHEN TRIM(LOWER(src_opportunity.stagename)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(src_opportunity.stagename)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN TRIM(LOWER(src_opportunity.stagename)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(src_opportunity.stagename)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(src_opportunity.stagename)) = 'closed won' THEN 'Closed Won'
        WHEN TRIM(LOWER(src_opportunity.stagename)) = 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN TRIM(src_opportunity.closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(src_opportunity.closedate)
        WHEN TRIM(src_opportunity.closedate) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(src_opportunity.closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(src_opportunity.closedate) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(src_opportunity.closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN src_opportunity.amount ~ '^[+-]?[0-9]{1,3}(\.[0-9]{3})*([,][0-9]+)?$' THEN
            REGEXP_REPLACE(
                REGEXP_REPLACE(src_opportunity.amount, '[^0-9,-]', '', 'g'),
                '\.', '', 'g'
            )::DOUBLE PRECISION
        WHEN src_opportunity.amount ~ '^[+-]?[0-9]+([,][0-9]+)?$' THEN
            REGEXP_REPLACE(src_opportunity.amount, ',', '.')::DOUBLE PRECISION
        WHEN src_opportunity.amount ~ '^[+-]?[0-9]+(\.[0-9]+)?$' THEN
            src_opportunity.amount::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    TRIM(UPPER(src_opportunity.currencyisocode)) AS "CurrencyIsoCode",
    src_opportunity.accountid AS "AccountId",
    src_opportunity.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS src_opportunity