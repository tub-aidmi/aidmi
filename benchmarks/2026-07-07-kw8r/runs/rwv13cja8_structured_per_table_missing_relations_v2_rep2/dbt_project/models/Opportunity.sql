{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    TRIM(UPPER(o.name)) AS "Name",
    CASE LOWER(TRIM(o.stage))
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
        ELSE NULL
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate",
    CAST(o.amount AS DOUBLE PRECISION) AS "Amount",
    'EUR'::TEXT AS "CurrencyIsoCode",
    acct.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acct
    ON REGEXP_REPLACE(o.customer_number, '^\D+', '') = REGEXP_REPLACE(acct.id, '^\D+', '')