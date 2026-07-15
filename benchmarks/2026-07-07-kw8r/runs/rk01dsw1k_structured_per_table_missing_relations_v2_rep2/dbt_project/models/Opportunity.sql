{{ config(materialized='table') }}

WITH opp AS (
    SELECT
        o.id,
        TRIM(o.name) AS name,
        o.stage,
        o.amount,
        o.customer_number,
        REGEXP_REPLACE(o.customer_number, '^\D+', '') AS num_suffix
    FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
),
acct AS (
    SELECT
        a.id,
        a.name AS acct_name,
        a.tier,
        a.region,
        a.industry,
        REGEXP_REPLACE(a.id, '^\D+', '') AS num_suffix
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a
),
joined AS (
    SELECT
        o.*,
        a.id AS account_id,
        a.name AS acct_name,
        a.tier,
        a.region,
        a.industry
    FROM opp o
    LEFT JOIN acct a
        ON o.num_suffix = a.num_suffix
)

SELECT
    CAST(id AS TEXT) AS "Id",
    TRIM(UPPER(name)) AS "Name",
    CASE LOWER(TRIM(stage))
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
    CAST(NULL AS TEXT) AS "CloseDate",
    amount::DOUBLE PRECISION AS "Amount",
    'EUR' AS "CurrencyIsoCode",
    account_id AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM joined;