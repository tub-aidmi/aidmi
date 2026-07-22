{{ config(materialized='table') }}

WITH opps AS (
    SELECT
        id,
        name,
        stage,
        amount,
        customer_number,
        account_name
    FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
),
account_map AS (
    SELECT 
        id AS acc_id,
        'ACC-' || SPLIT_PART(customer_number, '-', 2) AS mapped_customer_number
    FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}, 
         LATERAL regexp_split_to_table(customer_number, '-') AS parts(customer_part)
    WHERE customer_number IS NOT NULL AND customer_number ~ '^KD-\d+$'
),
account_ids AS (
    SELECT DISTINCT 
        SPLIT_PART(op.customer_number, '-', 2) AS numeric_id,
        acc.id AS account_id
    FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} op
    LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc 
        ON 'ACC-' || SPLIT_PART(op.customer_number, '-', 2) = acc.id
    WHERE op.customer_number IS NOT NULL
)
SELECT
    o.id AS "Id",
    o.name AS "Name",
    CASE 
        WHEN LOWER(o.stage) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(o.stage) = 'qualification' THEN 'Qualification'
        WHEN LOWER(o.stage) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(o.stage) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(o.stage) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(o.stage) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(o.stage) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(o.stage) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(o.stage) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(o.stage) = 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    NULL AS "CloseDate",
    o.amount AS "Amount",
    'EUR' AS "CurrencyIsoCode",
    ai.account_id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN account_ids ai ON SPLIT_PART(o.customer_number, '-', 2) = ai.numeric_id
