{{ config(materialized='table') }}

WITH source_account AS (
    SELECT * FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
),
source_opportunity AS (
    SELECT * FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
),
mapped_accounts AS (
    SELECT
        '003' || SUBSTRING(MD5(id) FROM 1 FOR 15) AS sf_id,
        TRIM(name) AS src_name_trimmed
    FROM source_account
)

SELECT
    '006' || SUBSTRING(MD5(o.id) FROM 1 FOR 15) AS "Id",
    COALESCE(o.name, 'Unknown Opportunity') AS "Name",
    CASE LOWER(TRIM(COALESCE(o.stage, '')))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'lead gen' THEN 'Prospecting'
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
    CAST(CURRENT_DATE AS TEXT) AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    m.sf_id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    CAST(CURRENT_DATE AS TEXT) AS "CreatedDate",
    CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_opportunity o
LEFT JOIN mapped_accounts m
    ON TRIM(COALESCE(o.account_name, '')) = COALESCE(m.src_name_trimmed, '')