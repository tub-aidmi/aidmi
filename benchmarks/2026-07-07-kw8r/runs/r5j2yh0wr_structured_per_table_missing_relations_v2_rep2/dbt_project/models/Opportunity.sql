{{ config(materialized='table') }}

WITH source_opportunity AS (
    SELECT * FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
),

source_account AS (
    SELECT * FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
),

staged AS (
    SELECT
        -- Generate Salesforce-style Id from source id using a hash for consistency
        '006' || SUBSTRING(MD5(o.id) FROM 1 FOR 15) AS "Id",
        
        o.name AS "Name",
        
        -- Map stage values to enum domain with case normalization
        CASE
            WHEN LOWER(TRIM(o.stage)) IN ('prospecting', 'lead gen') THEN 'Prospecting'
            WHEN LOWER(TRIM(o.stage)) IN ('qualification', 'qualifying') THEN 'Qualification'
            WHEN LOWER(TRIM(o.stage)) IN ('needs analysis', 'needs_analysis', 'discovery') THEN 'Needs Analysis'
            WHEN LOWER(TRIM(o.stage)) IN ('value proposition', 'value_proposition', 'proposal dev') THEN 'Value Proposition'
            WHEN LOWER(TRIM(o.stage)) IN ('id. decision makers', 'identifying decision makers', 'decision maker id') THEN 'Id. Decision Makers'
            WHEN LOWER(TRIM(o.stage)) IN ('perception analysis', 'perception_analysis', 'evaluation') THEN 'Perception Analysis'
            WHEN LOWER(TRIM(o.stage)) IN ('proposal/price quote', 'proposal_price_quote', 'quote', 'proposal') THEN 'Proposal/Price Quote'
            WHEN LOWER(TRIM(o.stage)) IN ('negotiation/review', 'negotiation_review', 'negotiation', 'review') THEN 'Negotiation/Review'
            WHEN LOWER(TRIM(o.stage)) IN ('closed won', 'won', 'accepted') THEN 'Closed Won'
            WHEN LOWER(TRIM(o.stage)) IN ('closed lost', 'lost', 'rejected') THEN 'Closed Lost'
            ELSE NULL
        END AS "StageName",
        
        -- CloseDate: not available in source, use CURRENT_DATE as fallback
        CAST(CURRENT_DATE AS TEXT) AS "CloseDate",
        
        o.amount AS "Amount",
        
        -- Default currency since not in source
        'USD' AS "CurrencyIsoCode",
        
        -- AccountId: join account to get Salesforce-style Id, not raw customer_number
        a.id AS "AccountId",
        
        -- Legacy opportunity id from source natural key
        o.id AS "Legacy_Opportunity_ID__c",
        
        -- Missing dates default
        CAST(CURRENT_DATE AS TEXT) AS "CreatedDate",
        CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",
        
        0 AS "IsDeleted"
        
    FROM source_opportunity o
    LEFT JOIN source_account a 
        ON COALESCE(TRIM(o.customer_number), '') = COALESCE(TRIM(a.id), '')
    WHERE o.name IS NOT NULL OR o.stage IS NOT NULL OR o.amount IS NOT NULL
)

SELECT
    "Id",
    "Name",
    "StageName",
    "CloseDate",
    "Amount",
    "CurrencyIsoCode",
    "AccountId",
    "Legacy_Opportunity_ID__c",
    "CreatedDate",
    "LastModifiedDate",
    "IsDeleted"
FROM staged;