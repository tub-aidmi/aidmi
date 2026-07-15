{{ config(materialized='table') }}

SELECT
     -- Legacy/primary key from source
    o.id AS "Id",

     -- Opportunity name (NOT NULL: fallback for missing names)
    COALESCE(o.name, 'Unnamed Opportunity') AS "Name",

     -- Stage mapped to target enum with fallback
    CASE
        WHEN LOWER(TRIM(o.stage)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stage)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(o.stage)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stage)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.stage)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.stage)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.stage)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.stage)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.stage)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stage)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",

     -- CloseDate: not available in source; return NULL (sentinel values violate transformation rules)
    NULL::TEXT AS "CloseDate",

     -- Amount from source
    o.amount AS "Amount",

     -- CurrencyIsoCode: not in source, default to USD
    'USD' AS "CurrencyIsoCode",

     -- AccountId: transform customer_number KD-XXXX → ACC-XXXX
    CASE
        WHEN o.customer_number ~ '^KD-\d{4}$' THEN
            'ACC-' || SUBSTRING(o.customer_number FROM 5)
        ELSE NULL
    END AS "AccountId",

     -- Legacy Opportunity ID (source natural key)
    o.id AS "Legacy_Opportunity_ID__c",

     -- Audit fields: not present in source; use explicit TEXT type for consistency
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o