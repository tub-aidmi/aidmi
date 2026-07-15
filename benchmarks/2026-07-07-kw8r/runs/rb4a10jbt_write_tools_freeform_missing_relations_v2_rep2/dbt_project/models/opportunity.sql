{{ config(materialized='table') }}

SELECT
    CAST(s.id AS TEXT) AS "Id",
    CAST(s.name AS TEXT) AS "Name",
     -- Map stage to valid enum values
    CASE LOWER(TRIM(s.stage))
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
     -- CloseDate: source is not directly available, need to derive or use NULL
    NULL AS "CloseDate",
    CAST(s.amount AS DOUBLE PRECISION) AS "Amount",
     'EUR' AS "CurrencyIsoCode",
     -- AccountId: map customer_number (KD-XXXX) to account Id (ACC-XXXX)
    CASE WHEN s.customer_number IS NOT NULL THEN 'ACC-' || LTRIM(CAST(SUBSTRING(s.customer_number FROM 4) AS TEXT), '0') ELSE NULL END AS "AccountId",
     -- Legacy_Opportunity_ID__c
    CAST(s.id AS TEXT) AS "Legacy_Opportunity_ID__c",
     -- CreatedDate, LastModifiedDate: not in source, use placeholder or NULL
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} s
