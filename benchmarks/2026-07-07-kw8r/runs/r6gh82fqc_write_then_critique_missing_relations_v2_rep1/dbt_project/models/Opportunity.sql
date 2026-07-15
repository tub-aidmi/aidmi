{{ config(materialized='table') }}

SELECT 
    CAST(TRIM(o.id) AS TEXT) AS "Id",
    COALESCE(INITCAP(TRIM(o.name)), 'Unknown Opportunity') AS "Name",
    CASE 
        WHEN LOWER(TRIM(o.stage)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stage)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(o.stage)) IN ('needs analysis', 'need analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stage)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.stage)) IN ('id. decision makers', 'identify decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.stage)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.stage)) IN ('proposal/price quote', 'proposal & price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.stage)) IN ('negotiation/review', 'negotiation & review') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.stage)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stage)) = 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN p.go_live ~ '^\d{4}[-/]\d{2}[-/]\d{2}$' THEN TO_CHAR(TO_DATE(REGEXP_REPLACE(p.go_live, '[/-]', '-', 'g'), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{2}[/-]\d{2}[/-]\d{4}$' THEN TO_CHAR(TO_DATE(REGEXP_REPLACE(p.go_live, '[/-]', '-', 'g'), 'MM-DD-YYYY'), 'YYYY-MM-DD')  
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CAST(o.amount AS DOUBLE PRECISION) AS "Amount",
    NULL::TEXT AS "CurrencyIsoCode",
    TRIM(a.id) AS "AccountId",
    TRIM(o.id) AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p
    ON TRIM(o.id) = TRIM(p.opportunity_ref)
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON TRIM(o.customer_number) = TRIM(a.id);