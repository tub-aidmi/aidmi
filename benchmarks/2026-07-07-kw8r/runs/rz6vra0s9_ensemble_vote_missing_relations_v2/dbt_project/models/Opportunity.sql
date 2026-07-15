{{ config(materialized='table') }}

SELECT 
    CAST(opp.id AS TEXT) AS "Id",
    INITCAP(TRIM(COALESCE(opp.name, 'Untitled Opportunity'))) AS "Name",
    CASE 
        WHEN LOWER(TRIM(opp.stage)) IN ('new', 'new lead', 'lead', 'inquiry') THEN 'Prospecting'
        WHEN LOWER(TRIM(opp.stage)) IN ('qualified', 'qualification', 'qualifying') THEN 'Qualification'
        WHEN LOWER(TRIM(opp.stage)) IN ('needs analysis', 'need analysis', 'discovery', 'needs identification', 'requirements analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(opp.stage)) IN ('value proposition', 'value prop', 'solution defined') THEN 'Value Proposition'
        WHEN LOWER(TRIM(opp.stage)) IN ('decision makers identified', 'identify decision makers', 'identify dm', 'decision maker identification') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(opp.stage)) IN ('perception analysis', 'perception check', 'gap analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(opp.stage)) IN ('proposal/price quote', 'proposal and price quote', 'proposal', 'quote', 'price quote', 'proposal price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(opp.stage)) IN ('negotiation/review', 'negotiation and review', 'negotiation', 'review', 'closing') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(opp.stage)) IN ('closed won', 'won', 'closed-won') THEN 'Closed Won'
        WHEN LOWER(TRIM(opp.stage)) IN ('closed lost', 'lost', 'closed-lost', 'declined', 'rejected') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    NULL::TEXT AS "CloseDate",
    opp.amount AS "Amount",
    'USD'::TEXT AS "CurrencyIsoCode",
    acc.id AS "AccountId",
    opp.id AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} opp
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc 
    ON TRIM(opp.customer_number) = TRIM(acc.id)