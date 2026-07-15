{{ config(materialized='table') }}

SELECT 
    TRIM(id) AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unnamed Opportunity') AS "Name",
    
    CASE UPPER(TRIM(COALESCE(stage, '')))
        WHEN 'PROSPECTING' THEN 'Prospecting'
        WHEN 'QUALIFICATION' THEN 'Qualification'
        WHEN 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN 'CLOSED WON' THEN 'Closed Won'
        WHEN 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    
    COALESCE(NULL::TEXT, '1900-01-01') AS "CloseDate",
    
    amount AS "Amount",
    
    'USD' AS "CurrencyIsoCode",
    
    TRIM(customer_number) AS "AccountId",
    TRIM(id) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}