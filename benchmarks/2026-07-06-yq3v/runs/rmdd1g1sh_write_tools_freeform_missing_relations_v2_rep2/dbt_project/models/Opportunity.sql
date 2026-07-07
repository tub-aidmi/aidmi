-- models/Opportunity.sql

{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(src.name, 'Unknown Opportunity') AS "Name",
    CASE UPPER(TRIM(src.stage))
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
        ELSE 'Prospecting' -- Default for NOT NULL target
    END AS "StageName",
    CURRENT_DATE::TEXT AS "CloseDate", -- No direct mapping, default for NOT NULL target
    src.amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    src.customer_number AS "AccountId",
    src.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS src