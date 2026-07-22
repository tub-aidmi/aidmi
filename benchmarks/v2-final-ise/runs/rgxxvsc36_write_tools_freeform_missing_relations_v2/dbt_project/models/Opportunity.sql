{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    o.name AS "Name",
    CASE
        WHEN UPPER(o.stage) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(o.stage) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(o.stage) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(o.stage) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(o.stage) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(o.stage) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(o.stage) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(o.stage) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(o.stage) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(o.stage) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    NULL AS "CloseDate",
    o.amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON REPLACE(o.customer_number, 'KD-', 'ACC-') = a.id
