{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'N/A') AS "Name",
    CASE UPPER(TRIM(stage))
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
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(NOW(), 'YYYY-MM-DD'), -- Default to current date if no sensible source for 'CloseDate'
        '1900-01-01' -- Fallback to a fixed date
    ) AS "CloseDate",
    amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    customer_number AS "AccountId", -- Assuming customer_number is the Salesforce Account Id
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
