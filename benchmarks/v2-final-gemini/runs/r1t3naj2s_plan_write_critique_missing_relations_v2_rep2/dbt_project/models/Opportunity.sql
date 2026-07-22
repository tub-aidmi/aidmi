{{ config(materialized='table') }}

SELECT
    MD5(opportunity.id) AS "Id",
    COALESCE(opportunity.name, 'Unknown Opportunity') AS "Name",
    COALESCE(
        CASE UPPER(TRIM(opportunity.stage))
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
        END,
        'Prospecting'
    ) AS "StageName",
    CURRENT_DATE::TEXT AS "CloseDate",
    opportunity.amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    MD5(opportunity.customer_number) AS "AccountId",
    opportunity.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opportunity