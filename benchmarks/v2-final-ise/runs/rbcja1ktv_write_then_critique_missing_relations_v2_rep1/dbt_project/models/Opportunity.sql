{{ config(materialized='table') }}
SELECT
    opp.id AS "Id",
    opp.name AS "Name",
    CASE
        WHEN UPPER(TRIM(opp.stage)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(opp.stage)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(opp.stage)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(opp.stage)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(opp.stage)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(opp.stage)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(opp.stage)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(opp.stage)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(opp.stage)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(opp.stage)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    NULL::text AS "CloseDate",
    CAST(opp.amount AS DOUBLE PRECISION) AS "Amount",
    NULL::text AS "CurrencyIsoCode",
    acc.id AS "AccountId",
    opp.customer_number AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opp
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
    ON TRIM(opp.account_name) = TRIM(acc.name)