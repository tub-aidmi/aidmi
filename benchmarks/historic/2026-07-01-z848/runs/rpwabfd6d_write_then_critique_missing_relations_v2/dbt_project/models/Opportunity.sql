
{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(src.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN src.stage IS NULL THEN 'Prospecting'
        WHEN src.stage ILIKE 'Prospecting' THEN 'Prospecting'
        WHEN src.stage ILIKE 'Qualification' THEN 'Qualification'
        WHEN src.stage ILIKE 'Needs Analysis' THEN 'Needs Analysis'
        WHEN src.stage ILIKE 'Value Proposition' THEN 'Value Proposition'
        WHEN src.stage ILIKE 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN src.stage ILIKE 'Perception Analysis' THEN 'Perception Analysis'
        WHEN src.stage ILIKE 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN src.stage ILIKE 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN src.stage ILIKE 'Closed Won' THEN 'Closed Won'
        WHEN src.stage ILIKE 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate",
    src.amount AS "Amount",
    NULL::TEXT AS "CurrencyIsoCode",
    src.customer_number AS "AccountId",
    NULL::TEXT AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS src
