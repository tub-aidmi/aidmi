-- noinspection SqlNoDataSourceInspectionForFile

{{ config(materialized='table') }}

SELECT
    opp.id AS "Id",
    COALESCE(opp.name, 'Untitled Opportunity') AS "Name",
    CASE UPPER(TRIM(opp.stage))
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
        ELSE 'Prospecting' -- Default to 'Prospecting' as per plan for NOT NULL constraint
    END AS "StageName",
    '1900-01-01' AS "CloseDate",
    opp.amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    acc.id AS "AccountId",
    opp.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opp
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
ON
    REPLACE(opp.customer_number, 'KD-', 'ACC-') = acc.id
