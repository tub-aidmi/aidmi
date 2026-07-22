{{ config(materialized='table') }}
SELECT
    opp.id AS "Id",
    INITCAP(TRIM(opp.name)) AS "Name",
    CASE
        WHEN TRIM(UPPER(opp.stage)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN TRIM(UPPER(opp.stage)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN TRIM(UPPER(opp.stage)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN TRIM(UPPER(opp.stage)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN TRIM(UPPER(opp.stage)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN TRIM(UPPER(opp.stage)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN TRIM(UPPER(opp.stage)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN TRIM(UPPER(opp.stage)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN TRIM(UPPER(opp.stage)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN TRIM(UPPER(opp.stage)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    NULL AS "CloseDate",
    opp.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    COALESCE(acc.id, NULL) AS "AccountId",
    opp.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} opp
LEFT JOIN LATERAL (
    SELECT acc.id
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    WHERE REPLACE(opp.customer_number, 'KD-', 'ACC-') = acc.id
    UNION ALL
    SELECT acc.id
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    WHERE TRIM(LOWER(opp.account_name)) = TRIM(LOWER(acc.name))
    LIMIT 1
) acc ON true