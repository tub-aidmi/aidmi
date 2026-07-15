{{ config(materialized='table') }}

SELECT
    CAST(o.id AS TEXT) AS "Id",
    INITCAP(TRIM(o.name)) AS "Name",
    CASE
        WHEN UPPER(TRIM(o.stage)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(o.stage)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(o.stage)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(o.stage)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(o.stage)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(o.stage)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(o.stage)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(o.stage)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(o.stage)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(o.stage)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL -- Source data contains only 4 of 10 enum values; unmapped stages intentionally NULL
    END AS "StageName",
    NULL::TEXT AS "CloseDate",
    CAST(o.amount AS DOUBLE PRECISION) AS "Amount",
    CAST('EUR' AS TEXT) AS "CurrencyIsoCode",
    CAST(a.id AS TEXT) AS "AccountId",
    CAST(o.id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON regexp_replace(o.customer_number, '^KD-', 'ACC-') = a.id