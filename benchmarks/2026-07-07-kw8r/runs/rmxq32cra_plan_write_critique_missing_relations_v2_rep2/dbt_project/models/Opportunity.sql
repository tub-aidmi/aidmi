{{ config(materialized='table') }}

SELECT
    TRIM(o.id) AS "Id",
    INITCAP(TRIM(COALESCE(o.name, ''))) AS "Name",
    CASE UPPER(TRIM(o.stage))
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
    NULL::TEXT AS "CloseDate",
    CAST(o.amount AS DOUBLE PRECISION) AS "Amount",
    NULL::TEXT AS "CurrencyIsoCode",
    TRIM(UPPER(ac.id)) AS "AccountId",
    TRIM(o.id) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
     0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} ac
    ON TRIM(UPPER(o.customer_number)) = TRIM(UPPER(ac.id))