{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(NULLIF(TRIM(o.name), ''), 'Unknown') AS "Name",
    CASE 
        WHEN TRIM(o.stage) IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost') 
        THEN INITCAP(LOWER(TRIM(o.stage)))
        ELSE NULL
    END AS "StageName",
    NULL AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON TRIM(o.account_name) = TRIM(a.name)