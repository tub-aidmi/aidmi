{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(NULLIF(TRIM(o.name), ''), 'Unknown') AS "Name",
    CASE
        WHEN TRIM(o.stage) IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost') THEN TRIM(o.stage)
        ELSE 'Prospecting'
    END AS "StageName",
    '1970-01-01' AS "CloseDate",
    o.amount AS "Amount",
    'EUR' AS "CurrencyIsoCode",
    REPLACE(o.customer_number, 'KD-', 'ACC-') AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
