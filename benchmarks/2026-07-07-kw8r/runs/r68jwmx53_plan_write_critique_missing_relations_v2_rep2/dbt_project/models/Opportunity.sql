{{ config(materialized='table') }}
SELECT
    o.id AS "Id",
    INITCAP(o.name) AS "Name",
    CASE
        WHEN INITCAP(o.stage) IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost')
        THEN INITCAP(o.stage)
        ELSE NULL
    END AS "StageName",
    NULL AS "CloseDate",
    o.amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON REPLACE(o.customer_number, 'KD-', 'ACC-') = a.id