{{ config(materialized='table') }}

SELECT
    CAST(o.id AS TEXT) AS "Id",
    INITCAP(TRIM(o.name)) AS "Name",
    CASE LOWER(TRIM(COALESCE(o.stage, '')))
        WHEN 'new' THEN 'Prospecting'
        WHEN 'lead' THEN 'Prospecting'
        WHEN 'qualified' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'proposal' THEN 'Proposal/Price Quote'
        WHEN 'quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        WHEN 'closed' THEN 'Closed Won'
        ELSE NULL
    END AS "StageName",
    '1900-01-01' AS "CloseDate",
    CAST(o.amount AS DOUBLE PRECISION) AS "Amount",
    'USD' AS "CurrencyIsoCode",
    CAST(acc.id AS TEXT) AS "AccountId",
    CAST(o.id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    ON TRIM(o.customer_number) = TRIM(acc.id)