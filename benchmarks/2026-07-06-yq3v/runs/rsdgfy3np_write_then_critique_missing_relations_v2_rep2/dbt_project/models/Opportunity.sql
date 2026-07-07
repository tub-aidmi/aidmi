-- This model is for the Opportunity target table.
{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(TRIM(o.name), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(o.stage) = 'new' THEN 'Prospecting'
        WHEN LOWER(o.stage) = 'qualified' THEN 'Qualification'
        WHEN LOWER(o.stage) = 'analysis' THEN 'Needs Analysis'
        WHEN LOWER(o.stage) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(o.stage) = 'decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(o.stage) = 'proposal' THEN 'Proposal/Price Quote'
        WHEN LOWER(o.stage) = 'negotiation' THEN 'Negotiation/Review'
        WHEN LOWER(o.stage) = 'won' THEN 'Closed Won'
        WHEN LOWER(o.stage) = 'lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON TRIM(o.customer_number) = TRIM(a.id)