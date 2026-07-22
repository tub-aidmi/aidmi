-- depends_on: {{ ref('account') }}

{{ config(materialized='table') }}

SELECT
    TRIM(o.id) AS "Id",
    TRIM(o.name) AS "Name",
    CASE TRIM(o.stage)
        WHEN 'Prospecting' THEN 'Prospecting'
        WHEN 'Qualification' THEN 'Qualification'
        WHEN 'Needs Analysis' THEN 'Needs Analysis'
        WHEN 'Value Proposition' THEN 'Value Proposition'
        WHEN 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN 'Perception Analysis' THEN 'Perception Analysis'
        WHEN 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN 'Closed Won' THEN 'Closed Won'
        WHEN 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default value for unmapped stages
    END AS "StageName",
    CAST('2000-01-01' AS TEXT) AS "CloseDate",
    o.amount AS "Amount",
    CAST('USD' AS TEXT) AS "CurrencyIsoCode",
    a.id AS "AccountId",
    TRIM(o.id) AS "Legacy_Opportunity_ID__c",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "CreatedDate",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    CONCAT('ACC-', SUBSTRING(TRIM(o.customer_number) FROM '-(\\d+)$')) = a.id