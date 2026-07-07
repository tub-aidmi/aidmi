{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(TRIM(o.name), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(o.stage) LIKE '%prospect%' THEN 'Prospecting'
        WHEN LOWER(o.stage) LIKE '%qualificat%' THEN 'Qualification'
        WHEN LOWER(o.stage) LIKE '%need analysi%' THEN 'Needs Analysis'
        WHEN LOWER(o.stage) LIKE '%value prop%' THEN 'Value Proposition'
        WHEN LOWER(o.stage) LIKE '%decision maker%' THEN 'Id. Decision Makers'
        WHEN LOWER(o.stage) LIKE '%perception analysi%' THEN 'Perception Analysis'
        WHEN LOWER(o.stage) LIKE '%proposal%' THEN 'Proposal/Price Quote'
        WHEN LOWER(o.stage) LIKE '%negotiat%' THEN 'Negotiation/Review'
        WHEN LOWER(o.stage) LIKE '%closed won%' THEN 'Closed Won'
        WHEN LOWER(o.stage) LIKE '%won%' THEN 'Closed Won'
        WHEN LOWER(o.stage) LIKE '%closed lost%' THEN 'Closed Lost'
        WHEN LOWER(o.stage) LIKE '%lost%' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    '2000-01-01'::TEXT AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON o.customer_number = a.id