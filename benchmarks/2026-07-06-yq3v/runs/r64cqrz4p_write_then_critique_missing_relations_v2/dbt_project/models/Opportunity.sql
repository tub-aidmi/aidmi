-- noinspection SqlNoDataSourceInspectionForFile
{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, '') AS "Name",
    CASE
        WHEN LOWER(o.stage) LIKE '%prospect%' THEN 'Prospecting'
        WHEN LOWER(o.stage) LIKE '%qualif%' THEN 'Qualification'
        WHEN LOWER(o.stage) LIKE '%needs analy%' THEN 'Needs Analysis'
        WHEN LOWER(o.stage) LIKE '%value prop%' THEN 'Value Proposition'
        WHEN LOWER(o.stage) LIKE '%decision maker%' THEN 'Id. Decision Makers'
        WHEN LOWER(o.stage) LIKE '%perception analy%' THEN 'Perception Analysis'
        WHEN LOWER(o.stage) LIKE '%proposal%' OR LOWER(o.stage) LIKE '%price quote%' THEN 'Proposal/Price Quote'
        WHEN LOWER(o.stage) LIKE '%negotiation%' OR LOWER(o.stage) LIKE '%review%' THEN 'Negotiation/Review'
        WHEN LOWER(o.stage) LIKE '%closed won%' THEN 'Closed Won'
        WHEN LOWER(o.stage) LIKE '%closed lost%' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate",
    o.amount AS "Amount",
    COALESCE(CAST(o.customer_number AS TEXT), '') AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON o.customer_number = a.id