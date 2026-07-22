{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Opportunity') AS "Name",
    COALESCE(
        CASE
            WHEN stage = 'Prospecting' THEN 'Prospecting'
            WHEN stage = 'Qualification' THEN 'Qualification'
            WHEN stage = 'Closed Won' THEN 'Closed Won'
            WHEN stage = 'Closed Lost' THEN 'Closed Lost'
            ELSE 'Prospecting'
        END,
        'Prospecting'
    ) AS "StageName",
    '1900-01-01'::text AS "CloseDate",
    amount AS "Amount",
    NULL::text AS "CurrencyIsoCode",
    NULL::text AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
