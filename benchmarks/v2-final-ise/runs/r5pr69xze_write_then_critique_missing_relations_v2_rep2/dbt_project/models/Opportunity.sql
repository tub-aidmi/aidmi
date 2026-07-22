{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(name, 'Unknown') AS "Name",
    CASE
        WHEN UPPER(TRIM(stage)) IN ('PROSPECTING', 'QUALIFICATION', 'NEEDS ANALYSIS', 'VALUE PROPOSITION', 'ID. DECISION MAKERS', 'PERCEPTION ANALYSIS', 'PROPOSAL/PRICE QUOTE', 'NEGOTIATION/REVIEW', 'CLOSED WON', 'CLOSED LOST') THEN INITCAP(TRIM(stage))
        ELSE COALESCE(INITCAP(TRIM(COALESCE(stage, ''))), 'Prospecting')
    END AS "StageName",
    '1900-01-01' AS "CloseDate",
    CAST(amount AS DOUBLE PRECISION) AS "Amount",
    NULL::TEXT AS "CurrencyIsoCode",
    CASE
        WHEN customer_number IS NOT NULL THEN REGEXP_REPLACE(customer_number, '^KD-', 'ACC-')
        ELSE NULL
    END AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}