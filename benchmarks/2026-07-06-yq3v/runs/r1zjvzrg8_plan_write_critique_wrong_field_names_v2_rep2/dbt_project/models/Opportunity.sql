{{ config(materialized='table') }}

SELECT
    chance.chance_id AS "Id",
    COALESCE(TRIM(INITCAP(chance.bezeichnung)), '') AS "Name",
    CASE
        WHEN LOWER(chance.phase) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(chance.phase) = 'qualification' THEN 'Qualification'
        WHEN LOWER(chance.phase) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(chance.phase) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(chance.phase) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(chance.phase) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(chance.phase) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(chance.phase) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(chance.phase) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(chance.phase) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN chance.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(chance.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE TO_CHAR(CAST('1900-01-01' AS DATE), 'YYYY-MM-DD')
    END AS "CloseDate",
    chance.volumen AS "Amount",
    TRIM(UPPER(chance.waehrung)) AS "CurrencyIsoCode",
    chance.kd_nr AS "AccountId",
    chance.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chance
