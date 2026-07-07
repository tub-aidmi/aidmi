{{ config(materialized='table') }}

SELECT
    ch.chance_id AS "Id",
    COALESCE(ch.bezeichnung, 'N/A') AS "Name",
    CASE
        WHEN LOWER(ch.phase) IN ('prospecting', 'lead generation') THEN 'Prospecting'
        WHEN LOWER(ch.phase) IN ('qualification', 'discovery') THEN 'Qualification'
        WHEN LOWER(ch.phase) IN ('needs analysis', 'solution design') THEN 'Needs Analysis'
        WHEN LOWER(ch.phase) IN ('value proposition', 'presentation') THEN 'Value Proposition'
        WHEN LOWER(ch.phase) IN ('id. decision makers', 'identify stakeholders') THEN 'Id. Decision Makers'
        WHEN LOWER(ch.phase) IN ('perception analysis', 'competitor analysis') THEN 'Perception Analysis'
        WHEN LOWER(ch.phase) IN ('proposal/price quote', 'quote sent') THEN 'Proposal/Price Quote'
        WHEN LOWER(ch.phase) IN ('negotiation/review', 'contract review') THEN 'Negotiation/Review'
        WHEN LOWER(ch.phase) IN ('closed won', 'won') THEN 'Closed Won'
        WHEN LOWER(ch.phase) IN ('closed lost', 'lost') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to a valid stage if unmapped or NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            TO_DATE(ch.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'
        ),
        TO_CHAR(
            TO_DATE(ch.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'
        ),
        '1900-01-01' -- Default to a valid date if parsing fails for this NOT NULL field
    ) AS "CloseDate",
    ch.volumen AS "Amount",
    ch.waehrung AS "CurrencyIsoCode",
    ch.kd_nr AS "AccountId", -- AccountId is kunden_nr from the kunden table
    ch.chance_id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS ch
WHERE
    ch.chance_id IS NOT NULL
    AND COALESCE(ch.bezeichnung, '') != ''
