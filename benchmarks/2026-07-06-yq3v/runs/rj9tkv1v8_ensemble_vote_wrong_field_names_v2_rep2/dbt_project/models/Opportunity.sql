{{ config(materialized='table') }}

SELECT
    -- Id
    ch.chance_id AS "Id",

    -- Name
    COALESCE(ch.bezeichnung, 'Unknown Opportunity Name') AS "Name",

    -- StageName
    CASE
        WHEN ch.phase = 'Prospecting' THEN 'Prospecting'
        WHEN ch.phase = 'Qualifizierung' THEN 'Qualification'
        WHEN ch.phase = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN ch.phase = 'Value Proposition' THEN 'Value Proposition'
        WHEN ch.phase = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN ch.phase = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN ch.phase = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN ch.phase = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN ch.phase = 'Closed Won' THEN 'Closed Won'
        WHEN ch.phase = 'Closed Lost' THEN 'Closed Lost'
        -- German common mappings
        WHEN ch.phase = 'Initial' THEN 'Prospecting'
        WHEN ch.phase = 'Analyse' THEN 'Needs Analysis'
        WHEN ch.phase = 'Angebot' THEN 'Proposal/Price Quote'
        WHEN ch.phase = 'Gewonnen' THEN 'Closed Won'
        WHEN ch.phase = 'Verloren' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",

    -- CloseDate
    CASE
        WHEN ch.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(ch.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN ch.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ch.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ch.abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(ch.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL if parsing fails
    END AS "CloseDate",

    -- Amount
    ch.volumen AS "Amount",

    -- CurrencyIsoCode
    ch.waehrung AS "CurrencyIsoCode",

    -- AccountId
    ch.kd_nr AS "AccountId",

    -- Legacy_Opportunity_ID__c
    ch.chance_id AS "Legacy_Opportunity_ID__c",

    -- CreatedDate (not in source, default to NULL)
    NULL AS "CreatedDate",

    -- LastModifiedDate (not in source, default to NULL)
    NULL AS "LastModifiedDate",

    -- IsDeleted (not in source, default to 0 for active)
    0 AS "IsDeleted"

FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS ch
