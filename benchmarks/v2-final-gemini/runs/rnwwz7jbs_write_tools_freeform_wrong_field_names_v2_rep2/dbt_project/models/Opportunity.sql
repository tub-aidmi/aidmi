{{ config(materialized='table') }}

SELECT
    chance_id AS "Id",
    COALESCE(bezeichnung, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN phase IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost') THEN phase
        ELSE NULL
    END AS "StageName",
    -- Assuming abschlussdatum is already in YYYY-MM-DD format based on inspection
    COALESCE(abschlussdatum, '1970-01-01') AS "CloseDate", -- CloseDate is NOT NULL
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    kd_nr AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
