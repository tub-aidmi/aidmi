{{ config(materialized='table') }}

SELECT
    chance_id AS "Id",
    COALESCE(bezeichnung, '') AS "Name",
    CASE
        WHEN phase IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost')
            THEN phase
        ELSE 'Prospecting' -- Default for StageName if not in valid enum or NULL
    END AS "StageName",
    COALESCE(abschlussdatum, CURRENT_DATE::TEXT) AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    kd_nr AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
