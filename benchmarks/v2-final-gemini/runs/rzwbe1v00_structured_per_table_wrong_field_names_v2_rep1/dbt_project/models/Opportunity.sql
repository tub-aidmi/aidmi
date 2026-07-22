{{ config(materialized='table') }}

SELECT
    chancen.chance_id AS "Id",
    COALESCE(chancen.bezeichnung, 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN chancen.phase IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost') THEN chancen.phase
        ELSE 'Prospecting' -- Default to 'Prospecting' if phase is NULL or not in the enum
    END AS "StageName",
    COALESCE(
        CASE
            WHEN chancen.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN chancen.abschlussdatum
            ELSE NULL
        END,
        '1900-01-01' -- Default date if source is NULL or invalid format
    ) AS "CloseDate",
    chancen.volumen AS "Amount",
    chancen.waehrung AS "CurrencyIsoCode",
    chancen.kd_nr AS "AccountId",
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
