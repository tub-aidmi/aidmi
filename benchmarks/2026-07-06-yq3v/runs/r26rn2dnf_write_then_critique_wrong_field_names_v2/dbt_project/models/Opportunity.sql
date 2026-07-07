{{ config(materialized='table') }}

SELECT
    chancen.chance_id AS "Id",
    COALESCE(chancen.bezeichnung, 'Opportunity ' || chancen.chance_id) AS "Name",
    -- Map source phase to target StageName enum, with a default 'Prospecting' for unmapped or NULL values
    CASE chancen.phase
        WHEN 'Prospecting' THEN 'Prospecting'
        WHEN 'Qualification' THEN 'Qualification'
        WHEN 'Needs Analysis' THEN 'Needs Analysis'
        WHEN 'Value Proposition' THEN 'Value Proposition'
        WHEN 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN 'Perception Analysis' THEN 'Perception Analysis'
        WHEN 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN 'Closed Lost' THEN 'Closed Lost'
        WHEN 'Closed Won' THEN 'Closed Won'
        ELSE 'Prospecting' -- Default for unmapped or NULL phases (target is NOT NULL)
    END AS "StageName",
    -- Validate and parse abschlussdatum, falling back to end of current year if NULL or unparseable
    COALESCE(
        CASE
            WHEN chancen.abschlussdatum IS NULL THEN NULL
            WHEN chancen.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN
                -- Attempt to parse and format if it matches YYYY-MM-DD regex.
                -- This assumes source data, if it matches regex, does not contain
                -- semantically invalid dates (e.g., '2024-02-30') that would error TO_DATE.
                TO_CHAR(TO_DATE(chancen.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            ELSE NULL -- Treat non-matching format as unparseable
        END,
        TO_CHAR(DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year' - INTERVAL '1 day', 'YYYY-MM-DD')
    ) AS "CloseDate",
    chancen.volumen AS "Amount",
    chancen.waehrung AS "CurrencyIsoCode",
    chancen.kd_nr AS "AccountId",
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
