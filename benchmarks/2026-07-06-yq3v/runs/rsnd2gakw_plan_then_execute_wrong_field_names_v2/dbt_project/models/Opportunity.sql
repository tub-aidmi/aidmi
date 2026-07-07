{{ config(materialized='table') }}

SELECT
    -- Id: Generate a stable ID from the source opportunity ID.
    MD5(chancen.chance_id)::TEXT AS "Id",

    -- Name: Use the opportunity description, defaulting to 'Unnamed Opportunity' if null.
    COALESCE(chancen.bezeichnung, 'Unnamed Opportunity') AS "Name",

    -- StageName: Map source phase to target enum, defaulting to 'Prospecting'.
    CASE
        WHEN LOWER(TRIM(chancen.phase)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(chancen.phase)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(chancen.phase)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(chancen.phase)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(chancen.phase)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(chancen.phase)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(chancen.phase)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(chancen.phase)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(chancen.phase)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(chancen.phase)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default value for unmapped phases
    END AS "StageName",

    -- CloseDate: Parse various date formats to YYYY-MM-DD, defaulting to current date if unparseable.
    COALESCE(
        TO_CHAR(
            CASE
                WHEN chancen.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(chancen.abschlussdatum, 'DD.MM.YYYY')
                WHEN chancen.abschlussdatum ~ '^\d{8}$' THEN TO_DATE(chancen.abschlussdatum, 'YYYYMMDD')
                WHEN chancen.abschlussdatum ~ '^\d{1,2}\/\d{1,2}\/\d{4}$' THEN TO_DATE(chancen.abschlussdatum, 'MM/DD/YYYY')
                ELSE NULL
            END,
            'YYYY-MM-DD'
        ),
        CURRENT_DATE::TEXT
    ) AS "CloseDate",

    -- Amount: Directly map the volume.
    chancen.volumen AS "Amount",

    -- CurrencyIsoCode: Directly map the currency.
    chancen.waehrung AS "CurrencyIsoCode",

    -- AccountId: Generate a stable ID for the related Account based on kunden.kunden_nr.
    MD5(kunden.kunden_nr)::TEXT AS "AccountId",

    -- Legacy_Opportunity_ID__c: Store the original opportunity ID for lineage.
    chancen.chance_id AS "Legacy_Opportunity_ID__c",

    -- CreatedDate: Set to NULL as no source is specified.
    NULL::TEXT AS "CreatedDate",

    -- LastModifiedDate: Set to NULL as no source is specified.
    NULL::TEXT AS "LastModifiedDate",

    -- IsDeleted: Default to 0.
    0 AS "IsDeleted"

FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON chancen.kd_nr = kunden.kunden_nr