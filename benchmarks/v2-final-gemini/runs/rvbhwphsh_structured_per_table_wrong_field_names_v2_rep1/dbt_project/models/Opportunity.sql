-- depends_on: {{ ref('Account') }}

{{ config(materialized='table') }}

SELECT
    chancen.chance_id AS "Id",
    COALESCE(TRIM(chancen.bezeichnung), 'Unknown Opportunity ' || chancen.chance_id) AS "Name",
    CASE LOWER(TRIM(chancen.phase))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default stage for unmapped or NULL values
    END AS "StageName",
    COALESCE(
        CASE
            WHEN chancen.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(chancen.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            ELSE '1900-01-01' -- Fallback for unparseable or different date formats
        END,
        '1900-01-01'
    ) AS "CloseDate",
    chancen.volumen AS "Amount",
    chancen.waehrung AS "CurrencyIsoCode",
    kunden.kunden_nr AS "AccountId", -- Assuming kunden_nr directly maps to AccountId for simplicity without a separate Account model reference
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate", -- No direct source, default to NULL
    NULL AS "LastModifiedDate", -- No direct source, default to NULL
    0 AS "IsDeleted" -- Default to not deleted
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON chancen.kd_nr = kunden.kunden_nr