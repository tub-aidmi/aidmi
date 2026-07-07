-- no-op
{{ config(materialized='table') }}

SELECT
    c.chance_id AS "Id",
    COALESCE(c.bezeichnung, 'Opportunity ' || c.chance_id) AS "Name",
    CASE
        WHEN c.phase = 'Prospecting' THEN 'Prospecting'
        WHEN c.phase = 'Qualification' THEN 'Qualification'
        -- Source 'phase' does not contain values for these target stages, so they will fall through to the default.
        -- 'Needs Analysis',
        -- 'Value Proposition',
        -- 'Id. Decision Makers',
        -- 'Perception Analysis',
        -- 'Proposal/Price Quote',
        -- 'Negotiation/Review',
        WHEN c.phase = 'Closed Won' THEN 'Closed Won'
        WHEN c.phase = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for unmappable or NULL values to satisfy NOT NULL constraint
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN c.abschlussdatum IS NOT NULL AND c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(c.abschlussdatum, 'YYYY-MM-DD')
                ELSE NULL
            END,
            'YYYY-MM-DD'
        ),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "CloseDate",
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    k.kunden_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0::integer AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    c.kd_nr = k.kunden_nr