-- This dbt model maps raw opportunity data to the Salesforce Opportunity schema.
{{ config(materialized='table') }}

SELECT
    chance.chance_id AS "Id",
    COALESCE(chance.bezeichnung, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(chance.phase) LIKE '%won%' THEN 'Closed Won'
        WHEN LOWER(chance.phase) LIKE '%lost%' THEN 'Closed Lost'
        WHEN LOWER(chance.phase) LIKE '%proposal%' THEN 'Proposal/Price Quote'
        WHEN LOWER(chance.phase) LIKE '%negotiation%' THEN 'Negotiation/Review'
        WHEN LOWER(chance.phase) LIKE '%analysis%' THEN 'Needs Analysis'
        WHEN LOWER(chance.phase) LIKE '%qualification%' THEN 'Qualification'
        WHEN LOWER(chance.phase) LIKE '%id. decision makers%' THEN 'Id. Decision Makers'
        WHEN LOWER(chance.phase) LIKE '%value proposition%' THEN 'Value Proposition'
        WHEN LOWER(chance.phase) LIKE '%perception analysis%' THEN 'Perception Analysis'
        ELSE 'Prospecting' -- Default for unmapped phases and NULL
    END AS "StageName",
    TO_CHAR(
        COALESCE(
            -- Attempt to parse common date formats for robustness
            CASE WHEN chance.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(chance.abschlussdatum, 'YYYY-MM-DD') END,
            CASE WHEN chance.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(chance.abschlussdatum, 'DD.MM.YYYY') END,
            CASE WHEN chance.abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(chance.abschlussdatum, 'MM/DD/YYYY') END,
            CURRENT_DATE -- Fallback to current date if parsing fails or source is NULL
        ),
        'YYYY-MM-DD'
    ) AS "CloseDate",
    chance.volumen AS "Amount",
    chance.waehrung AS "CurrencyIsoCode",
    kunden.kunden_nr AS "AccountId",
    chance.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chance
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON chance.kd_nr = kunden.kunden_nr