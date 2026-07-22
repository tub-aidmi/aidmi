-- This dbt model transforms raw opportunity data into the target Opportunity schema.
-- It handles mapping of source fields, type conversions, and enum normalization.

{{ config(materialized='table') }}

WITH cleaned_chancen AS (
    SELECT
        TRIM(chance_id) AS chance_id,
        TRIM(bezeichnung) AS bezeichnung,
        TRIM(LOWER(phase)) AS phase,
        TRIM(abschlussdatum) AS abschlussdatum,
        volumen,
        TRIM(waehrung) AS waehrung,
        TRIM(kd_nr) AS kd_nr
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
),

stage_mapping AS (
    SELECT
        chance_id,
        bezeichnung,
        CASE
            WHEN phase IN ('prospecting', 'akquise') THEN 'Prospecting'
            WHEN phase IN ('qualification', 'qualifizierung') THEN 'Qualification'
            WHEN phase IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
            WHEN phase IN ('value proposition', 'wertangebot') THEN 'Value Proposition'
            WHEN phase IN ('id. decision makers', 'entscheidungsträger identifizieren') THEN 'Id. Decision Makers'
            WHEN phase IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
            WHEN phase IN ('proposal/price quote', 'angebot/preisauszeichnung') THEN 'Proposal/Price Quote'
            WHEN phase IN ('negotiation/review', 'verhandlung/überprüfung') THEN 'Negotiation/Review'
            WHEN phase IN ('closed won', 'gewonnen') THEN 'Closed Won'
            WHEN phase IN ('closed lost', 'verloren') THEN 'Closed Lost'
            ELSE 'Prospecting' -- Default for unmapped phases
        END AS stage_name,
        abschlussdatum,
        volumen,
        waehrung,
        kd_nr
    FROM cleaned_chancen
)

SELECT
    s.chance_id AS