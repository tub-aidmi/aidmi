{{ config(materialized='table') }}

WITH opportunity_source AS (
    SELECT
        c.chance_id,
        TRIM(c.bezeichnung) AS bezeichnung,
        c.phase,
        c.abschlussdatum,
        c.volumen,
        c.waehrung,
        c.kd_nr,
        k.kunden_nr
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
        ON c.kd_nr = k.kunden_nr
)

SELECT
    md5(chance_id || 'opportunity_salt') AS "Id",
    COALESCE(NULLIF(TRIM(bezeichnung), ''), chance_id) AS "Name",
    CASE
        WHEN phase = 'Qualifizierung' THEN 'Qualification'
        WHEN phase = 'Angebot' THEN 'Proposal/Price Quote'
        WHEN phase = 'Verhandlung' THEN 'Negotiation/Review'
        WHEN phase = 'Gewonnen' THEN 'Closed Won'
        WHEN phase = 'Verloren' THEN 'Closed Lost'
        WHEN phase IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost') THEN phase
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN abschlussdatum
        WHEN abschlussdatum IS NOT NULL THEN TO_CHAR(TO_DATE(abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    md5(kunden_nr || 'account_salt') AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunity_source