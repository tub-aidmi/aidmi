{{ config(materialized='table') }}

WITH opportunity_base AS (
    SELECT
        c.chance_id,
        c.bezeichnung,
        c.phase,
        c.abschlussdatum,
        c.volumen,
        c.waehrung,
        c.kd_nr,
        k.kunden_nr AS account_kunden_nr
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
        ON c.kd_nr = k.kunden_nr
)

SELECT
    '001' || SUBSTRING(md5(chance_id), 1, 15) AS "Id",
    COALESCE(TRIM(bezeichnung), 'Unknown') AS "Name",
    CASE
        WHEN phase = 'Prospecting' THEN 'Prospecting'
        WHEN phase = 'Qualification' THEN 'Qualification'
        WHEN phase = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN phase = 'Value Proposition' THEN 'Value Proposition'
        WHEN phase = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN phase = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN phase = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN phase = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN phase = 'Closed Won' THEN 'Closed Won'
        WHEN phase = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' 
            THEN abschlussdatum
        ELSE NULL
    END AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    account_kunden_nr AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunity_base