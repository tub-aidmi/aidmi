-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}

{{ config(materialized='table') }}

SELECT
    c.chance_id AS "Id",
    COALESCE(c.bezeichnung, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(c.phase)) = 'NEU' THEN 'Prospecting'
        WHEN UPPER(TRIM(c.phase)) = 'QUALIFIZIERUNG' THEN 'Qualification'
        WHEN UPPER(TRIM(c.phase)) = 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(c.phase)) = 'WERTVORSCHLAG' THEN 'Value Proposition'
        WHEN UPPER(TRIM(c.phase)) = 'ENTSCHEIDUNGSTRÄGER IDENTIFIZIERT' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(c.phase)) = 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(c.phase)) = 'ANGEBOT/PREISVERHANDLUNG' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(c.phase)) = 'VERHANDLUNG/PRÜFUNG' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(c.phase)) = 'GEWONNEN' THEN 'Closed Won'
        WHEN UPPER(TRIM(c.phase)) = 'VERLOREN' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for unmapped phases, as StageName is NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(c.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(c.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for unparseable or NULL dates, as CloseDate is NOT NULL
    ) AS "CloseDate",
    c.volumen AS "Amount",
    COALESCE(UPPER(TRIM(c.waehrung)), 'EUR') AS "CurrencyIsoCode",
    -- Generate AccountId deterministically from kunden_nr, same logic as Account model
    MD5(COALESCE(k.kunden_nr, '')) AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate", -- No source for CreatedDate
    NULL AS "LastModifiedDate", -- No source for LastModifiedDate
    0 AS "IsDeleted" -- Default to not deleted
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON c.kd_nr = k.kunden_nr