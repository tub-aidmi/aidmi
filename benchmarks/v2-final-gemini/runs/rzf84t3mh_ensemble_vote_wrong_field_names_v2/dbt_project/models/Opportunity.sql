{{ config(materialized='table') }}

SELECT
    c.chance_id AS "Id",
    c.bezeichnung AS "Name",
    CASE
        WHEN c.phase = 'Initial' THEN 'Prospecting'
        WHEN c.phase = 'Qualifizierung' THEN 'Qualification'
        WHEN c.phase = 'Bedarfsanalyse' THEN 'Needs Analysis'
        WHEN c.phase = 'Werteangebot' THEN 'Value Proposition'
        WHEN c.phase = 'Entscheider Identifiziert' THEN 'Id. Decision Makers'
        WHEN c.phase = 'Wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN c.phase = 'Angebot/Preis' THEN 'Proposal/Price Quote'
        WHEN c.phase = 'Verhandlung/Prüfung' THEN 'Negotiation/Review'
        WHEN c.phase = 'Geschlossen Gewonnen' THEN 'Closed Won'
        WHEN c.phase = 'Geschlossen Verloren' THEN 'Closed Lost'
        WHEN c.phase IS NULL THEN 'Prospecting' -- Default for NULL
        ELSE 'Prospecting' -- Default for unknown phases
    END AS "StageName",
    TO_CHAR(
        CASE
            WHEN c.abschlussdatum IS NOT NULL AND c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN c.abschlussdatum::DATE
            WHEN c.abschlussdatum IS NOT NULL AND c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(c.abschlussdatum, 'DD.MM.YYYY')
            WHEN c.abschlussdatum IS NOT NULL AND c.abschlussdatum ~ '^\d{8}$' THEN TO_DATE(c.abschlussdatum, 'YYYYMMDD')
            ELSE '1900-01-01'::DATE -- Default date for unparseable or NULL
        END,
        'YYYY-MM-DD'
    ) AS "CloseDate",
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    k.kunden_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    c.kd_nr = k.kunden_nr;