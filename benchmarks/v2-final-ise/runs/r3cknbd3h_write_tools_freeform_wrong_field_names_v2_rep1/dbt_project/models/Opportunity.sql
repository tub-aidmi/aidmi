{{ config(materialized='table') }}

SELECT
    chance_id AS "Id",
    COALESCE(NULLIF(TRIM(bezeichnung), ''), 'Unknown') AS "Name",
    COALESCE(
        CASE
            WHEN UPPER(phase) = 'PROSPEKTION' THEN 'Prospecting'
            WHEN UPPER(phase) = 'QUALIFIZIERUNG' THEN 'Qualification'
            WHEN UPPER(phase) = 'BEDARFSANALYSE' THEN 'Needs Analysis'
            WHEN UPPER(phase) = 'WERTVORSCHLAG' THEN 'Value Proposition'
            WHEN UPPER(phase) = 'ENTSCHEIDUNGSTRÄGER' THEN 'Id. Decision Makers'
            WHEN UPPER(phase) = 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
            WHEN UPPER(phase) = 'ANGEBOT' THEN 'Proposal/Price Quote'
            WHEN UPPER(phase) = 'VERHANDLUNG' THEN 'Negotiation/Review'
            WHEN UPPER(phase) = 'GESCHLOSSEN GEWONNEN' THEN 'Closed Won'
            WHEN UPPER(phase) = 'GESCHLOSSEN VERLOREN' THEN 'Closed Lost'
            ELSE NULL
        END,
        'Prospecting'
    ) AS "StageName",
    CASE
        WHEN abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN abschlussdatum
        WHEN abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN abschlussdatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    k.kunden_nr AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON c.kd_nr = k.kunden_nr
