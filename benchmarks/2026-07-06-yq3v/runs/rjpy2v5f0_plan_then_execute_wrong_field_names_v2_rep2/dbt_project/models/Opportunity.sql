{{ config(materialized='table') }}

SELECT
    MD5(chancen.chance_id) AS "Id",
    COALESCE(INITCAP(TRIM(chancen.bezeichnung)), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(chancen.phase) = 'qualifizierung' THEN 'Qualification'
        WHEN LOWER(chancen.phase) = 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN LOWER(chancen.phase) = 'wertangebot' THEN 'Value Proposition'
        WHEN LOWER(chancen.phase) = 'entscheidungsträger identifiziert' THEN 'Id. Decision Makers'
        WHEN LOWER(chancen.phase) = 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN LOWER(chancen.phase) = 'angebot/preisangebot' THEN 'Proposal/Price Quote'
        WHEN LOWER(chancen.phase) = 'verhandlung/prüfung' THEN 'Negotiation/Review'
        WHEN LOWER(chancen.phase) = 'geschlossen gewonnen' THEN 'Closed Won'
        WHEN LOWER(chancen.phase) = 'geschlossen verloren' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        TO_CHAR(CASE WHEN chancen.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(chancen.abschlussdatum, 'YYYY-MM-DD') END, 'YYYY-MM-DD'),
        TO_CHAR(CASE WHEN chancen.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(chancen.abschlussdatum, 'DD.MM.YYYY') END, 'YYYY-MM-DD'),
        TO_CHAR(CASE WHEN chancen.abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(chancen.abschlussdatum, 'MM/DD/YYYY') END, 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "CloseDate",
    chancen.volumen AS "Amount",
    COALESCE(UPPER(TRIM(chancen.waehrung)), 'EUR') AS "CurrencyIsoCode",
    MD5(kunden.kunden_nr) AS "AccountId",
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
ON
    chancen.kd_nr = kunden.kunden_nr
