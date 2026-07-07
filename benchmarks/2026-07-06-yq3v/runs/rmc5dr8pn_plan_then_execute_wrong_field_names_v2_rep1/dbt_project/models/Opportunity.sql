{{ config(materialized='table') }}

SELECT
    ch.chance_id AS "Id",
    COALESCE(ch.bezeichnung, 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN UPPER(ch.phase) = 'PROSPEKTION' THEN 'Prospecting'
        WHEN UPPER(ch.phase) = 'QUALIFIKATION' THEN 'Qualification'
        WHEN UPPER(ch.phase) = 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN UPPER(ch.phase) = 'WERTANGEBOT' THEN 'Value Proposition'
        WHEN UPPER(ch.phase) = 'ENTSCHEIDER IDENTIFIZIEREN' THEN 'Id. Decision Makers'
        WHEN UPPER(ch.phase) = 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN UPPER(ch.phase) = 'ANGEBOT/PREIS' THEN 'Proposal/Price Quote'
        WHEN UPPER(ch.phase) = 'VERHANDLUNG/ÜBERPRÜFUNG' THEN 'Negotiation/Review'
        WHEN UPPER(ch.phase) = 'GESCHLOSSEN GEWONNEN' THEN 'Closed Won'
        WHEN UPPER(ch.phase) = 'GESCHLOSSEN VERLOREN' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to Prospecting if phase is unmapped or NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(ch.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(ch.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(ch.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(ch.abschlussdatum, 'YYYYMMDD'), 'YYYY-MM-DD'),
        '2099-12-31' -- Default value for NOT NULL CloseDate
    ) AS "CloseDate",
    ch.volumen AS "Amount",
    COALESCE(ch.waehrung, 'EUR') AS "CurrencyIsoCode",
    k.kunden_nr AS "AccountId",
    ch.chance_id AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS ch
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON ch.kd_nr = k.kunden_nr
