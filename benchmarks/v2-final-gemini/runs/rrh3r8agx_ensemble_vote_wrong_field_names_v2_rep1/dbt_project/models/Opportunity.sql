{{ config(materialized='table') }}

SELECT
    c.chance_id AS "Id",
    COALESCE(TRIM(c.bezeichnung), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(c.phase) IN ('prospecting', 'prospektierung') THEN 'Prospecting'
        WHEN LOWER(c.phase) IN ('qualification', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(c.phase) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(c.phase) IN ('value proposition', 'wertangebot') THEN 'Value Proposition'
        WHEN LOWER(c.phase) IN ('id. decision makers', 'entscheidungsträger identifiziert') THEN 'Id. Decision Makers'
        WHEN LOWER(c.phase) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(c.phase) IN ('proposal/price quote', 'angebot/preisangebot') THEN 'Proposal/Price Quote'
        WHEN LOWER(c.phase) IN ('negotiation/review', 'verhandlung/überprüfung') THEN 'Negotiation/Review'
        WHEN LOWER(c.phase) IN ('closed won', 'abgeschlossen gewonnen') THEN 'Closed Won'
        WHEN LOWER(c.phase) IN ('closed lost', 'abgeschlossen verloren') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for unknown or NULL phases to satisfy NOT NULL constraint
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN c.abschlussdatum::DATE -- YYYY-MM-DD
                WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(c.abschlussdatum, 'DD.MM.YYYY')
                WHEN c.abschlussdatum ~ '^\d{4}\d{2}\d{2}$' THEN TO_DATE(c.abschlussdatum, 'YYYYMMDD')
                WHEN c.abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(c.abschlussdatum, 'MM/DD/YYYY')
                ELSE NULL
            END,
            'YYYY-MM-DD'
        ),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default date if source is NULL or unparseable
    ) AS "CloseDate",
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    k.kunden_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON c.kd_nr = k.kunden_nr
