{{ config(materialized='table') }}

SELECT
    TRIM(c.chance_id) AS "Id",
    INITCAP(TRIM(c.bezeichnung)) AS "Name",
    CASE LOWER(TRIM(c.phase))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'wertversprechen' THEN 'Value Proposition'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'entscheidungstraeger' THEN 'Id. Decision Makers'
        WHEN 'decision makers' THEN 'Id. Decision Makers'
        WHEN 'wahrnehmung' THEN 'Perception Analysis'
        WHEN 'perception' THEN 'Perception Analysis'
        WHEN 'angebot' THEN 'Proposal/Price Quote'
        WHEN 'preis' THEN 'Proposal/Price Quote'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'verhandlung' THEN 'Negotiation/Review'
        WHEN 'negotiation' THEN 'Negotiation/Review'
        WHEN 'abgeschlossen gewinnt' THEN 'Closed Won'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'abgeschlossen verliert' THEN 'Closed Lost'
        WHEN 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN c.abschlussdatum IS NOT NULL AND c.abschlussdatum != ''
            THEN CASE
                WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$'
                    THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
                WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$'
                    THEN TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
                ELSE NULL
            END
        ELSE NULL
    END AS "CloseDate",
    c.volumen AS "Amount",
    COALESCE(NULLIF(UPPER(TRIM(c.waehrung)), ''), 'EUR') AS "CurrencyIsoCode",
    TRIM(k.kunden_nr) AS "AccountId",
    TRIM(c.chance_id) AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(c.kd_nr) = TRIM(k.kunden_nr)