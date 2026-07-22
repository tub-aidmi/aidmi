{{ config(materialized='table') }}

SELECT
    -- Id: natural key, normalized to trimmed source ID
    TRIM(c.chance_id) AS "Id",
    -- Name: opportunity description
    INITCAP(TRIM(c.bezeichnung)) AS "Name",
    -- StageName: phase mapped to English SFDC stages (source already in English enum values)
    CASE LOWER(TRIM(c.phase))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'bedarfsanalyse' OR 'needs analysis' THEN 'Needs Analysis'
        WHEN 'wertversprechen' OR 'value proposition' THEN 'Value Proposition'
        WHEN 'entscheidungstraeger' OR 'decision makers' THEN 'Id. Decision Makers'
        WHEN 'wahrnehmung' OR 'perception' THEN 'Perception Analysis'
        WHEN 'angebot' OR 'preis' OR 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'verhandlung' OR 'negotiation' THEN 'Negotiation/Review'
        WHEN 'abgeschlossen gewinnt' OR 'closed won' THEN 'Closed Won'
        WHEN 'abgeschlossen verliert' OR 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    -- CloseDate: parsed from ISO YYYY-MM-DD format (primary) or DD.MM.YYYY fallback
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
    -- Amount: already double precision, passthrough with NULL safety
    CAST(c.volumen AS DOUBLE PRECISION) AS "Amount",
    -- CurrencyIsoCode: uppercased ISO 4217 code, default to EUR if NULL/empty
    COALESCE(NULLIF(UPPER(TRIM(c.waehrung)), ''), 'EUR') AS "CurrencyIsoCode",
    -- AccountId: normalized customer reference from chancen.kd_nr (matches kunden.kunden_nr format)
    TRIM(k.kunden_nr) AS "AccountId",
    -- Legacy_Opportunity_ID__c: original source natural key for traceability
    TRIM(c.chance_id) AS "Legacy_Opportunity_ID__c",
    -- CreatedDate: no source equivalent
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    -- LastModifiedDate: no source equivalent
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    -- IsDeleted: no source equivalent, default 0
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(c.kd_nr) = TRIM(k.kunden_nr)