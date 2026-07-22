{{ config(materialized='table') }}

SELECT
    -- Opportunity Salesforce Id (006 prefix + trimmed source chance_id)
    '006' || TRIM(c.chance_id) AS "Id",

    -- Opportunity Name from bezeichnung
    COALESCE(TRIM(c.bezeichnung), '') AS "Name",

    -- StageName: map German/English phase values to Salesforce sales pipeline stages
    CASE LOWER(TRIM(COALESCE(c.phase, '')))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'lead' THEN 'Prospecting'
        WHEN 'lead generierung' THEN 'Prospecting'
        WHEN 'qualifizierung' THEN 'Qualification'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'wertversprechen' THEN 'Value Proposition'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'entscheidungsträger identifizieren' THEN 'Id. Decision Makers'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'angebot/preisangebot' THEN 'Proposal/Price Quote'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'verhandlung/überprüfung' THEN 'Negotiation/Review'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'geschlossen gewonnen' THEN 'Closed Won'
        WHEN 'verloren' THEN 'Closed Lost'
        WHEN 'closed lost' THEN 'Closed Lost'
        WHEN 'geschlossen verloren' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",

    -- CloseDate: parse DD.MM.YYYY or YYYY-MM-DD formats, output ISO YYYY-MM-DD
    CASE
        WHEN c.abschlussdatum IS NULL OR TRIM(c.abschlussdatum) = '' THEN NULL
        WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
            TO_CHAR(TO_DATE(c.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN LEFT(c.abschlussdatum, 10)
        ELSE NULL
    END AS "CloseDate",

    -- Amount: source is already double precision (volumen)
    CASE
        WHEN c.volumen IS NOT NULL AND ABS(c.volumen) > 0 THEN CAST(c.volumen AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",

    -- CurrencyIsoCode: trimmed currency code from source
    TRIM(COALESCE(c.waehrung, '')) AS "CurrencyIsoCode",

    -- AccountId: construct Salesforce Account Id (001 prefix + source customer number) by joining to kunden
    '001' || TRIM(k.kunden_nr) AS "AccountId",

    -- Legacy_Opportunity_ID__c: raw source chance_id for row-level verification
    TRIM(c.chance_id) AS "Legacy_Opportunity_ID__c",

    -- Audit fields (derived from load time; no source dates available)
    CAST(NOW() AS TEXT) AS "CreatedDate",
    CAST(NOW() AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(c.kd_nr) = TRIM(k.kunden_nr)