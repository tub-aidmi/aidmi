{{ config(materialized='table') }}

SELECT
    -- Id: source natural key (direct copy)
    CAST(TRIM(c.chance_id) AS TEXT) AS "Id",

    -- Name: INITCAP of bezeichnung, fallback 'Unnamed Opportunity'
    COALESCE(INITCAP(TRIM(c.bezeichnung)), 'Unnamed Opportunity') AS "Name",

    -- StageName: map phase values to target enum domain
    CASE UPPER(TRIM(c.phase))
        WHEN 'AKQUISITION' THEN 'Prospecting'
        WHEN 'PROSPECTING' THEN 'Prospecting'
        WHEN 'QUALIFIZIERUNG' THEN 'Qualification'
        WHEN 'QUALIFICATION' THEN 'Qualification'
        WHEN 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN 'WERTPROPOSITION' THEN 'Value Proposition'
        WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN 'ENTSCHEIDER IDENTIFIZIEREN' THEN 'Id. Decision Makers'
        WHEN 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN 'ANGEBOT/PRIS' THEN 'Proposal/Price Quote'
        WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN 'VERHANDLUNG/REVIEW' THEN 'Negotiation/Review'
        WHEN 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN 'GEWONNEN' THEN 'Closed Won'
        WHEN 'ERFOLGREICH' THEN 'Closed Won'
        WHEN 'CLOSED WON' THEN 'Closed Won'
        WHEN 'VERLOREN' THEN 'Closed Lost'
        WHEN 'GESCHEITERT' THEN 'Closed Lost'
        WHEN 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",

    -- CloseDate: parse ISO (YYYY-MM-DD) or German (DD.MM.YYYY) formats; NULL for unparseable
    CASE
        WHEN TRIM(c.abschlussdatum) IS NULL OR TRIM(c.abschlussdatum) = '' THEN NULL
        WHEN TRIM(c.abschlussdatum) ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TO_DATE(TRIM(c.abschlussdatum), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(c.abschlussdatum) ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_DATE(TRIM(c.abschlussdatum), 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",

    -- Amount: source column is already double precision; safe cast
    CAST(c.volumen AS DOUBLE PRECISION) AS "Amount",

    -- CurrencyIsoCode: normalise to upper, fallback EUR when missing
    COALESCE(NULLIF(UPPER(TRIM(c.waehrung)), ''), 'EUR') AS "CurrencyIsoCode",

    -- AccountId: transform source customer number with same rule as Account.Id (strip prefix, prepend ACC-)
    CASE
        WHEN TRIM(k.kunden_nr) IS NOT NULL AND TRIM(k.kunden_nr) != ''
            THEN 'ACC-' || REGEXP_REPLACE(TRIM(k.kunden_nr), '.*([0-9]+)', '$1')
        ELSE NULL
    END AS "AccountId",

    -- Legacy_Opportunity_ID__c: exact source natural key
    TRIM(c.chance_id) AS "Legacy_Opportunity_ID__c",

    -- Audit columns
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c

LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(c.kd_nr) = TRIM(k.kunden_nr)