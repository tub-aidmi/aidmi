{{ config(materialized='table') }}

SELECT
    TRIM(c.chance_id) AS "Id",
    COALESCE(NULLIF(TRIM(c.bezeichnung), ''), 'Unnamed Opportunity') AS "Name",
    CASE LOWER(TRIM(c.phase))
        WHEN 'offene chance' THEN 'Prospecting'
        WHEN 'qualifizierung' THEN 'Qualification'
        WHEN 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN 'wertproposition' THEN 'Value Proposition'
        WHEN 'entscheidungsträger identifizieren' THEN 'Id. Decision Makers'
        WHEN 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN 'angebot/preisangebot' THEN 'Proposal/Price Quote'
        WHEN 'verhandlung/überprüfung' THEN 'Negotiation/Review'
        WHEN 'geschlossen - gewonnen' THEN 'Closed Won'
        WHEN 'geschlossen - verloren' THEN 'Closed Lost'
    END AS "StageName",
    CASE 
        WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(c.abschlussdatum), 'DD.MM.YYYY')::TEXT
        WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(c.abschlussdatum), 'YYYY-MM-DD')::TEXT
        WHEN c.abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(c.abschlussdatum), 'MM/DD/YYYY')::TEXT
    END AS "CloseDate",
    CAST(c.volumen AS DOUBLE PRECISION) AS "Amount",
    COALESCE(NULLIF(TRIM(UPPER(c.waehrung)), ''), 'EUR') AS "CurrencyIsoCode",
    'A' || TRIM(k.kunden_nr) AS "AccountId",
    TRIM(c.chance_id) AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(c.kd_nr) = TRIM(k.kunden_nr)