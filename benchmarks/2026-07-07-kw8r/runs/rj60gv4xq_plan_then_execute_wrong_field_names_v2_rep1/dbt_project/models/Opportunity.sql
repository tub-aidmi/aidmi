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
        ELSE NULL
    END AS "StageName",
    COALESCE(
        TRY_CAST(TO_DATE(TRIM(c.abschlussdatum), 'DD.MM.YYYY') AS text),
        TRY_CAST(TO_DATE(TRIM(c.abschlussdatum), 'YYYY-MM-DD') AS text),
        TRY_CAST(TO_DATE(TRIM(c.abschlussdatum), 'MM/DD/YYYY') AS text),
        '2025-12-31'
    ) AS "CloseDate",
    CASE
        WHEN c.volumen IS NOT NULL AND TRIM(c.volumen) != '' THEN
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(TRIM(c.volumen), '^EUR\s*', '', 'i'),
                        '\.', ''
                    ),
                    ',', '.'
                )::DOUBLE PRECISION
            END
        ELSE NULL
    END AS "Amount",
    COALESCE(NULLIF(UPPER(TRIM(c.waehrung)), ''), 'EUR') AS "CurrencyIsoCode",
    a."Id" AS "AccountId",
    TRIM(c.chance_id) AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ ref('Account') }} a
    ON TRIM(c.kd_nr) = a."Legacy_Customer_ID__c"