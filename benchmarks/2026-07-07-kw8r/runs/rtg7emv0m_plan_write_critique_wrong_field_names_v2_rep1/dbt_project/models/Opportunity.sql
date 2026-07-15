{{ config(materialized='table') }}

SELECT
    CAST(chance_id AS TEXT) AS "Id",
    COALESCE(INITCAP(TRIM(bezeichnung)), 'Unnamed Opportunity') AS "Name",
    CASE LOWER(TRIM(phase))
        WHEN 'akquise' THEN 'Prospecting'
        WHEN 'qualifizierung' THEN 'Qualification'
        WHEN 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN 'wertversprechen' THEN 'Value Proposition'
        WHEN 'identifikation der entscheidungsträger' THEN 'Id. Decision Makers'
        WHEN 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN 'angebot/preisangebot' THEN 'Proposal/Price Quote'
        WHEN 'verhandlung/überprüfung' THEN 'Negotiation/Review'
        WHEN 'abschluss gewonnen' THEN 'Closed Won'
        WHEN 'abschluss verloren' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        CASE TRIM(abschlussdatum)
            WHEN '' THEN NULL
            ELSE CASE 
                WHEN abschlussdatum ~ '^\d{8}$' THEN TO_DATE(TRIM(abschlussdatum), 'YYYYMMDD')::TEXT
                WHEN abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(abschlussdatum), 'DD.MM.YYYY')::TEXT
                ELSE NULL
            END
        END, 
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "CloseDate",
    volumen AS "Amount",
    CASE UPPER(TRIM(waehrung))
        WHEN 'EUR' THEN 'EUR'
        WHEN 'EURO' THEN 'EUR'
        WHEN 'USD' THEN 'USD'
        WHEN 'DOLLAR' THEN 'USD'
        WHEN 'GBP' THEN 'GBP'
        WHEN 'Pound' THEN 'GBP'
        WHEN 'CHF' THEN 'CHF'
        WHEN 'FRANKEN' THEN 'CHF'
        ELSE NULL
    END AS "CurrencyIsoCode",
    TRIM(UPPER(kd_nr)) AS "AccountId",
    CAST(chance_id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}