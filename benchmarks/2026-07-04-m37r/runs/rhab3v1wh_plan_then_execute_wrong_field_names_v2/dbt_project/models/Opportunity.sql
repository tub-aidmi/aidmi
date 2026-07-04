-- {{ config(materialized='table') }}

SELECT
    chance_id AS "Id",
    COALESCE(INITCAP(TRIM(bezeichnung)), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN TRIM(phase) = 'Prospecting' THEN 'Prospecting'
        WHEN TRIM(phase) = 'Qualification' THEN 'Qualification'
        WHEN TRIM(phase) = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN TRIM(phase) = 'Value Proposition' THEN 'Value Proposition'
        WHEN TRIM(phase) = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN TRIM(phase) = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN TRIM(phase) = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(phase) = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN TRIM(phase) = 'Closed Won' THEN 'Closed Won'
        WHEN TRIM(phase) = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NULL or unmapped values
    END AS "StageName",
    COALESCE(
        CASE
            WHEN abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN abschlussdatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Ensure type compatibility with previous CASE branches
    ) AS "CloseDate",
    volumen AS "Amount",
    COALESCE(UPPER(TRIM(waehrung)), 'EUR') AS "CurrencyIsoCode",
    kd_nr AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}