{{ config(materialized='table') }}

SELECT
    chancen.chance_id AS "Id",
    COALESCE(TRIM(chancen.bezeichnung), 'Unknown Opportunity Name') AS "Name",
    CASE UPPER(TRIM(chancen.phase))
        WHEN 'QUALIFIZIERUNG' THEN 'Qualification'
        WHEN 'ANGEBOT' THEN 'Proposal/Price Quote'
        WHEN 'GEWONNEN' THEN 'Closed Won'
        WHEN 'VERLOREN' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    CASE
        WHEN TRIM(chancen.abschlussdatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(chancen.abschlussdatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(chancen.abschlussdatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(chancen.abschlussdatum), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(chancen.abschlussdatum) ~ '^\d{2}\/\d{2}\/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(chancen.abschlussdatum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Fallback for NOT NULL CloseDate
    END AS "CloseDate",
    chancen.volumen AS "Amount",
    COALESCE(UPPER(TRIM(chancen.waehrung)), 'EUR') AS "CurrencyIsoCode",
    kunden.kunden_nr AS "AccountId",
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON TRIM(chancen.kd_nr) = TRIM(kunden.kunden_nr)