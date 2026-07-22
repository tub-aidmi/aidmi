
{{ config(materialized='table') }}

SELECT
    TRIM(c.chance_id) AS "Id",
    COALESCE(TRIM(c.bezeichnung), 'Untitled Opportunity') AS "Name",
    CASE TRIM(c.phase)
        WHEN 'Prospecting' THEN 'Prospecting'
        WHEN 'Qualification' THEN 'Qualification'
        WHEN 'Closed Won' THEN 'Closed Won'
        WHEN 'Closed Lost' THEN 'Closed Lost'
        -- Add other mappings if they exist in source but not in sample
        ELSE 'Prospecting' -- Fallback for NOT NULL StageName
    END AS "StageName",
    COALESCE(TO_CHAR(CAST(TRIM(c.abschlussdatum) AS DATE), 'YYYY-MM-DD'), '1900-01-01') AS "CloseDate",
    c.volumen AS "Amount",
    CASE TRIM(c.waehrung)
        WHEN 'EUR' THEN 'EUR'
        ELSE NULL -- Fallback for CurrencyIsoCode if no match
    END AS "CurrencyIsoCode",
    TRIM(c.kd_nr) AS "AccountId",
    TRIM(c.chance_id) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_src', 'chancen') }} AS c
LEFT JOIN
    {{ source('fixture_wrong_field_names_src', 'kunden') }} AS k
ON
    TRIM(c.kd_nr) = TRIM(k.kunden_nr)
