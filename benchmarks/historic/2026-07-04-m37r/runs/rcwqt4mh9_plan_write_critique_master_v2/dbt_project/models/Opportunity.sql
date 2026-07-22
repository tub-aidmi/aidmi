-- dbt model for Opportunity

{{ config(materialized='table') }}

WITH source_opportunities AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref,
        -- Clean the amount string once for easier parsing logic
        NULLIF(TRIM(REGEXP_REPLACE(LOWER(auftragswert), '^(eur|usd|chf|gbp|\$|€|£)\s*', '', 'g')), '') AS cleaned_amount_str
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
)

SELECT
    opp_kennung AS "Id",
    COALESCE(titel, 'Unnamed Opportunity') AS "Name",
    COALESCE(
        CASE
            WHEN LOWER(vertriebsphase) IN ('in kontakt', 'prospecting', 'prospect') THEN 'Prospecting'
            WHEN LOWER(vertriebsphase) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
            WHEN LOWER(vertriebsphase) = 'in prüfung' THEN 'Negotiation/Review'
            WHEN LOWER(vertriebsphase) IN ('closed won', 'abgeschlossen (gewonnen)', 'won', 'gewonnen') THEN 'Closed Won'
            WHEN LOWER(vertriebsphase) IN ('lost', 'abgeschlossen (verloren)', 'verloren') THEN 'Closed Lost'
            ELSE NULL
        END,
        'Prospecting'
    ) AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD'),
        '1900-01-01'
    ) AS "CloseDate",
    CASE
        WHEN cleaned_amount_str IS NULL THEN NULL
        -- If the string contains a comma, assume European format: remove dots, then replace comma with dot.
        WHEN cleaned_amount_str LIKE '%,%' THEN
            -- Validate the resulting number pattern before casting
            CASE
                WHEN REPLACE(REPLACE(cleaned_amount_str, '.', ''), ',', '.') ~ '^-?\d+(\.\d+)?$' THEN
                    CAST(REPLACE(REPLACE(cleaned_amount_str, '.', ''), ',', '.') AS DOUBLE PRECISION)
                ELSE NULL
            END
        -- If no comma, assume US/standard format: just remove any thousand commas (if any exist) and cast.
        -- And check if it matches a numeric pattern (with or without a dot for decimal).
        WHEN cleaned_amount_str ~ '^-?\d+(,\d+)*(\.\d+)?$' THEN
            CASE
                WHEN REPLACE(cleaned_amount_str, ',', '') ~ '^-?\d+(\.\d+)?$' THEN
                    CAST(REPLACE(cleaned_amount_str, ',', '') AS DOUBLE PRECISION)
                ELSE NULL
            END
        ELSE NULL
    END AS "Amount",
    CASE
        WHEN LOWER(waehrungscode) IN ('chf') THEN 'CHF'
        WHEN LOWER(waehrungscode) IN ('eur', 'euro', '€') THEN 'EUR'
        WHEN LOWER(waehrungscode) IN ('usd', '$', 'dollar') THEN 'USD'
        WHEN LOWER(waehrungscode) IN ('gbp', '£') THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    MD5(kunden_ref) AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_opportunities
