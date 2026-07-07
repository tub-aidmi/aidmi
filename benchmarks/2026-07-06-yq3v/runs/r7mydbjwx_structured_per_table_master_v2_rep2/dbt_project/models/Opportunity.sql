-- depends_on: {{ ref('Account') }}

{{ config(materialized='table') }}

WITH 

cleaned_opportunities AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref,
        -- Clean and parse auftragswert (Amount)
        CASE
            WHEN auftragswert IS NULL THEN NULL
            WHEN auftragswert ~ '^(EUR|USD|GBP)\s*['-]?\s*['-]?\d+\.?\d*' THEN -- Starts with currency symbol and digits
                CAST(
                    REPLACE(
                        REPLACE(TRIM(REGEXP_REPLACE(auftragswert, '^(EUR|USD|GBP)\s*['-]?\s*', '', 'i')),
                                '.', ''), -- Remove thousand separators
                        ',', '.' -- Replace decimal comma with dot
                    ) AS DOUBLE PRECISION
                )
            WHEN auftragswert ~ '^[\-]?\d+\.\d{3},\d+$' THEN -- European format (e.g., 123.456,78)
                CAST(
                    REPLACE(
                        REPLACE(auftragswert, '.', ''), -- Remove thousand separators
                        ',', '.' -- Replace decimal comma with dot
                    ) AS DOUBLE PRECISION
                )
            WHEN auftragswert ~ '^[\-]?\d+,\d+$' THEN -- European format (e.g., 123,78 - no thousands dot)
                CAST(
                    REPLACE(auftragswert, ',', '.') AS DOUBLE PRECISION
                )
            WHEN auftragswert ~ '^[\-]?\d+\.?\d*$' THEN -- Standard format (e.g., 1234.56 or 1234)
                CAST(auftragswert AS DOUBLE PRECISION)
            ELSE NULL
        END AS parsed_amount,
        -- Parse zieldatum (CloseDate) robustly with regex guards
        COALESCE(
            CASE
                WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
                WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
                WHEN zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
                WHEN zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
                ELSE NULL
            END,
            '1900-01-01' -- Fallback for NOT NULL constraint
        ) AS parsed_closedate
    FROM
        {{ source('fixture_master_v2_src', 'master_opportunities') }}
)

SELECT
    opp_kennung AS "Id",
    COALESCE(titel, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN vertriebsphase ILIKE '%Prospecting%' THEN 'Prospecting'
        WHEN vertriebsphase ILIKE '%Qualification%' THEN 'Qualification'
        WHEN vertriebsphase ILIKE '%Needs Analysis%' THEN 'Needs Analysis'
        WHEN vertriebsphase ILIKE '%Value Proposition%' THEN 'Value Proposition'
        WHEN vertriebsphase ILIKE '%Id. Decision Makers%' THEN 'Id. Decision Makers'
        WHEN vertriebsphase ILIKE '%Perception Analysis%' THEN 'Perception Analysis'
        WHEN vertriebsphase ILIKE '%Proposal/Price Quote%' THEN 'Proposal/Price Quote'
        WHEN vertriebsphase ILIKE '%Negotiation/Review%' THEN 'Negotiation/Review'
        WHEN vertriebsphase ILIKE '%Closed Won%' THEN 'Closed Won'
        WHEN vertriebsphase ILIKE '%Closed Lost%' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to 'Prospecting' for NOT NULL constraint
    END AS "StageName",
    parsed_closedate AS "CloseDate",
    parsed_amount AS "Amount",
    waehrungscode AS "CurrencyIsoCode",
    kunden_ref AS "AccountId", -- kunden_ref maps to Account Id (source natural key)
    opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_opportunities