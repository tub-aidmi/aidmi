-- normalize opportunity data

{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE("Name", 'Unnamed Opportunity') AS "Name",
    COALESCE(
        CASE
            WHEN LOWER(TRIM("StageName")) IN ('prospect', 'prospecting') THEN 'Prospecting'
            WHEN LOWER(TRIM("StageName")) IN ('quali', 'qualification', 'qualifikation', 'in kontakt') THEN 'Qualification'
            WHEN LOWER(TRIM("StageName")) IN ('lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
            WHEN LOWER(TRIM("StageName")) IN ('gewonnen', 'closed won', 'abgeschlossen (gewonnen)', 'won') THEN 'Closed Won'
            WHEN TRIM("StageName") = 'In Prüfung' THEN 'Negotiation/Review'
            WHEN TRIM("StageName") = 'Needs Analysis' THEN 'Needs Analysis'
            WHEN TRIM("StageName") = 'Value Proposition' THEN 'Value Proposition'
            WHEN TRIM("StageName") = 'Id. Decision Makers' THEN 'Id. Decision Makers'
            WHEN TRIM("StageName") = 'Perception Analysis' THEN 'Perception Analysis'
            WHEN TRIM("StageName") = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
            WHEN TRIM("StageName") = 'Negotiation/Review' THEN 'Negotiation/Review'
            ELSE NULL
        END,
        'Prospecting' -- Default for unmapped values since target is NOT NULL
    ) AS "StageName",
    COALESCE(
        (CASE
            WHEN TRIM("CloseDate") IN ('0000-00-00', 'N/A') THEN NULL -- Explicitly handle known invalid date strings
            WHEN "CloseDate" ~ '^\d{4}-\d{2}-\d{2}$' THEN "CloseDate" -- YYYY-MM-DD
            WHEN "CloseDate" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE("CloseDate", 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN "CloseDate" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE("CloseDate", 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN "CloseDate" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE("CloseDate", 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END),
        '1900-01-01' -- Default for unparseable or missing dates, as CloseDate is NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN "Amount" IS NULL OR TRIM("Amount") IN ('None', '') THEN NULL
        ELSE
            -- Clean the string first (remove 'EUR ', trim)
            CASE
                -- Check if it looks like a European format (e.g., 1.234,56 -> 1234,56 after removing dots)
                WHEN REGEXP_REPLACE(TRIM(REPLACE("Amount", 'EUR ', '')), '[.]', '', 'g') ~ '^-?\d+,\d+$'
                THEN CAST(REPLACE(REGEXP_REPLACE(TRIM(REPLACE("Amount", 'EUR ', '')), '[.]', '', 'g'), ',', '.') AS DOUBLE PRECISION)
                -- Otherwise, assume US format or standard number
                ELSE CAST(TRIM(REPLACE("Amount", 'EUR ', '')) AS DOUBLE PRECISION)
            END
    END AS "Amount",
    "CurrencyIsoCode" AS "CurrencyIsoCode",
    "AccountId" AS "AccountId",
    NULL::text AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    NULL::integer AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Opportunity') }}