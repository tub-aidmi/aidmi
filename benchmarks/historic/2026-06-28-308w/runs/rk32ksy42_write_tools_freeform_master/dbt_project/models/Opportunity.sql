-- models/Opportunity.sql

{{ config(materialized='table') }}

WITH cleaned_opportunities AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        waehrungscode,
        kunden_ref,
        -- Clean auftragswert by removing currency symbols and non-numeric characters
        -- while preserving decimal/thousands separators for later parsing
        REGEXP_REPLACE(auftragswert, '[^0-9,.-]', '', 'g') AS cleaned_auftragswert
    FROM
        {{ source('fixture_master_src', 'master_opportunities') }}
),
transformed_opportunities AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        waehrungscode,
        kunden_ref,
        CASE
            WHEN cleaned_auftragswert IS NULL OR cleaned_auftragswert = '' THEN NULL
            -- European format with thousands separator (e.g., 1.234,56)
            WHEN cleaned_auftragswert LIKE '%.%,%' THEN REPLACE(REPLACE(cleaned_auftragswert, '.', ''), ',', '.')
            -- European format without thousands separator (e.g., 1234,56)
            WHEN cleaned_auftragswert LIKE '%,%' THEN REPLACE(cleaned_auftragswert, ',', '.')
            -- US format or just numbers (e.g., 1,234.56 or 1234.56)
            ELSE cleaned_auftragswert
        END AS final_numeric_string
    FROM
        cleaned_opportunities
)

SELECT
    opp_kennung AS "Id",
    COALESCE(titel, opp_kennung) AS "Name",
    CASE
        WHEN LOWER(vertriebsphase) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(vertriebsphase) IN ('qualification', 'qualifikation', 'quali', 'in prüfung') THEN 'Qualification'
        WHEN LOWER(vertriebsphase) IN ('won', 'closed won', 'abgeschlossen (gewonnen)', 'gewonnen', 'closed won') THEN 'Closed Won'
        WHEN LOWER(vertriebsphase) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    CASE
        WHEN zieldatum IN ('0000-00-00', 'N/A', '') THEN NULL
        ELSE COALESCE(
            TO_CHAR(CAST(zieldatum AS DATE), 'YYYY-MM-DD'), -- YYYY-MM-DD
            TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'), -- DD.MM.YYYY
            TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'), -- M/D/YYYY
            TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
        )
    END AS "CloseDate",
    CASE
        WHEN final_numeric_string ~ '^[+-]?\d+(\.\d+)?$' THEN final_numeric_string::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    waehrungscode AS "CurrencyIsoCode",
    kunden_ref AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    transformed_opportunities