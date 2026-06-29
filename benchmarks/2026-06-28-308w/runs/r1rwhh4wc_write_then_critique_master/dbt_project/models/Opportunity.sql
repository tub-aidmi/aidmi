
{{ config(materialized='table') }}

WITH opportunities_raw AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref
    FROM {{ source('fixture_master_src', 'master_opportunities') }}
)
SELECT
    -- Id: Primary key for the Opportunity object, directly mapped from the source opportunity identifier.
    ops.opp_kennung AS "Id",

    -- Name: The title of the opportunity, trimmed and defaulting to 'Unknown Opportunity' if the source is NULL.
    COALESCE(TRIM(ops.titel), 'Unknown Opportunity') AS "Name",

    -- StageName: Categorized from the source 'vertriebsphase', translating German values to the target enum, with 'Prospecting' as a default.
    CASE
        WHEN LOWER(TRIM(ops.vertriebsphase)) IN ('won', 'closed won', 'abgeschlossen (gewonnen)', 'gewonnen', 'closed won') THEN 'Closed Won'
        WHEN LOWER(TRIM(ops.vertriebsphase)) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)', 'lost') THEN 'Closed Lost'
        WHEN LOWER(TRIM(ops.vertriebsphase)) IN ('qualifikation', 'quali', 'qualification') THEN 'Qualification'
        WHEN LOWER(TRIM(ops.vertriebsphase)) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(ops.vertriebsphase)) = 'in prüfung' THEN 'Negotiation/Review'
        ELSE 'Prospecting'
    END AS "StageName",

    -- CloseDate: Parsed from 'zieldatum', handling multiple date formats with basic component validation before conversion to prevent TO_DATE errors, and defaulting to '1900-01-01' if unparseable or NULL, to satisfy NOT NULL constraint.
    COALESCE(
        CASE
            -- YYYY-MM-DD
            WHEN ops.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN
                CASE
                    WHEN CAST(SUBSTRING(ops.zieldatum, 6, 2) AS INTEGER) BETWEEN 1 AND 12
                    AND CAST(SUBSTRING(ops.zieldatum, 9, 2) AS INTEGER) BETWEEN 1 AND 31
                    THEN ops.zieldatum
                    ELSE NULL
                END
            -- MM/DD/YYYY
            WHEN ops.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
                CASE
                    WHEN CAST(SUBSTRING(ops.zieldatum FROM '\d{1,2}/(\d{1,2})/\d{4}') AS INTEGER) BETWEEN 1 AND 12
                    AND CAST(SUBSTRING(ops.zieldatum FROM '(\d{1,2})/\d{1,2}/\d{4}') AS INTEGER) BETWEEN 1 AND 31
                    THEN TO_CHAR(TO_DATE(ops.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
                    ELSE NULL
                END
            -- DD.MM.YYYY
            WHEN ops.zieldatum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN
                CASE
                    WHEN CAST(SUBSTRING(ops.zieldatum FROM '\d{1,2}\.(\d{1,2})\.\d{4}') AS INTEGER) BETWEEN 1 AND 12
                    AND CAST(SUBSTRING(ops.zieldatum FROM '^(\d{1,2})\.\d{1,2}\.\d{4}') AS INTEGER) BETWEEN 1 AND 31
                    THEN TO_CHAR(TO_DATE(ops.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
                    ELSE NULL
                END
            -- YYYYMMDD
            WHEN ops.zieldatum ~ '^\d{8}$' THEN
                CASE
                    WHEN CAST(SUBSTRING(ops.zieldatum, 5, 2) AS INTEGER) BETWEEN 1 AND 12
                    AND CAST(SUBSTRING(ops.zieldatum, 7, 2) AS INTEGER) BETWEEN 1 AND 31
                    THEN TO_CHAR(TO_DATE(ops.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
                    ELSE NULL
                END
            ELSE NULL
        END,
        '1900-01-01'
    ) AS "CloseDate",

    -- Amount: Transformed from 'auftragswert', cleaning 'EUR' prefix, handling European vs. standard decimal/thousands separators, and casting to DOUBLE PRECISION. NULL if source is invalid or empty.
    CASE
        WHEN ops.auftragswert IS NULL OR TRIM(ops.auftragswert) = '' THEN NULL
        ELSE
            (
                CASE
                    WHEN TRIM(REPLACE(LOWER(ops.auftragswert), 'eur', '')) ~ '^\s*\d{1,3}(\.\d{3})*,\d+\s*$' THEN -- European format (e.g. 1.234,56 or 123,45)
                        CAST(REPLACE(REPLACE(TRIM(REPLACE(LOWER(ops.auftragswert), 'eur', '')), '.', ''), ',', '.') AS DOUBLE PRECISION)
                    WHEN TRIM(REPLACE(LOWER(ops.auftragswert), 'eur', '')) ~ '^\s*-?\d+\.?\d*\s*$' THEN -- Standard format (e.g. 123456.78 or 123456)
                        CAST(REPLACE(TRIM(REPLACE(LOWER(ops.auftragswert), 'eur', '')), ',', '.') AS DOUBLE PRECISION)
                    ELSE NULL
                END
            )
    END AS "Amount",

    -- CurrencyIsoCode: Directly mapped from the source currency code.
    ops.waehrungscode AS "CurrencyIsoCode",

    -- AccountId: The customer reference from the source, transformed to match the Account.Id format (e.g., changing 'KD-' prefix to 'CUST-').
    CASE WHEN ops.kunden_ref ~ '^KD-.+$' THEN REPLACE(ops.kunden_ref, 'KD-', 'CUST-') ELSE ops.kunden_ref END AS "AccountId",

    -- Legacy_Opportunity_ID__c: The original identifier from the source system, also used as the primary Id.
    ops.opp_kennung AS "Legacy_Opportunity_ID__c",

    -- CreatedDate: Not available in the source system for this entity, defaulted to NULL.
    NULL AS "CreatedDate",

    -- LastModifiedDate: Not available in the source system for this entity, defaulted to NULL.
    NULL AS "LastModifiedDate",

    -- IsDeleted: Defaulted to 0 as there is no corresponding soft-delete indicator in the source.
    0 AS "IsDeleted"

FROM
    opportunities_raw ops