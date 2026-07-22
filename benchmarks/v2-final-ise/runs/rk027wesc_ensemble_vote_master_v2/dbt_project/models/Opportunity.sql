{{ config(materialized='table') }}

SELECT
    opp_kennung AS "Id",
    titel AS "Name",

    -- Map vertriebsphase to Salesforce opportunity stages
    CASE LOWER(TRIM(vertriebsphase))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'prospect' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'quali' THEN 'Qualification'
        WHEN 'qualifikation' THEN 'Qualification'
        WHEN 'in kontakt' THEN 'Needs Analysis'
        WHEN 'in prüfung' THEN 'Value Proposition'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'abgeschlossen (gewonnen)' THEN 'Closed Won'
        WHEN 'won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        WHEN 'verloren' THEN 'Closed Lost'
        WHEN 'lost' THEN 'Closed Lost'
        WHEN 'abgeschlossen (verloren)' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",

    -- Parse multiple date formats into YYYY-MM-DD
    CASE
        WHEN zieldatum IS NULL OR TRIM(zieldatum) = '' THEN NULL
        WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(zieldatum, 'YYYY-MM-DD')::TEXT
        WHEN zieldatum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(zieldatum, 'DD.MM.YYYY')::TEXT
        WHEN zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(zieldatum, 'MM/DD/YYYY')::TEXT
        WHEN zieldatum ~ '^\d{8}$' THEN TO_DATE(zieldatum, 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "CloseDate",

    -- Parse amount: strip currency symbols/prefixes, handle European format
    CASE
        WHEN TRIM(auftragswert) IS NULL OR TRIM(auftragswert) = '' THEN NULL::DOUBLE PRECISION
        WHEN UPPER(TRIM(auftragswert)) = 'NONE' THEN NULL::DOUBLE PRECISION
        ELSE
            CAST(
                CASE
                    -- European format: digits with dots as thousands and comma as decimal (e.g., "400.902,63" or "-193824,73")
                    WHEN TRIM(auftragswert) ~ '^-?\d{1,3}(\.\d{3})+,\d+$' THEN
                        CAST(
                            REGEXP_REPLACE(
                                REPLACE(
                                    REGEXP_REPLACE(TRIM(auftragswert), '^[A-Za-z€$£]+', ''),
                                    '.', ''
                                ),
                                ',', '.'
                            ) AS DOUBLE PRECISION
                        )
                    -- Standard format with currency prefix/symbol: strip prefix then cast
                    ELSE
                        CAST(
                            REGEXP_REPLACE(
                                TRIM(auftragswert), '^[A-Za-z€$£]+', ''
                            ) AS DOUBLE PRECISION
                        )
                END AS DOUBLE PRECISION
            )
    END AS "Amount",

    -- Standardize currency to 3-letter ISO codes
    CASE UPPER(TRIM(waehrungscode))
        WHEN 'USD' THEN 'USD'
        WHEN 'EUR' THEN 'EUR'
        WHEN 'GBP' THEN 'GBP'
        WHEN 'CHF' THEN 'CHF'
        WHEN '$' THEN 'USD'
        WHEN 'DOLLAR' THEN 'USD'
        WHEN 'EURO' THEN 'EUR'
        WHEN '€' THEN 'EUR'
        WHEN '£' THEN 'GBP'
        ELSE UPPER(TRIM(waehrungscode))
    END AS "CurrencyIsoCode",

    -- Map kunden_ref KD-Mxxx to AccountId ACC-Mxxx format
    -- Source uses KD- prefix, target Accounts use ACC- prefix with same number part
    'ACC-' || RIGHT(kunden_ref, LENGTH(kunden_ref) - 3) AS "AccountId",

    opp_kennung AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}