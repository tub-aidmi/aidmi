{{ config(materialized='table') }}

SELECT
    opp_kennung AS "Id",
    COALESCE(titel, 'Opportunity - ' || LEFT(opp_kennung, 8)) AS "Name",
    CASE
        WHEN LOWER(TRIM(vertriebsphase)) IN ('prospecting', 'prospect') THEN 'Prospecting'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM(vertriebsphase)) = 'in kontakt' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(vertriebsphase)) = 'in prüfung' THEN 'Value Proposition'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('gewonnen', 'won', 'closed won', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('verloren', 'lost', 'closed lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        -- ISO format: YYYY-MM-DD
        WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' AND zieldatum != '0000-00-00'
            THEN TO_CHAR(TO_DATE(zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        -- European dot format: DD.MM.YYYY
        WHEN zieldatum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$'
            THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        -- US slash format: M/D/YYYY or MM/DD/YYYY
        WHEN zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        -- Compact numeric: YYYYMMDD
        WHEN zieldatum ~ '^\d{8}$' AND zieldatum != '00000000'
            THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN auftragswert = 'None' OR auftragswert IS NULL OR auftragswert = ''
            THEN NULL::DOUBLE PRECISION
        WHEN auftragswert ~ '^\s*EUR\s+'
            THEN REGEXP_REPLACE(REGEXP_REPLACE(TRIM(SUBSTRING(auftragswert FROM 5)), '\.', ''), ',', '.')::DOUBLE PRECISION
        WHEN auftragswert ~ '^\s*-?EUR\s+'
            THEN REGEXP_REPLACE(REGEXP_REPLACE(TRIM(SUBSTRING(auftragswert FROM 5)), '\.', ''), ',', '.')::DOUBLE PRECISION * -1
        WHEN auftragswert ~ '^-?\d{1,3}(\.\d{3})+,\d+$'
            -- European format: thousands dot, decimal comma (e.g., "316.863,04")
            THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(auftragswert, '\.', ''), ',', '.'), '^-', '')::DOUBLE PRECISION * CASE WHEN auftragswert ~ '^-' THEN -1 ELSE 1 END
        WHEN auftragswert ~ '^-?\d+,\d+$'
            -- Decimal comma format (e.g., "1.234,56" already handled above for dotted thousands; plain comma decimal)
            THEN REGEXP_REPLACE(REGEXP_REPLACE(auftragswert, '\.', ''), ',', '.')::DOUBLE PRECISION * CASE WHEN auftragswert ~ '^-' THEN -1 ELSE 1 END
        WHEN auftragswert ~ '^-?\d+\.\d+$'
            -- Standard decimal (e.g., "253569.24")
            THEN auftragswert::DOUBLE PRECISION
        WHEN auftragswert ~ '^-?\d+$'
            -- Integer (e.g., "0", "-440691.0")
            THEN auftragswert::DOUBLE PRECISION
        ELSE NULL::DOUBLE PRECISION
    END AS "Amount",
    CASE
        WHEN TRIM(LOWER(waehrungscode)) IN ('eur', '€') THEN 'EUR'
        WHEN TRIM(waehrungscode) = 'USD' OR TRIM(LOWER(waehrungscode)) = 'dollar' THEN 'USD'
        WHEN TRIM(waehrungscode) = 'CHF' THEN 'CHF'
        WHEN TRIM(waehrungscode) = 'GBP' THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    kunden_ref AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_opportunities') }}