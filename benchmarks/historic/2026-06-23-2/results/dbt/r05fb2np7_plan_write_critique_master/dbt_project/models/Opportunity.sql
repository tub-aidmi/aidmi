{{ config(materialized='table') }}

SELECT
    opp_kennung AS Id,
    COALESCE(INITCAP(TRIM(titel)), 'UNKNOWN OPPORTUNITY') AS Name,

    INITCAP(TRIM(
        CASE
            WHEN UPPER(TRIM(vertriebsphase)) IN ('ABGESCHLOSSEN (GEWONNEN)', 'GEWONNEN', 'WON', 'CLOSED WON', 'CLOSED-WON') THEN 'Closed Won'
            WHEN UPPER(TRIM(vertriebsphase)) IN ('ABGESCHLOSSEN (VERLOREN)', 'VERLOREN', 'LOST', 'CLOSED LOST', 'CLOSED-LOST') THEN 'Closed Lost'
            WHEN UPPER(TRIM(vertriebsphase)) IN ('PROSPECTING', 'PROSPECT') THEN 'Prospecting'
            WHEN UPPER(TRIM(vertriebsphase)) IN ('QUALIFICATION', 'QUALI', 'QUALIFIKATION') THEN 'Qualification'
            WHEN UPPER(TRIM(vertriebsphase)) IN ('IN KONTAKT') THEN 'Needs Analysis'
            WHEN UPPER(TRIM(vertriebsphase)) IN ('IN PRÜFUNG') THEN 'Value Proposition'
            ELSE INITCAP(TRIM(vertriebsphase))
        END
    )) AS StageName,

    CASE
        WHEN zieldatum IS NULL OR TRIM(zieldatum) = '' THEN NULL
        WHEN UPPER(TRIM(zieldatum)) = 'N/A' THEN NULL
        WHEN TRIM(zieldatum) = '0000-00-00' THEN NULL
        WHEN TRIM(zieldatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(TRIM(zieldatum) AS DATE)::TEXT
        WHEN TRIM(zieldatum) ~ '^\d{8}$' 
            THEN SUBSTR(TRIM(zieldatum), 1, 4) || '-' || SUBSTR(TRIM(zieldatum), 5, 2) || '-' || SUBSTR(TRIM(zieldatum), 7, 2)
        WHEN TRIM(zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$' 
            THEN TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(zieldatum) ~ '^\d+/\d+/\d{4}$' 
            THEN TO_CHAR(
                MAKE_DATE(
                    CAST(SPLIT_PART(TRIM(zieldatum), '/', 3) AS INTEGER),
                    CAST(SPLIT_PART(TRIM(zieldatum), '/', 1) AS INTEGER),
                    CAST(SPLIT_PART(TRIM(zieldatum), '/', 2) AS INTEGER)
                ),
                'YYYY-MM-DD'
            )
        ELSE NULL
    END AS CloseDate,

    CASE
        WHEN auftragswert IS NULL OR TRIM(auftragswert) = '' THEN NULL
        WHEN UPPER(TRIM(auftragswert)) IN ('NONE', 'NULL', 'N/A') THEN NULL
        WHEN TRIM(auftragswert) = '0' THEN 0.0
        ELSE
            CAST(
                CASE
                    -- European format: contains both dot and comma, with comma after last dot (e.g., "316.863,04")
                    WHEN REGEXP_REPLACE(REGEXP_REPLACE(auftragswert, 'EUR', '', 'gi'), '[^A-Za-z€]', '', '') = '' 
                        AND TRIM(auftragswert) ~ '\d\.\d{3},\d+' THEN
                        CAST(
                            REPLACE(SPLIT_PART(REGEXP_REPLACE(auftragswert, 'EUR', '', 'gi'), ',', 1), '.', '') || '.' || SPLIT_PART(REGEXP_REPLACE(auftragswert, 'EUR', '', 'gi'), ',', 2)
                        AS DOUBLE PRECISION)
                    -- European format without EUR prefix (e.g., "316.863,04")
                    WHEN TRIM(auftragswert) ~ '\d\.\d{3},\d+' THEN
                        CAST(
                            REPLACE(SPLIT_PART(TRIM(auftragswert), ',', 1), '.', '') || '.' || SPLIT_PART(TRIM(auftragswert), ',', 2)
                        AS DOUBLE PRECISION)
                    -- Remove currency text and symbols, dots may be decimal or thousands, remove non-numeric except dot and comma
                    ELSE 
                        CAST(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(TRIM(auftragswert), 'EUR', '', 'gi'),
                                    '[€]', '', ''
                                ),
                                '[^0-9.,\-]', '', 'g'
                            ) AS DOUBLE PRECISION
                        )
                END
            ) AS Amount,

    CASE 
        WHEN waehrungscode IS NULL THEN NULL
        WHEN UPPER(TRIM(waehrungscode)) IN ('EUR', '€') THEN 'EUR'
        WHEN UPPER(TRIM(waehrungscode)) IN ('USD', 'DOLLAR', '$') THEN 'USD'
        WHEN UPPER(TRIM(waehrungscode)) IN ('CHF', 'SWISS FRANC') THEN 'CHF'
        WHEN UPPER(TRIM(waehrungscode)) IN ('GBP', 'POUND', '£') THEN 'GBP'
        ELSE UPPER(REGEXP_REPLACE(waehrungscode, '[^A-Z]', '', 'gi'))
    END AS CurrencyIsoCode,

    kunden_ref AS AccountId,
    opp_kennung AS Legacy_Opportunity_ID__c,
    CURRENT_DATE::TEXT AS CreatedDate,
    CURRENT_DATE::TEXT AS LastModifiedDate,
    0 AS IsDeleted

FROM "fixture_master_src"."master_opportunities"