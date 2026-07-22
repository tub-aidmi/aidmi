{{ config(materialized='table') }}

SELECT
    CAST(m.opp_kennung AS text) AS "Id",
    COALESCE(NULLIF(TRIM(m.titel), ''), 'Unnamed Opportunity') AS "Name",
    CASE UPPER(TRIM(m.vertriebsphase))
        WHEN 'PROSPECTING' THEN 'Prospecting'
        WHEN 'PROSPECT' THEN 'Prospecting'
        WHEN 'QUALIFICATION' THEN 'Qualification'
        WHEN 'QUALI' THEN 'Qualification'
        WHEN 'QUALIFIKATION' THEN 'Qualification'
        WHEN 'IN KONTAKT' THEN 'Needs Analysis'
        WHEN 'IN PRüFUNG' THEN 'Value Proposition'
        WHEN 'CLOSED WON' THEN 'Closed Won'
        WHEN 'WON' THEN 'Closed Won'
        WHEN 'GEWONNEN' THEN 'Closed Won'
        WHEN 'ABGESCHLOSSEN (GEWONNEN)' THEN 'Closed Won'
        WHEN 'CLOSED LOST' THEN 'Closed Lost'
        WHEN 'LOST' THEN 'Closed Lost'
        WHEN 'VERLOREN' THEN 'Closed Lost'
        WHEN 'ABGESCHLOSSEN (VERLOREN)' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN TRIM(m.zieldatum) IS NULL OR TRIM(m.zieldatum) = '' THEN NULL
        WHEN UPPER(TRIM(m.zieldatum)) = 'N/A' THEN NULL
        WHEN TRIM(m.zieldatum) = '0000-00-00' THEN NULL
        WHEN m.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(m.zieldatum), 'YYYY-MM-DD')::text
        WHEN m.zieldatum ~ '^\d{8}$' THEN TO_DATE(TRIM(m.zieldatum), 'YYYYMMDD')::text
        WHEN m.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(m.zieldatum), 'MM/DD/YYYY')::text
        WHEN m.zieldatum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(m.zieldatum), 'DD.MM.YYYY')::text
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(m.auftragswert) IS NULL OR TRIM(m.auftragswert) = ''
             OR UPPER(TRIM(m.auftragswert)) = 'NONE'
             OR TRIM(m.auftragswert) = '0' THEN NULL
        ELSE
            CASE
                WHEN REGEXP_REPLACE(UPPER(TRIM(m.auftragswert)), '(EUR|DOLLAR|[€$])\s*', '') ~ ',' THEN
                    -- European format: remove thousand-sep dots, then swap comma to decimal point
                    CAST(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(TRIM(m.auftragswert), '\.', ''),  -- Remove dot thousands first
                            ',', '.'                                        -- Swap comma to period
                        ) AS DOUBLE PRECISION
                    )
                ELSE
                    -- Standard format: strip currency prefix and cast
                    CAST(
                        REGEXP_REPLACE(TRIM(m.auftragswert), '(EUR|DOLLAR|[€$])\s*', '') AS DOUBLE PRECISION
                    )
            END
    END AS "Amount",
    CASE UPPER(TRIM(COALESCE(m.waehrungscode, '')))
        WHEN 'USD' THEN 'USD'
        WHEN 'EUR' THEN 'EUR'
        WHEN 'GBP' THEN 'GBP'
        WHEN 'CHF' THEN 'CHF'
        WHEN 'DOLLAR' THEN 'USD'
        WHEN '€' THEN 'EUR'
        ELSE NULL
    END AS "CurrencyIsoCode",
    REPLACE(TRIM(k.kundennummer), '', '') AS "AccountId",
    CAST(m.opp_kennung AS text) AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_src', 'master_opportunities') }} m
LEFT JOIN {{ source('fixture_master_src', 'master_kunden') }} k
    ON REPLACE(TRIM(m.kunden_ref), 'KD-', 'CUST-') = TRIM(k.kundennummer)